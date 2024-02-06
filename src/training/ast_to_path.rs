use std::{collections::HashMap, error::Error, fmt, io::Write, path::PathBuf, str::FromStr};

use rand::SeedableRng;
use rand_chacha::ChaCha8Rng;
use smol_str::SmolStr;
use sqlparser::ast::{self, BinaryOperator, DataType, Expr, FunctionArg, FunctionArgExpr, Ident, ObjectName, Query, Select, SelectItem, SetExpr, TableWithJoins, TrimWhereField, UnaryOperator, Value};

use crate::{config::{Config, MainConfig, TomlReadable}, query_creation::{query_generator::{aggregate_function_settings::{AggregateFunctionAgruments, AggregateFunctionDistribution}, call_modifiers::{TypesTypeValue, ValueSetterValue, WildcardRelationsValue}, query_info::{ClauseContext, DatabaseSchema, QueryProps}, value_choosers::{DeterministicValueChooser, RandomValueChooser}, QueryGenerator}, state_generator::{markov_chain_generator::{error::SyntaxError, markov_chain::{CallModifiers, MarkovChain}, ChainStateMemory, DynClone, StateGeneratorConfig}, state_choosers::{MaxProbStateChooser, ProbabilisticStateChooser}, subgraph_type::SubgraphType, substitute_models::{AntiCallModel, DeterministicModel, PathModel, SubstituteModel}, CallTypes, MarkovChainGenerator}}, unwrap_variant, unwrap_variant_or_else};

pub struct AST2PathTestingConfig {
    pub schema: PathBuf,
    pub n_tests: usize,
}

impl TomlReadable for AST2PathTestingConfig {
    fn from_toml(toml_config: &toml::Value) -> Self {
        let section = &toml_config["ast_to_path_testing"];
        Self {
            schema: PathBuf::from_str(section["testing_schema"].as_str().unwrap()).unwrap(),
            n_tests: section["n_tests"].as_integer().unwrap() as usize,
        }
    }
}

/// currently uses training DB to test (subject to change)
pub struct TestAST2Path {
    config: AST2PathTestingConfig,
    main_config: MainConfig,
    path_generator: PathGenerator,
    random_query_generator: QueryGenerator<AntiCallModel, ProbabilisticStateChooser, RandomValueChooser>,
    path_query_generator: QueryGenerator<PathModel, MaxProbStateChooser, DeterministicValueChooser>,
}

impl TestAST2Path {
    pub fn with_config(config: Config) -> Result<Self, SyntaxError> {
        Ok(Self {
            path_generator: PathGenerator::new(
                DatabaseSchema::parse_schema(&config.ast2path_testing_config.schema),
                &config.chain_config,
                config.generator_config.aggregate_functions_distribution.clone(),
            )?,
            config: config.ast2path_testing_config,
            main_config: config.main_config,
            random_query_generator: QueryGenerator::from_state_generator_and_schema(
                MarkovChainGenerator::with_config(&config.chain_config).unwrap(),
                config.generator_config.clone(),
            ),
            path_query_generator: QueryGenerator::from_state_generator_and_schema(
                MarkovChainGenerator::with_config(&config.chain_config).unwrap(),
                config.generator_config,
            ),
        })
    }

    pub fn test(&mut self) -> Result<(), ConvertionError> {
        for i in 0..self.config.n_tests {
            // println!("\n\n\n ================== Beggining GENERATION ================== \n\n\n");
            let query = Box::new(self.random_query_generator.generate());
            // 3700
            let path = match self.path_generator.get_query_path(&query) {
                Ok(path) => path,
                Err(err) => {
                    println!("Tested query: {query}");
                    return Err(err)
                },
            };
            let generated_query = self.path_query_generator.generate_with_substitute_model_and_value_chooser(
                Box::new(PathModel::from_path_nodes(&path)),
                Box::new(DeterministicValueChooser::from_path_nodes(&path))
            );
            if *query != generated_query {
                println!("\nAST -> path -> AST mismatch!\nOriginal  query: {}\nGenerated query: {}", query, generated_query);
                println!("Path: {:?}", path);
            }
            if i % 10 == 0 {
                if self.main_config.print_progress {
                    print!("{}/{}      \r", i, self.config.n_tests);
                }
                std::io::stdout().flush().unwrap();
            }
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct ConvertionError {
    reason: String,
}

impl Error for ConvertionError { }

impl fmt::Display for ConvertionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "AST to path convertion error: {}", self.reason)
    }
}

impl ConvertionError {
    pub fn new(reason: String) -> Self {
        Self { reason }
    }
}

#[derive(Debug, Clone)]
pub enum PathNode {
    State(SmolStr),
    NewFunction(SmolStr),
    SelectedTableName(ObjectName),
    SelectedColumnNameFROM(Vec<Ident>),
    SelectedColumnNameGROUPBY(Vec<Ident>),
    NumericValue(String),
    IntegerValue(String),
    BigIntValue(String),
    QualifiedWildcardSelectedRelation(Ident),
    SelectAlias(Ident),
}

pub struct PathGenerator {
    current_path: Vec<PathNode>,
    database_schema: DatabaseSchema,
    state_generator: MarkovChainGenerator<MaxProbStateChooser>,
    state_selector: DeterministicModel,
    clause_context: ClauseContext,
    aggregate_functions_distribution: AggregateFunctionDistribution,
    rng: ChaCha8Rng,
}

impl PathGenerator {
    pub fn new(
        database_schema: DatabaseSchema,
        chain_config: &StateGeneratorConfig,
        aggregate_functions_distribution: AggregateFunctionDistribution,
    ) -> Result<Self, SyntaxError> {
        Ok(Self {
            current_path: vec![],
            database_schema,
            state_generator: MarkovChainGenerator::<MaxProbStateChooser>::with_config(chain_config)?,
            state_selector: DeterministicModel::new(),
            clause_context: ClauseContext::new(),
            aggregate_functions_distribution,
            rng: ChaCha8Rng::seed_from_u64(1),
        })
    }

    fn process_query(&mut self, query: &Box<Query>) -> Result<(), ConvertionError> {
        self.handle_query(query)?;
        // reset the generator
        if let Some(state) = self.next_state_opt().unwrap() {
            panic!("Couldn't reset state generator: Received {state}");
        }
        Ok(())
    }

    pub fn get_query_path(&mut self, query: &Box<Query>) -> Result<Vec<PathNode>, ConvertionError> {
        self.process_query(query)?;
        Ok(std::mem::replace(&mut self.current_path, vec![]))
    }

    pub fn markov_chain_ref(&self) -> &MarkovChain {
        self.state_generator.markov_chain_ref()
    }
}

macro_rules! unexpected_expr {
    ($el: expr) => {{
        return Err(ConvertionError::new(format!("Unexpected expression: {:#?}", $el)))
    }};
}

macro_rules! unexpected_subgraph_type {
    ($el: expr) => {{
        return Err(ConvertionError::new(format!("Unexpected subgraph type: {:#?}", $el)))
    }};
}

fn add_error(error_mem: &mut HashMap<String, Vec<(SubgraphType, ConvertionError)>>, key: &str, graph_type: SubgraphType, error: ConvertionError) {
    error_mem.entry(key.to_string()).or_insert(vec![]).push((graph_type, error));
}

fn get_errors_str(error_mem: &HashMap<String, Vec<(SubgraphType, ConvertionError)>>) -> String {
    error_mem.iter().fold(String::new(), |mut acc, x| {
        acc += format!(
            "\n\"{}\" ======> {}\n", x.0, x.1.iter().fold(String::new(), |mut acc, y| {
                acc += format!("{:?}: {}\n", y.0, y.1).as_str();
                acc
            })
        ).as_str();
        acc
    })
}

struct Checkpoint {
    clause_context: ClauseContext,
    chain_state_memory: ChainStateMemory,
    current_path: Vec<PathNode>,
}

impl PathGenerator {
    fn expect_compat(&self, target: &SubgraphType, compat_with: &SubgraphType) {
        if !target.is_compat_with(compat_with) {
            self.state_generator.print_stack();
            panic!("Incompatible types: expected compatible with {:?}, got {:?}", compat_with, target);
        }
    }

    fn next_state_opt(&mut self) -> Result<Option<SmolStr>, ConvertionError> {
        match self.state_generator.next_node_name(
            &mut self.rng, &self.clause_context, &mut self.state_selector, None
        ) {
            Ok(state) => Ok(state),
            Err(err) => Err(ConvertionError::new(format!("{err}"))),
        }
    } 

    fn try_push_state(&mut self, state: &str) -> Result<(), ConvertionError> {
        // println!("pushing state: {state}");
        let is_new_function_initial_state = self.state_generator.has_pending_call();
        let state = SmolStr::new(state);
        self.state_selector.set_state(state.clone());
        if let Some(actual_state) = self.next_state_opt()? {
            if actual_state != state {
                self.state_generator.print_stack();
                panic!("State generator returned {actual_state}, expected {state}");
            }
        } else {
            panic!("State generator stopped prematurely");
        }
        if is_new_function_initial_state {
            self.current_path.push(PathNode::NewFunction(state));
        } else {
            self.current_path.push(PathNode::State(state));
        }
        Ok(())
    }

    fn try_push_states(&mut self, state_names: &[&str]) -> Result<(), ConvertionError> {
        for state_name in state_names {
            self.try_push_state(state_name)?;
        }
        Ok(())
    }

    fn push_node(&mut self, path_node: PathNode) {
        self.current_path.push(path_node);
    }

    fn get_checkpoint(&mut self) -> Checkpoint {
        Checkpoint {
            clause_context: self.clause_context.clone(),
            chain_state_memory: self.state_generator.get_chain_state(),
            current_path: self.current_path.clone(),
        }
    }

    fn restore_checkpoint(&mut self, checkpoint: &Checkpoint) {
        self.clause_context = checkpoint.clause_context.clone();
        self.state_generator.set_chain_state(checkpoint.chain_state_memory.dyn_clone());
        self.current_path = checkpoint.current_path.clone();
    }

    fn restore_checkpoint_consume(&mut self, checkpoint: Checkpoint) {
        self.clause_context = checkpoint.clause_context;
        self.state_generator.set_chain_state(checkpoint.chain_state_memory);
        self.current_path = checkpoint.current_path;
    }

    /// subgraph def_Query
    fn handle_query(&mut self, query: &Box<Query>) -> Result<Vec<(Option<Ident>, SubgraphType)>, ConvertionError> {
        self.clause_context.on_query_begin();
        self.try_push_state("Query")?;

        let select_body = unwrap_variant!(&*query.body, SetExpr::Select);
        
        self.try_push_state("call0_FROM")?;
        self.handle_from(&select_body.from)?;

        if let Some(ref selection) = select_body.selection {
            self.try_push_state("call0_WHERE")?;
            self.handle_where(selection)?;
        }

        if select_body.group_by.len() != 0 {
            self.handle_query_after_group_by(select_body, true)?;
        } else {
            let implicit_group_by_checkpoint = self.get_checkpoint();
            match self.handle_query_after_group_by(select_body, false) {
                Ok(..) => { },
                Err(err_no_grouping) => {
                    self.restore_checkpoint_consume(implicit_group_by_checkpoint);
                    match self.handle_query_after_group_by(select_body, true) {
                        Ok(..) => { },
                        Err(err_grouping) => {
                            return Err(ConvertionError::new(format!(
                                "Error converting query: {query}\n\
                                Tried without implicit groping, got an error: {:#?}\n\
                                Tried with implicit groping, got an error: {:#?}",
                                err_no_grouping, err_grouping
                            )))
                        }
                    }
                }
            }
        };

        self.try_push_state("call0_LIMIT")?;
        self.handle_limit(&query.limit)?;

        self.try_push_state("EXIT_Query")?;
        let output_type = self.clause_context.query_mut().pop_output_type();
        self.clause_context.on_query_end();

        return Ok(output_type)
    }

    fn handle_query_after_group_by(&mut self, select_body: &Box<Select>, with_group_by: bool) -> Result<(), ConvertionError> {
        if with_group_by {
            self.try_push_state("call0_GROUP_BY")?;
            self.handle_group_by(&select_body.group_by)?;
        }

        if let Some(ref having) = select_body.having {
            if self.clause_context.group_by().is_grouping_active() {
                self.try_push_state("call0_HAVING")?;
                self.handle_having(having)?;
            } else {
                return Err(ConvertionError::new(format!(
                    "HAVING was used but with_grouping was set to false"
                )))
            }
        }

        self.try_push_state("call0_SELECT")?;
        self.handle_select(select_body.distinct, &select_body.projection)?;

        Ok(())
    }

    /// subgraph def_FROM
    fn handle_from(&mut self, from: &Vec<TableWithJoins>) -> Result<(), ConvertionError> {
        self.try_push_state("FROM")?;
        for table_with_joins in from.iter() {
            self.clause_context.from_mut().create_empty_subfrom();
            match &table_with_joins.relation {
                sqlparser::ast::TableFactor::Table { name, .. } => {
                    self.try_push_state("FROM_table")?;
                    self.handle_from_table(name)?;
                },
                sqlparser::ast::TableFactor::Derived { subquery, .. } => {
                    self.try_push_state("call0_Query")?;
                    self.handle_from_query(subquery)?;
                },
                any => unexpected_expr!(any)
            }
            for join in &table_with_joins.joins {
                self.try_push_state("FROM_join_by")?;
                let (join_type, join_on) = match &join.join_operator {
                    ast::JoinOperator::Inner(join_on) => ("FROM_join_join", join_on),
                    ast::JoinOperator::LeftOuter(join_on) => ("FROM_left_join", join_on),
                    ast::JoinOperator::RightOuter(join_on) => ("FROM_right_join", join_on),
                    ast::JoinOperator::FullOuter(join_on) => ("FROM_full_join", join_on),
                    any => unexpected_expr!(any),
                };
                let join_on = unwrap_variant!(join_on, ast::JoinConstraint::On);
                self.try_push_state(join_type)?;
                self.try_push_state("FROM_join_to")?;
                match &join.relation {
                    sqlparser::ast::TableFactor::Table { name, .. } => {
                        self.try_push_state("FROM_join_table")?;
                        self.handle_from_table(name)?;
                    },
                    sqlparser::ast::TableFactor::Derived { subquery, .. } => {
                        self.try_push_state("call5_Query")?;
                        self.handle_from_query(subquery)?;
                    },
                    any => unexpected_expr!(any)
                }
                self.try_push_states(&["FROM_join_on", "call83_types"])?;
                self.handle_types(join_on, Some(&[SubgraphType::Val3]), None)?;
            }
            self.try_push_state("FROM_cartesian_product")?;
            self.clause_context.from_mut().delete_subfrom();
        }
        self.try_push_state("EXIT_FROM")?;
        Ok(())
    }

    fn handle_from_table(&mut self, name: &ObjectName) -> Result<(), ConvertionError> {
        self.push_node(PathNode::SelectedTableName(name.clone()));
        let create_table_st = self.database_schema.get_table_def_by_name(name);
        self.clause_context.from_mut().append_table(create_table_st);
        Ok(())
    }

    fn handle_from_query(&mut self, subquery: &Box<Query>) -> Result<(), ConvertionError> {
        let column_idents_and_graph_types = self.handle_query(subquery)?;
        self.clause_context.from_mut().append_query(column_idents_and_graph_types);
        Ok(())
    }

    /// subgraph def_WHERE
    fn handle_where(&mut self, selection: &Expr) -> Result<SubgraphType, ConvertionError> {
        self.try_push_state("WHERE")?;
        self.try_push_state("call53_types")?;
        let tp = self.handle_types(selection, Some(&[SubgraphType::Val3]), None)?;
        self.try_push_state("EXIT_WHERE")?;
        Ok(tp)
    }

    /// subgraph def_SELECT
    fn handle_select(&mut self, distinct: bool, projection: &Vec<SelectItem>) -> Result<(), ConvertionError> {
        self.try_push_state("SELECT")?;
        if distinct {
            self.try_push_state("SELECT_DISTINCT")?;
        }

        let mut column_idents_and_graph_types = vec![];

        let single_column_mod = match self.state_generator.get_fn_modifiers() {
            CallModifiers::StaticList(list) if list.contains(&SmolStr::new("single column")) => true,
            _ => false,
        };

        let select_item_state = if self.clause_context.group_by().is_grouping_active() {
            "call73_types"
        } else {
            "call54_types"
        };

        for (i, select_item) in projection.iter().enumerate() {
            if i > 0 {
                if single_column_mod {
                    return Err(ConvertionError::new(format!(
                        "single column modifier is ON but the projection contains multiple columns: {:#?}",
                        projection
                    )))
                } else {
                    self.try_push_state("SELECT_list_multiple_values")?;
                }
            }
            self.try_push_state("SELECT_list")?;
            match select_item {
                SelectItem::UnnamedExpr(expr) => {
                    self.try_push_states(&["SELECT_unnamed_expr", "select_expr", select_item_state])?;
                    let alias = QueryProps::extract_alias(&expr);
                    let expr = self.handle_types(expr, None, None)?;
                    self.try_push_state("select_expr_done")?;
                    column_idents_and_graph_types.push((alias, expr));
                },
                SelectItem::ExprWithAlias { expr, alias } => {
                    self.try_push_states(&["SELECT_expr_with_alias", "select_expr", select_item_state])?;
                    let expr = self.handle_types(expr, None, None)?;
                    self.try_push_state("select_expr_done")?;
                    self.push_node(PathNode::SelectAlias(alias.clone()));  // the order is important
                    column_idents_and_graph_types.push((Some(alias.clone()), expr));
                },
                SelectItem::QualifiedWildcard(alias, ..) => {
                    self.try_push_states(&["SELECT_tables_eligible_for_wildcard", "SELECT_qualified_wildcard"])?;
                    let relation = match alias.0.as_slice() {
                        [ident] => {
                            let wildcard_relations = unwrap_variant!(self.state_generator.get_named_value::<WildcardRelationsValue>().unwrap(), ValueSetterValue::WildcardRelations);
                            if !wildcard_relations.wildcard_selectable_relations.contains(ident) {
                                return Err(ConvertionError::new(format!(
                                    "{ident}.* is not available: wildcard_selectable_relations = {:?}",
                                    wildcard_relations.wildcard_selectable_relations
                                )))
                            }
                            self.clause_context.from().get_relation_by_name(ident)
                        },
                        any => panic!("schema.table alias is not supported: {}", ObjectName(any.to_vec())),
                    };
                    column_idents_and_graph_types.extend(relation.get_columns_with_types().into_iter());
                    self.push_node(PathNode::QualifiedWildcardSelectedRelation(alias.0.last().unwrap().clone()));
                },
                SelectItem::Wildcard(..) => {
                    self.try_push_states(&["SELECT_tables_eligible_for_wildcard", "SELECT_wildcard"])?;
                    column_idents_and_graph_types.extend(self.clause_context
                        .from().get_wildcard_columns().into_iter()
                    );
                },
            }
        }
        self.try_push_state("EXIT_SELECT")?;

        self.clause_context.query_mut().set_select_type(column_idents_and_graph_types);

        Ok(())
    }

    /// subgraph def_LIMIT
    fn handle_limit(&mut self, expr_opt: &Option<Expr>) -> Result<SubgraphType, ConvertionError> {
        self.try_push_state("LIMIT")?;

        let limit_type = if let Some(expr) = expr_opt {
            match self.state_generator.get_fn_modifiers() {
                CallModifiers::StaticList(list) if list.contains(&SmolStr::new("single row")) => {
                    if expr != &Expr::Value(Value::Number("1".to_string(), false)) {
                        return Err(ConvertionError::new(format!(
                            "Expected query to have \"LIMIT 1\" because of \'single row\', got {:#?}", expr
                        )))
                    }
                    self.try_push_state("single_row_true")?;
                    SubgraphType::Integer
                },
                _ => {
                    self.try_push_states(&["limit_num", "call52_types"])?;
                    self.handle_types(expr, Some(&[SubgraphType::Numeric, SubgraphType::Integer, SubgraphType::BigInt]), None)?
                },
            }
        } else {
            self.try_push_states(&["query_can_skip_limit_set_val", "query_can_skip_limit"])?;
            SubgraphType::Undetermined
        };

        self.try_push_state("EXIT_LIMIT")?;

        Ok(limit_type)
    }

    /// subgraph def_VAL_3
    fn handle_val_3(&mut self, expr: &Expr) -> Result<SubgraphType, ConvertionError> {
        self.try_push_state("VAL_3")?;

        match expr {
            x @ (Expr::IsNull(expr) | Expr::IsNotNull(expr)) => {
                self.try_push_state("IsNull")?;
                if matches!(x, Expr::IsNotNull(..)) { self.try_push_state("IsNull_not")?; }
                self.try_push_state("call55_types")?;
                self.handle_types(expr, None, None)?;
            },
            x @ (Expr::IsDistinctFrom(expr_1, expr_2) | Expr::IsNotDistinctFrom(expr_1, expr_2)) =>  {
                self.try_push_states(&["IsDistinctFrom", "call56_types"])?;
                let types_selected_type = self.handle_types(expr_1, None, None)?;
                self.state_generator.set_compatible_list(types_selected_type.get_compat_types());
                if matches!(x, Expr::IsNotDistinctFrom(..)) { self.try_push_state("IsDistinctNOT")?; }
                self.try_push_states(&["DISTINCT", "call21_types"])?;
                self.handle_types(expr_2, None, Some(types_selected_type))?;
            },
            Expr::Exists { subquery, negated } => {
                self.try_push_state("Exists")?;
                if *negated { self.try_push_state("Exists_not")?; }
                self.try_push_state("call2_Query")?;
                self.handle_query(subquery)?;
            },
            Expr::InList { expr, list, negated } => {
                self.try_push_states(&["InList", "call57_types"])?;
                let types_selected_type = self.handle_types(expr, None, None)?;
                self.state_generator.set_compatible_list(types_selected_type.get_compat_types());
                if *negated { self.try_push_state("InListNot")?; }
                self.try_push_states(&["InListIn", "call1_list_expr"])?;
                self.handle_list_expr(list)?;
            },
            Expr::InSubquery { expr, subquery, negated } => {
                self.try_push_states(&["InSubquery", "call58_types"])?;
                let types_selected_type = self.handle_types(expr, None, None)?;
                self.state_generator.set_compatible_list(types_selected_type.get_compat_types());
                if *negated { self.try_push_state("InSubqueryNot")?; }
                self.try_push_states(&["InSubqueryIn", "call3_Query"])?;
                self.handle_query(subquery)?;
            },
            Expr::Between { expr, negated, low, high } => {
                self.try_push_states(&["Between", "call59_types"])?;
                let types_selected_type = self.handle_types(expr, None, None)?;
                self.state_generator.set_compatible_list(types_selected_type.get_compat_types());
                if *negated { self.try_push_state("BetweenBetweenNot")?; }
                self.try_push_states(&["BetweenBetween", "call22_types"])?;
                self.handle_types(low, None, Some(types_selected_type.clone()))?;
                self.try_push_states(&["BetweenBetweenAnd", "call23_types"])?;
                self.handle_types(high, None, Some(types_selected_type))?;
            },
            Expr::BinaryOp { left, op, right } => {
                match op {
                    BinaryOperator::And |
                    BinaryOperator::Or |
                    BinaryOperator::Xor => {
                        self.try_push_states(&["BinaryBooleanOpV3", "call27_types"])?;
                        self.handle_types(left, Some(&[SubgraphType::Val3]), None)?;
                        self.try_push_state(match op {
                            BinaryOperator::And => "BinaryBooleanOpV3AND",
                            BinaryOperator::Or => "BinaryBooleanOpV3OR",
                            BinaryOperator::Xor => "BinaryBooleanOpV3XOR",
                            any => unexpected_expr!(any),
                        })?;
                        self.try_push_state("call28_types")?;
                        self.handle_types(right, Some(&[SubgraphType::Val3]), None)?;
                    },
                    BinaryOperator::Eq |
                    BinaryOperator::Lt |
                    BinaryOperator::LtEq |
                    BinaryOperator::NotEq => {
                        match &**right {
                            Expr::AnyOp(right_inner) | Expr::AllOp(right_inner) => {
                                self.try_push_states(&["AnyAll", "call61_types"])?;
                                let types_selected_type = self.handle_types(left, None, None)?;
                                self.state_generator.set_compatible_list(types_selected_type.get_compat_types());
                                self.try_push_state("AnyAllSelectOp")?;
                                self.try_push_state(match op {
                                    BinaryOperator::Eq => "AnyAllEqual",
                                    BinaryOperator::Lt => "AnyAllLess",
                                    BinaryOperator::LtEq => "AnyAllLessEqual",
                                    BinaryOperator::NotEq => "AnyAllUnEqual",
                                    any => unexpected_expr!(any),
                                })?;
                                self.try_push_state("AnyAllSelectIter")?;
                                match &**right_inner {
                                    Expr::Subquery(query) => {
                                        self.try_push_state("call4_Query")?;
                                        self.handle_query(query)?;
                                    },
                                    any => unexpected_expr!(any),
                                }
                                self.try_push_state("AnyAllAnyAll")?;
                                self.try_push_state(match &**right {
                                    Expr::AllOp(..) => "AnyAllAnyAllAll",
                                    Expr::AnyOp(..) => "AnyAllAnyAllAny",
                                    any => unexpected_expr!(any),
                                })?;
                            },
                            _ => {
                                self.try_push_states(&["BinaryComp", "call60_types"])?;
                                let types_selected_type = self.handle_types(left, None, None)?;
                                self.state_generator.set_compatible_list(types_selected_type.get_compat_types());
                                self.try_push_state(match op {
                                    BinaryOperator::Eq => "BinaryCompEqual",
                                    BinaryOperator::Lt => "BinaryCompLess",
                                    BinaryOperator::LtEq => "BinaryCompLessEqual",
                                    BinaryOperator::NotEq => "BinaryCompUnEqual",
                                    any => unexpected_expr!(any),
                                })?;
                                self.try_push_state("call24_types")?;
                                self.handle_types(right, None, Some(types_selected_type))?;
                            }
                        }
                    },
                    any => unexpected_expr!(any),
                }
            },
            Expr::Like { negated, expr, pattern, .. } => {
                self.try_push_states(&["BinaryStringLike", "call25_types"])?;
                self.handle_types(expr, Some(&[SubgraphType::Text]), None)?;
                if *negated { self.try_push_state("BinaryStringLikeNot")?; }
                self.try_push_states(&["BinaryStringLikeIn", "call26_types"])?;
                self.handle_types(pattern, Some(&[SubgraphType::Text]), None)?;
            }
            Expr::Value(Value::Boolean(true)) => self.try_push_state("true")?,
            Expr::Value(Value::Boolean(false)) => self.try_push_state("false")?,
            Expr::Nested(expr) => {
                self.try_push_states(&["Nested_VAL_3", "call29_types"])?;
                self.handle_types(expr, Some(&[SubgraphType::Val3]), None)?;
            },
            Expr::UnaryOp { op, expr } if *op == UnaryOperator::Not => {
                self.try_push_states(&["UnaryNot_VAL_3", "call30_types"])?;
                self.handle_types(
                    expr, Some(&[SubgraphType::Val3]), None
                )?;
            }
            any => unexpected_expr!(any),
        }

        self.try_push_state("EXIT_VAL_3")?;

        Ok(SubgraphType::Val3)
    }

    /// subgraph def_numeric
    fn handle_number(&mut self, expr: &Expr) -> Result<SubgraphType, ConvertionError> {
        self.try_push_state("number")?;
        if unwrap_variant!(self.state_generator.get_fn_selected_types_unwrapped(), CallTypes::TypeList).len() > 1 {
            todo!(
                "The number subgraph cannot yet choose between types / perform mixed type \
                operations / type casts. It only accepts either integer, numeric or bigint, but not both"
            );
        }
        let number_type = match expr {
            Expr::Value(Value::Number(number_str, false)) => {
                self.try_push_state("number_literal")?;
                self.handle_number_determine_literal_type(number_str)?
            },
            Expr::BinaryOp { left, op, right } => {
                self.try_push_states(&["BinaryNumberOp", "call48_types"])?;
                let number_type = self.handle_types(left, None, None)?;
                self.try_push_state(match op {
                    BinaryOperator::BitwiseAnd => "binary_number_bin_and",
                    BinaryOperator::BitwiseOr => "binary_number_bin_or",
                    BinaryOperator::PGBitwiseXor => "binary_number_bin_xor",
                    BinaryOperator::PGExp => "binary_number_exp",
                    BinaryOperator::Divide => "binary_number_div",
                    BinaryOperator::Minus => "binary_number_minus",
                    BinaryOperator::Multiply => "binary_number_mul",
                    BinaryOperator::Plus => "binary_number_plus",
                    any => unexpected_expr!(any),
                })?;
                self.try_push_state("call47_types")?;
                self.handle_types(right, None, None)?;
                number_type
            },
            Expr::UnaryOp { op, expr } => {
                self.try_push_state("UnaryNumberOp")?;
                self.try_push_state(match op {
                    UnaryOperator::PGAbs => "unary_number_abs",
                    UnaryOperator::PGBitwiseNot => "unary_number_bin_not",
                    UnaryOperator::PGCubeRoot => "unary_number_cub_root",
                    UnaryOperator::Minus => "unary_number_minus",
                    UnaryOperator::Plus => "unary_number_plus",
                    UnaryOperator::PGSquareRoot => "unary_number_sq_root",
                    any => unexpected_expr!(any),
                })?;
                self.try_push_state("call1_types")?;
                self.handle_types(expr, None, None)?
            },
            Expr::Position { expr, r#in } => {
                self.try_push_states(&["number_string_position", "call2_types"])?;
                self.handle_types(expr, Some(&[SubgraphType::Text]), None)?;
                self.try_push_states(&["string_position_in", "call3_types"])?;
                self.handle_types(r#in, Some(&[SubgraphType::Text]), None)?;
                SubgraphType::Integer
            },
            Expr::Nested(inner) => {
                self.try_push_states(&["nested_number", "call4_types"])?;
                self.handle_types(inner, Some(&[SubgraphType::Numeric, SubgraphType::Integer, SubgraphType::BigInt]), None)?
            },
            any => unexpected_expr!(any),
        };
        self.try_push_state("EXIT_number")?;
        Ok(number_type)
    }

    fn handle_number_determine_literal_type(&mut self, number_str: &String) -> Result<SubgraphType, ConvertionError> {
        // assume that literals are the smallest type that they fit into
        // (this is now postgres determines them)
        let err_int_str = match number_str.parse::<u32>() {
            Ok(..) => {
                match self.try_push_state("number_literal_integer") {
                    Ok(..) => {
                        self.push_node(PathNode::IntegerValue(number_str.clone()));
                        return Ok(SubgraphType::Integer)
                    },
                    Err(err_int) => format!("{err_int}"),
                }
            },
            Err(err_int) => format!("{err_int}")
        };
        if number_str.parse::<u64>().is_ok() {
            match self.try_push_state("number_literal_bigint") {
                Ok(..) => {
                    self.push_node(PathNode::BigIntValue(number_str.clone()));
                    Ok(SubgraphType::BigInt)
                },
                Err(err_bigint) => {
                    Err(ConvertionError::new(format!(
                        "Error trying to find '{number_str}' literal type.\
                        Tried Integer, got: {err_int_str}\
                        Tried BigInt, got: {err_bigint}"
                    )))
                },
            }
        } else {
            self.try_push_state("number_literal_numeric")?;
            self.push_node(PathNode::NumericValue(number_str.clone()));
            Ok(SubgraphType::Numeric)
        }
    }

    /// subgraph def_text
    fn handle_text(&mut self, expr: &Expr) -> Result<SubgraphType, ConvertionError> {
        self.try_push_state("text")?;
        match expr {
            Expr::Value(Value::SingleQuotedString(_)) => {
                self.try_push_state("text_literal")?;
            },
            Expr::Nested(inner) => {
                self.try_push_states(&["text_nested", "call62_types"])?;
                self.handle_types(&inner, Some(&[SubgraphType::Text]), None)?;
            },
            Expr::Trim { expr, trim_where, trim_what } => {
                self.try_push_state("text_trim")?;
                match (trim_where, trim_what) {
                    (Some(trim_where), Some(trim_what)) => {
                        self.try_push_state("call6_types")?;
                        self.handle_types(trim_what, Some(&[SubgraphType::Text]), None)?;
                        self.try_push_state(match trim_where {
                            TrimWhereField::Both => "BOTH",
                            TrimWhereField::Leading => "LEADING",
                            TrimWhereField::Trailing => "TRAILING",
                        })?;
                    },
                    (None, None) => {},
                    any => unexpected_expr!(any),
                };
                self.try_push_state("call5_types")?;
                self.handle_types(expr, Some(&[SubgraphType::Text]), None)?;
            },
            Expr::BinaryOp { left, op, right } if *op == BinaryOperator::StringConcat => {
                self.try_push_states(&["text_concat", "call7_types"])?;
                self.handle_types(left, Some(&[SubgraphType::Text]), None)?;
                self.try_push_states(&["text_concat_concat", "call8_types"])?;
                self.handle_types(right, Some(&[SubgraphType::Text]), None)?;
            },
            Expr::Substring { expr, substring_from, substring_for} => {
                self.try_push_states(&["text_substring", "call9_types"])?;
                self.handle_types(expr, Some(&[SubgraphType::Text]), None)?;
                if let Some(substring_from) = substring_from {
                    self.try_push_states(&["text_substring_from", "call10_types"])?;
                    self.handle_types(substring_from, Some(&[SubgraphType::Integer]), None)?;
                }
                if let Some(substring_for) = substring_for {
                    self.try_push_states(&["text_substring_for", "call11_types"])?;
                    self.handle_types(substring_for, Some(&[SubgraphType::Integer]), None)?;
                }
                self.try_push_state("text_substring_end")?;
            },
            any => unexpected_expr!(any),
        }
        self.try_push_state("EXIT_text")?;
        Ok(SubgraphType::Text)
    }

    /// subgarph def_date
    fn handle_date(&mut self, expr: &Expr) -> Result<SubgraphType, ConvertionError> {
        self.try_push_state("date")?;
        match expr {
            Expr::TypedString { data_type, .. } if *data_type == DataType::Date => {
                self.try_push_state("date_literal")?;
            },
            any => unexpected_expr!(any),
        }
        self.try_push_state("EXIT_date")?;
        Ok(SubgraphType::Date)
    }

    /// subgraph def_types
    fn handle_types(
        &mut self,
        expr: &Expr,
        check_generated_by_one_of: Option<&[SubgraphType]>,
        check_compatible_with: Option<SubgraphType>,
    ) -> Result<SubgraphType, ConvertionError> {
        self.try_push_state("types")?;

        let selected_types = unwrap_variant!(self.state_generator.get_fn_selected_types_unwrapped(), CallTypes::TypeList);
        let modifiers = unwrap_variant!(self.state_generator.get_fn_modifiers().clone(), CallModifiers::StaticList);

        let selected_type = match expr {
            Expr::Value(Value::Null) => {
                self.try_push_state("types_null")?;
                SubgraphType::Undetermined
            },
            Expr::Cast { expr, data_type } if *expr == Box::new(Expr::Value(Value::Null)) => {
                self.handle_types_typed_null(selected_types, data_type)?
            },
            expr => self.handle_types_expr(selected_types, modifiers, expr)?,
        };
        self.try_push_state("EXIT_types")?;

        // TODO: this code any many other code is repeated here and in random query generator.
        if let Some(generators) = check_generated_by_one_of {
            if !generators.iter().any(|as_what| selected_type.is_same_or_more_determined_or_undetermined(&as_what)) {
                self.state_generator.print_stack();
                panic!("Unexpected type: expected one of {:?}, got {:?}", generators, selected_type);
            }
        }
        if let Some(with) = check_compatible_with {
            self.expect_compat(&selected_type, &with);
        }

        return Ok(selected_type)
    }

    fn handle_types_typed_null(&mut self, selected_types: Vec<SubgraphType>, data_type: &DataType) -> Result<SubgraphType, ConvertionError> {
        let null_type = SubgraphType::from_data_type(data_type);
        if !selected_types.contains(&null_type) {
            unexpected_subgraph_type!(null_type)
        }
        self.try_push_state(match &null_type {
            SubgraphType::BigInt => "types_select_type_bigint",
            SubgraphType::Integer => "types_select_type_integer",
            SubgraphType::Numeric => "types_select_type_numeric",
            SubgraphType::Val3 => "types_select_type_3vl",
            SubgraphType::Text => "types_select_type_text",
            SubgraphType::Date => "types_select_type_date",
            any => unexpected_subgraph_type!(any),
        })?;
        self.try_push_state("types_return_typed_null")?;
        Ok(null_type)
    }

    fn handle_types_expr(&mut self, selected_types: Vec<SubgraphType>, modifiers: Vec<SmolStr>, expr: &Expr) -> Result<SubgraphType, ConvertionError> {
        let mut error_mem: HashMap<String, Vec<(SubgraphType, ConvertionError)>> = HashMap::new();
        let types_before_state_selection = self.get_checkpoint();
        let selected_types = SubgraphType::sort_by_compatibility(selected_types);
        let mut selected_types_iter = selected_types.iter();

        // try out different allowed types to find the actual one (or not)
        let subgraph_type = loop {
            let subgraph_type = match selected_types_iter.next() {
                Some(subgraph_type) => subgraph_type,
                None => return Err(ConvertionError::new(
                    format!(
                        "Types didn't find a suitable type for expression, among:\n{:#?}\n\
                        Expression:\n{:#?}\nPrinted: {}\nErrors: {}\n",
                        selected_types, expr, expr, get_errors_str(&error_mem)
                    )
                )),
            };

            match self.try_push_state(match subgraph_type {
                SubgraphType::BigInt => "types_select_type_bigint",
                SubgraphType::Integer => "types_select_type_integer",
                SubgraphType::Numeric => "types_select_type_numeric",
                SubgraphType::Val3 => "types_select_type_3vl",
                SubgraphType::Text => "types_select_type_text",
                SubgraphType::Date => "types_select_type_date",
                any => unexpected_subgraph_type!(any),
            }) {
                Ok(_) => {},
                Err(err) => {
                    add_error(&mut error_mem, "Type not allowed", subgraph_type.clone(), err);
                    self.restore_checkpoint(&types_before_state_selection);
                    continue;
                },
            };

            let allowed_type_list = unwrap_variant!(
                self.state_generator.get_named_value::<TypesTypeValue>().unwrap(),
                ValueSetterValue::TypesType
            ).selected_types.clone();

            let (tp_res, error_key) = match expr {
                Expr::Case { .. } => (
                    self.handle_types_expr_case(subgraph_type, &modifiers, allowed_type_list, expr), "CASE"
                ),
                Expr::Function(function) => (
                    self.handle_types_expr_aggr_function(subgraph_type, &modifiers, allowed_type_list, function), "Aggregate function"
                ),
                Expr::Subquery(subquery) => (
                    self.handle_types_subquery(subgraph_type, &modifiers, allowed_type_list, subquery), "Subquery"
                ),
                expr => {
                    let types_after_state_selection = self.get_checkpoint();
                    match self.handle_types_column_expr(subgraph_type, &modifiers, allowed_type_list, expr) {
                        Ok(tp) => break tp,
                        Err(err) => {
                            add_error(&mut error_mem, "column identifier", subgraph_type.clone(), err);
                            self.restore_checkpoint(&types_after_state_selection);
                        }
                    }
                    (self.handle_types_expr_formula(subgraph_type, &modifiers, expr), "type expr.")
                }
            };
            match tp_res {
                Ok(tp) => break tp,
                Err(err) => {
                    add_error(&mut error_mem, error_key, subgraph_type.clone(), err);
                    self.restore_checkpoint(&types_before_state_selection)
                },
            }
        };

        Ok(subgraph_type)
    }

    fn handle_types_expr_case(
        &mut self,
        subgraph_type: &SubgraphType,
        modifiers: &Vec<SmolStr>,
        allowed_type_list: Vec<SubgraphType>,
        expr: &Expr
    ) -> Result<SubgraphType, ConvertionError> {
        if modifiers.contains(&SmolStr::new("no case")) {
            unexpected_expr!(expr)
        }
        self.try_push_states(&["types_select_special_expression", "call0_case"])?;
        self.state_generator.set_known_list(allowed_type_list);
        match self.handle_case(expr) {
            Ok(aggr_type) => {
                if aggr_type == *subgraph_type {
                    Ok(aggr_type)
                } else {
                    panic!("CASE did not return requested type. Requested: {subgraph_type} Got: {aggr_type}")
                }
            },
            Err(err) => Err(err),
        }
    }

    fn handle_types_expr_aggr_function(
        &mut self,
        subgraph_type: &SubgraphType,
        modifiers: &Vec<SmolStr>,
        allowed_type_list: Vec<SubgraphType>,
        function: &ast::Function
    ) -> Result<SubgraphType, ConvertionError> {
        if modifiers.contains(&SmolStr::new("no aggregate")) {
            unexpected_expr!(function)
        }
        self.try_push_states(&["types_select_special_expression", "call0_aggregate_function"])?;
        self.state_generator.set_known_list(allowed_type_list);
        match self.handle_aggregate_function(function) {
            Ok(aggr_type) => {
                if aggr_type == *subgraph_type {
                    Ok(aggr_type)
                } else {
                    panic!("Aggregate function did not return requested type. Requested: {subgraph_type} Got: {aggr_type}")
                }
            },
            Err(err) => Err(err),
        }
    }

    fn handle_types_subquery(
        &mut self,
        subgraph_type: &SubgraphType,
        modifiers: &Vec<SmolStr>,
        allowed_type_list: Vec<SubgraphType>,
        subquery: &Box<Query>, 
    ) -> Result<SubgraphType, ConvertionError> {
        if modifiers.contains(&SmolStr::new("no subquery")) {
            unexpected_expr!(subquery)
        }
        self.try_push_states(&["types_select_special_expression", "call1_Query"])?;
        self.state_generator.set_known_list(allowed_type_list);
        match self.handle_query(subquery) {
            Ok(col_type_list) => {
                if matches!(col_type_list.as_slice(), [(.., query_subgraph_type)] if query_subgraph_type == subgraph_type) {
                    Ok(subgraph_type.clone())
                } else {
                    panic!("Query did not return the requested type. Requested: {:?} Got: {:?}\nQuery: {subquery}", subgraph_type, col_type_list);
                }
            },
            Err(err) => Err(err),
        }
    }

    fn handle_types_column_expr(
        &mut self,
        subgraph_type: &SubgraphType,
        modifiers: &Vec<SmolStr>,
        allowed_type_list: Vec<SubgraphType>,
        expr: &Expr,
    ) -> Result<SubgraphType, ConvertionError> {
        self.try_push_states(&["types_select_special_expression", "call0_column_spec"])?;
        if modifiers.contains(&SmolStr::new("no column spec")) {
            panic!("'no column spec' modifier is present, but call0_column_spec was successfully selected");
        }
        self.state_generator.set_known_list(allowed_type_list);
        let column_type = self.handle_column_spec(expr)?;
        if *subgraph_type == column_type {
            Ok(column_type)
        } else {
            panic!("Column did not return the requested type. Requested: {subgraph_type} Got: {column_type}")
        }
    }

    fn handle_types_expr_formula(
        &mut self,
        subgraph_type: &SubgraphType,
        modifiers: &Vec<SmolStr>,
        expr: &Expr,
    ) -> Result<SubgraphType, ConvertionError> {
        if modifiers.contains(&SmolStr::new("no type expr")) {
            unexpected_expr!(expr)
        }
        match match subgraph_type {
            SubgraphType::BigInt => {
                self.try_push_state("call2_number")?;
                self.handle_number(expr)
            },
            SubgraphType::Integer => {
                self.try_push_state("call1_number")?;
                self.handle_number(expr)
            },
            SubgraphType::Numeric => {
                self.try_push_state("call0_number")?;
                self.handle_number(expr)
            },
            SubgraphType::Val3 => {
                self.try_push_state("call1_VAL_3")?;
                self.handle_val_3(expr)
            },
            SubgraphType::Text => {
                self.try_push_state("call0_text")?;
                self.handle_text(expr)
            },
            SubgraphType::Date => {
                self.try_push_state("call0_date")?;
                self.handle_date(expr)
            },
            any => unexpected_subgraph_type!(any),
        } {
            Ok(actual_type) => {
                if actual_type == *subgraph_type {
                    Ok(actual_type)
                } else {
                    panic!("Subgraph did not return the requested type. Requested: {:?} Got: {:?}", subgraph_type, actual_type);
                }
            },
            Err(err) => Err(err),
        }
    }

    /// subgraph def_column_spec
    fn handle_column_spec(&mut self, expr: &Expr) -> Result<SubgraphType, ConvertionError> {
        self.try_push_state("column_spec")?;
        let column_types = unwrap_variant_or_else!(
            self.state_generator.get_fn_selected_types_unwrapped(), CallTypes::TypeList, || self.state_generator.print_stack()
        );
        self.try_push_state("column_spec_choose_source")?;
        let check_group_by = if self.state_generator.get_fn_modifiers().contains(&SmolStr::new("group by columns")) {
            self.try_push_state("get_column_spec_from_group_by")?;
            true
        } else {
            self.try_push_state("get_column_spec_from_from")?;
            false
        };
        self.try_push_state("column_spec_choose_qualified")?;
        let mut ident_components = match expr {
            Expr::Identifier(ident) => {
                self.try_push_state("unqualified_column_name")?;
                vec![ident.clone()]
            },
            Expr::CompoundIdentifier(idents) if idents.len() == 2 => {
                self.try_push_state("qualified_column_name")?;
                idents.clone()
            },
            any => unexpected_expr!(any),
        };
        if check_group_by {
            ident_components = self.clause_context.from().qualify_column(ident_components);
        }
        let selected_type = match if check_group_by {
            self.clause_context.group_by().get_column_type_by_ident_components(&ident_components)
        } else {
            self.clause_context.from().get_column_type_by_ident_components(&ident_components)
        } {
            Ok(selected_type) => selected_type,
            Err(err) => return Err(ConvertionError::new(format!("{err}"))),
        };
        if !column_types.contains(&selected_type) {
            return Err(ConvertionError::new(format!(
                "get_column_type_by_ident_components() selected a column ({}) with type {:?}, but expected one of {:?}",
                ObjectName(ident_components), selected_type, column_types
            )))
        }
        if check_group_by {
            self.push_node(PathNode::SelectedColumnNameGROUPBY(ident_components));
        } else {
            self.push_node(PathNode::SelectedColumnNameFROM(ident_components));
        }
        self.try_push_state("EXIT_column_spec")?;
        Ok(selected_type)
    }

    /// subgraph def_list_expr
    fn handle_list_expr(&mut self, list: &Vec<Expr>) -> Result<SubgraphType, ConvertionError> {
        self.try_push_states(&["list_expr", "call16_types"])?;
        let inner_type = self.handle_types(&list[0], None, None)?;
        self.try_push_state("list_expr_multiple_values")?;
        self.state_generator.set_compatible_list(inner_type.get_compat_types());
        for expr in list.iter().skip(1) {
            self.try_push_state("call49_types")?;
            self.handle_types(expr, None, Some(inner_type.clone()))?;
        }
        self.try_push_state("EXIT_list_expr")?;
        Ok(SubgraphType::ListExpr(Box::new(inner_type)))
    }

    /// subgraph def_having
    fn handle_having(&mut self, selection: &Expr) -> Result<SubgraphType, ConvertionError> {
        self.try_push_state("HAVING")?;
        self.try_push_state("call45_types")?;
        let tp = self.handle_types(selection, Some(&[SubgraphType::Val3]), None)?;
        self.clause_context.query_mut().set_aggregation_indicated();
        self.try_push_state("EXIT_HAVING")?;
        Ok(tp)
    }

    /// subgraph def_case
    fn handle_case(&mut self, expr: &Expr) -> Result<SubgraphType, ConvertionError> {
        self.try_push_state("case")?;

        let (operand, conditions, results, else_result) = if let Expr::Case {
            operand, conditions, results, else_result
        } = expr {
            (operand, conditions, results, else_result)
        } else { panic!("handle_case reveived {expr}") };

        self.try_push_states(&["case_first_result", "call82_types"])?;
        let out_type = self.handle_types(&results[0], None, None)?;

        if let Some(operand_expr) = operand {
            self.try_push_states(&["simple_case", "simple_case_operand", "call78_types"])?;
            let operand_type = self.handle_types(operand_expr, None, None)?;
            
            for i in 0..conditions.len() {
                self.try_push_states(&["simple_case_condition", "call79_types"])?;
                self.state_generator.set_compatible_list(operand_type.get_compat_types());
                self.handle_types(&conditions[i], None, Some(operand_type.clone()))?;

                if i+1 < results.len() {
                    self.try_push_states(&["simple_case_result", "call80_types"])?;
                    self.state_generator.set_compatible_list(out_type.get_compat_types());
                    self.handle_types(&results[i+1], None, Some(out_type.clone()))?;
                }
            }
        } else {
            self.try_push_state("searched_case")?;

            for i in 0..conditions.len() {
                self.try_push_states(&["searched_case_condition", "call76_types"])?;
                self.handle_types(&conditions[i], Some(&[SubgraphType::Val3]), None)?;

                if i+1 < results.len() {
                    self.try_push_states(&["searched_case_result", "call77_types"])?;
                    self.state_generator.set_compatible_list(out_type.get_compat_types());
                    self.handle_types(&results[i+1], None, Some(out_type.clone()))?;
                }
            }
        }

        self.try_push_state("case_else")?;
        if let Some(else_expr) = else_result {
            self.try_push_state("call81_types")?;
            self.state_generator.set_compatible_list(out_type.get_compat_types());
            self.handle_types(else_expr, None, Some(out_type.clone()))?;
        }
        self.try_push_state("EXIT_case")?;

        Ok(out_type)
    }

    // subgraph def_aggregater_function
    fn handle_aggregate_function(&mut self, function: &ast::Function) -> Result<SubgraphType, ConvertionError> {
        self.try_push_state("aggregate_function")?;

        let selected_types = unwrap_variant!(self.state_generator.get_fn_selected_types_unwrapped(), CallTypes::TypeList);

        let return_type = match selected_types.as_slice() {
            &[ref tp] => tp,
            any => panic!("Aggregate function can only have one selected type. Got: {:?}", any),
        };

        let ast::Function {
            name: aggr_name,
            args: aggr_arg_expr_v,
            over: _,
            distinct,
            special: _,
        } = function;

        if *distinct {
            self.try_push_state("aggregate_distinct")?;
        } else {
            self.try_push_state("aggregate_not_distinct")?;
        }

        self.try_push_state("aggregate_select_return_type")?;

        match (return_type, aggr_arg_expr_v.as_slice()) {
            // * AS AN ARGUMENT: BIGINT
            (SubgraphType::BigInt, &[FunctionArg::Unnamed(FunctionArgExpr::Wildcard)])
            if self.aggregate_functions_distribution.func_names_include(
                &AggregateFunctionAgruments::Wildcard,
                &return_type, aggr_name
            ) => {
                self.try_push_states(&["aggregate_select_type_bigint", "arg_star"])?;
            },

            // SINGLE SAME TYPE ARGUMENT: VAL3, DATE, INTEGER, NUMERIC, TEXT, BIGINT
            (
                SubgraphType::Val3 | SubgraphType::Date | SubgraphType::Integer |
                SubgraphType::Numeric | SubgraphType::Text | SubgraphType::BigInt,
                &[FunctionArg::Unnamed(FunctionArgExpr::Expr(ref arg_expr))]
            ) if self.aggregate_functions_distribution.func_names_include(
                &AggregateFunctionAgruments::TypeList(vec![return_type.clone()]),
                &return_type, aggr_name
            ) => {
                self.try_push_states(match return_type {
                    SubgraphType::Val3 => &["aggregate_select_type_bool", "arg_single_3vl", "call64_types"],
                    SubgraphType::Date => &["aggregate_select_type_date", "arg_date", "call72_types"],
                    SubgraphType::Integer => &["aggregate_select_type_integer", "arg_integer", "call71_types"],
                    SubgraphType::Text => &["aggregate_select_type_text", "arg_single_text", "call63_types"],
                    SubgraphType::Numeric => &["aggregate_select_type_numeric", "arg_single_numeric", "call66_types"],
                    SubgraphType::BigInt => &["aggregate_select_type_bigint", "arg_bigint", "call75_types"],
                    _ => panic!("Something wrong with match arm"),
                })?;
                self.handle_types(arg_expr, Some(&[return_type.clone()]), None)?;
            },

            // ANY SINGLE ARGUMENT: BIGINT
            (SubgraphType::BigInt, &[FunctionArg::Unnamed(FunctionArgExpr::Expr(ref arg_expr))])
            if self.aggregate_functions_distribution.func_names_include(
                &AggregateFunctionAgruments::AnyType,
                &return_type, aggr_name
            ) => {
                self.try_push_states(&["aggregate_select_type_bigint", "arg_bigint_any", "call65_types"])?;
                self.handle_types(arg_expr, None, None)?;
            },

            // DOUBLE ARGUMENT: NUMERIC, TEXT
            (
                SubgraphType::Numeric | SubgraphType::Text,
                &[
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(ref arg_expr_1)),
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(ref arg_expr_2))
                ]
            ) if self.aggregate_functions_distribution.func_names_include(
                &AggregateFunctionAgruments::TypeList(vec![return_type.clone(), return_type.clone()]),
                &return_type, aggr_name
            ) => {
                let states = match return_type {
                    SubgraphType::Text => &["aggregate_select_type_text", "arg_double_text", "call74_types", "call63_types"],
                    SubgraphType::Numeric => &["aggregate_select_type_numeric", "arg_double_numeric", "call68_types", "call66_types"],
                    _ => panic!("Something wrong with match arm"),
                };
                self.try_push_states(&states[..3])?;
                self.handle_types(arg_expr_1, Some(&[return_type.clone()]), None)?;
                self.try_push_state(states[3])?;
                self.handle_types(arg_expr_2, Some(&[return_type.clone()]), None)?;
            },

            _ => {
                unexpected_expr!(function);
            },
        }
        self.try_push_state("EXIT_aggregate_function")?;

        Ok(return_type.clone())
    }

    /// subgraph def_group_by
    fn handle_group_by(&mut self, grouping_exprs: &Vec<Expr>) -> Result<(), ConvertionError> {
        self.try_push_state("GROUP_BY")?;
        if grouping_exprs.len() == 0 || grouping_exprs == &[Expr::Value(Value::Boolean(true))] {
            self.clause_context.group_by_mut().set_single_group_grouping();
            self.clause_context.group_by_mut().set_single_row_grouping();
            self.try_push_states(&["group_by_single_group", "EXIT_GROUP_BY"])?;
            return Ok(())
        } else {
            self.try_push_state("has_accessible_columns")?;
        }
        for grouping_expr in grouping_exprs {
            self.try_push_state("grouping_column_list")?;
            match grouping_expr {
                special_grouping @ (
                    Expr::GroupingSets(..) |
                    Expr::Rollup(..) |
                    Expr::Cube(..)
                ) => {
                    self.try_push_state("special_grouping")?;
                    let (set_list, groupping_type_str) = match special_grouping {
                        Expr::GroupingSets(set_list) => (set_list, "grouping_set"),
                        Expr::Rollup(set_list) => (set_list, "grouping_rollup"),
                        Expr::Cube(set_list) => (set_list, "grouping_cube"),
                        any => unexpected_expr!(any)
                    };
                    self.try_push_state(groupping_type_str)?;
                    for current_set in set_list {
                        self.try_push_state("set_list")?;
                        if current_set.len() == 0 {
                            self.try_push_state("set_list_empty_allowed")?;
                        } else {
                            for (expr_i, column_expr) in current_set.iter().enumerate() {
                                if expr_i > 0 {
                                    self.try_push_state("set_multiple")?;
                                }
                                self.try_push_state("call69_types")?;
                                let column_type = self.handle_types(column_expr, None, None)?;
                                let column_name = self.clause_context.from().get_qualified_ident_components(column_expr);
                                self.clause_context.group_by_mut().append_column(column_name, column_type);
                            }
                        }
                    }
                    if set_list.last().unwrap().len() > 0 {
                        // exit though set_multiple for non-empty sets
                        self.try_push_state("set_multiple")?;
                    }
                },
                column_expr @ (
                    Expr::Identifier(..) |
                    Expr::CompoundIdentifier(..)
                ) => {
                    self.try_push_state("call70_types")?;
                    let column_type = self.handle_types(column_expr, None, None)?;
                    let column_name = self.clause_context.from().get_qualified_ident_components(column_expr);
                    self.clause_context.group_by_mut().append_column(column_name, column_type);
                },
                any => unexpected_expr!(any)
            }
        }

        // For cases such as: GROUPING SETS ( (), (), () )
        if !self.clause_context.group_by().contains_columns() {
            self.clause_context.group_by_mut().set_single_group_grouping();
            // Check is GROUPING SETS ( () )
            if let &[Expr::GroupingSets(ref set_list)] = grouping_exprs.as_slice() {
                if set_list.len() == 1 {
                    self.clause_context.group_by_mut().set_single_row_grouping();
                }
            }
        }

        self.clause_context.query_mut().set_aggregation_indicated();

        self.try_push_state("EXIT_GROUP_BY")?;
        Ok(())
    }

}



