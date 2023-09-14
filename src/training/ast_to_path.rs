use std::{error::Error, fmt, path::PathBuf, str::FromStr, io::Write};

use rand::SeedableRng;
use rand_chacha::ChaCha8Rng;
use smol_str::SmolStr;
use sqlparser::ast::{Query, ObjectName, Expr, SetExpr, SelectItem, Value, BinaryOperator, UnaryOperator, Ident, DataType, TrimWhereField};

use crate::{query_creation::{random_query_generator::{query_info::{DatabaseSchema, ClauseContext}, call_modifiers::{ValueSetterValue, TypesTypeValue}, Unnested, QueryGenerator}, state_generators::{subgraph_type::SubgraphType, state_choosers::{MaxProbStateChooser, ProbabilisticStateChooser}, MarkovChainGenerator, markov_chain_generator::{StateGeneratorConfig, error::SyntaxError, markov_chain::CallModifiers, DynClone, ChainStateMemory}, dynamic_models::{DeterministicModel, DynamicModel, PathModel, AntiCallModel}, CallTypes}}, config::{TomlReadable, Config, MainConfig}, unwrap_variant, unwrap_variant_or_else};

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
    random_query_generator: QueryGenerator<AntiCallModel, ProbabilisticStateChooser>,
    path_query_generator: QueryGenerator<PathModel, MaxProbStateChooser>,
}

impl TestAST2Path {
    pub fn with_config(config: Config) -> Result<Self, SyntaxError> {
        Ok(Self {
            path_generator: PathGenerator::new(
                DatabaseSchema::parse_schema(&config.ast2path_testing_config.schema),
                &config.chain_config,
            )?,
            config: config.ast2path_testing_config,
            main_config: config.main_config,
            random_query_generator: QueryGenerator::from_state_generator_and_schema(
                MarkovChainGenerator::with_config(&config.chain_config).unwrap(),
                config.generator_config.clone(),
            ),
            path_query_generator: QueryGenerator::<PathModel, MaxProbStateChooser>::from_state_generator_and_schema(
                MarkovChainGenerator::<MaxProbStateChooser>::with_config(&config.chain_config).unwrap(),
                config.generator_config,
            ),
        })
    }

    pub fn test(&mut self) -> Result<(), ConvertionError> {
        for i in 0..self.config.n_tests {
            let query = Box::new(self.random_query_generator.next().unwrap());
            let path = self.path_generator.get_query_path(&query)?;
            let generated_query = self.path_query_generator.generate_with_dynamic_model(Box::new(PathModel::with_path(
                path.iter().cloned().filter_map(
                    |x| if let PathNode::State(state) = x { Some(state) } else { None }
                ).collect()
            )));
            if *query != generated_query {
                println!("\nAST -> path -> AST mismatch!\nOriginal  query: {}\nGenerated query: {}", query, generated_query);
                println!("Path: {:?}", path);
            }
            if i % 100 == 0 {
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
    SelectedColumnName(Ident),
}

pub struct PathGenerator {
    current_path: Vec<PathNode>,
    database_schema: DatabaseSchema,
    state_generator: MarkovChainGenerator<MaxProbStateChooser>,
    state_selector: DeterministicModel,
    clause_context: ClauseContext,
    rng: ChaCha8Rng,
}

impl PathGenerator {
    pub fn new(database_schema: DatabaseSchema, chain_config: &StateGeneratorConfig) -> Result<Self, SyntaxError> {
        Ok(Self {
            current_path: vec![],
            database_schema,
            state_generator: MarkovChainGenerator::<MaxProbStateChooser>::with_config(chain_config)?,
            state_selector: DeterministicModel::new(),
            clause_context: ClauseContext::new(),
            rng: ChaCha8Rng::seed_from_u64(1),
        })
    }

    pub fn get_query_path(&mut self, query: &Box<Query>) -> Result<Vec<PathNode>, ConvertionError> {
        self.handle_query(query)?;
        // reset the generator
        if let Some(state) = self.next_state_opt().unwrap() {
            panic!("Couldn't reset state generator: Received {state}");
        }
        Ok(std::mem::replace(&mut self.current_path, vec![]))
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
        match self.state_generator.next(
            &mut self.rng, &self.clause_context, &mut self.state_selector
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

    /// subgraph def_Query
    fn handle_query(&mut self, query: &Box<Query>) -> Result<Vec<(Option<Ident>, SubgraphType)>, ConvertionError> {
        self.clause_context.on_query_begin();
        self.try_push_state("Query")?;

        match self.state_generator.get_fn_modifiers() {
            CallModifiers::StaticList(list) if list.contains(&SmolStr::new("single row")) => {
                if query.limit != Some(Expr::Value(Value::Number("1".to_string(), false))) {
                    return Err(ConvertionError::new(format!(
                        "Expected query to have \"LIMIT 1\" because of \'single row\', got {:#?}", query.limit
                    )))
                }
                self.try_push_state("single_row_true")?;
            }
            _ => {
                self.try_push_state("single_row_false")?;
                if let Some(ref limit) = query.limit {
                    self.try_push_states(&["limit", "call52_types"])?;
                    self.handle_types(limit, Some(&[SubgraphType::Numeric]), None)?;
                }
            }
        }
        self.try_push_state("FROM")?;

        let select_body = unwrap_variant!(&*query.body, SetExpr::Select);

        for table_with_joins in select_body.from.iter() {
            match &table_with_joins.relation {
                sqlparser::ast::TableFactor::Table { name, .. } => {
                    self.try_push_state("Table")?;
                    self.push_node(PathNode::SelectedTableName(name.clone()));
                    let create_table_st = self.database_schema.get_table_def_by_name(name);
                    self.clause_context.from_mut().append_table(create_table_st);
                },
                sqlparser::ast::TableFactor::Derived { subquery, .. } => {
                    self.try_push_state("call0_Query")?;
                    let column_idents_and_graph_types = self.handle_query(&subquery)?;
                    self.clause_context.from_mut().append_query(column_idents_and_graph_types);
                },
                any => unexpected_expr!(any)
            }
            self.try_push_state("FROM_multiple_relations")?;
        }
        self.try_push_state("EXIT_FROM")?;

        if let Some(ref selection) = select_body.selection {
            self.try_push_state("WHERE")?;
            self.try_push_state("call53_types")?;
            self.handle_types(selection, Some(&[SubgraphType::Val3]), None)?;
        }
        self.try_push_state("EXIT_WHERE")?;

        self.try_push_state("SELECT")?;
        if select_body.distinct {
            self.try_push_state("SELECT_DISTINCT")?;
        }
        self.try_push_state("SELECT_distinct_end")?;

        let mut column_idents_and_graph_types = vec![];

        self.try_push_state("SELECT_projection")?;

        let single_column_mod = match self.state_generator.get_fn_modifiers() {
            CallModifiers::StaticList(list) if list.contains(&SmolStr::new("single column")) => true,
            _ => false,
        };

        for (i, select_item) in select_body.projection.iter().enumerate() {
            if i > 0 {
                if single_column_mod {
                    return Err(ConvertionError::new(format!(
                        "single column modifier is ON but the projection contains multiple columns: {:#?}",
                        select_body.projection
                    )))
                } else {
                    self.try_push_state("SELECT_list_multiple_values")?;
                }
            }
            self.try_push_state("SELECT_list")?;
            match (select_item, single_column_mod) {
                (SelectItem::UnnamedExpr(expr), _) => {
                    self.try_push_states(&["SELECT_unnamed_expr", "call54_types"])?;
                    let alias = match expr.unnested() {
                        Expr::Identifier(ident) => Some(ident.clone()),
                        Expr::CompoundIdentifier(idents) => Some(idents.last().unwrap().clone()),
                        _ => None,
                    };
                    column_idents_and_graph_types.push((
                        alias, self.handle_types(expr, None, None)?
                    ));
                },
                (SelectItem::ExprWithAlias { expr, alias }, _) => {
                    self.try_push_states(&["SELECT_expr_with_alias", "call54_types"])?;
                    column_idents_and_graph_types.push((
                        Some(alias.clone()),
                        self.handle_types(expr, None, None)?
                    ));
                },
                (SelectItem::QualifiedWildcard(alias, ..), false) => {
                    self.try_push_state("SELECT_qualified_wildcard")?;
                    let relation = match alias.0.as_slice() {
                        [ident] => self.clause_context.from().get_relation_by_name(ident),
                        any => panic!("schema.table alias is not supported: {}", ObjectName(any.to_vec())),
                    };
                    column_idents_and_graph_types.extend(relation.get_columns_with_types().into_iter());
                },
                (SelectItem::Wildcard(..), false) => {
                    self.try_push_state("SELECT_wildcard")?;
                    column_idents_and_graph_types.extend(self.clause_context
                        .from().get_wildcard_columns().into_iter()
                    );
                },
                _x @ (SelectItem::QualifiedWildcard(..) | SelectItem::Wildcard(..), true) => {
                    return Err(ConvertionError::new(format!(
                        "Wildcards are currently not allowed where single column is required: {:#?}",
                        select_body.projection
                    )))
                },
            }
        }
        self.try_push_state("EXIT_SELECT")?;

        self.try_push_state("EXIT_Query")?;
        self.clause_context.on_query_end();

        return Ok(column_idents_and_graph_types)
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
        if unwrap_variant!(self.state_generator.get_fn_selected_types_unwrapped(), CallTypes::TypeList).len() == 2 {
            todo!(
                "The number subgraph cannot yet choose between types / perform mixed type \
                operations / type casts. It only accepts either integer or numeric, but not both"
            );
        }
        let number_type = match expr {
            Expr::Value(Value::Number(number_str, false)) => {
                self.try_push_state("number_literal")?;
                if number_str.parse::<u64>().is_ok() {
                    self.try_push_state("number_literal_integer")?;
                    SubgraphType::Integer
                } else {
                    self.try_push_state("number_literal_numeric")?;
                    SubgraphType::Numeric
                }
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
                self.handle_types(inner, Some(&[SubgraphType::Numeric, SubgraphType::Integer]), None)?
            },
            any => unexpected_expr!(any),
        };
        self.try_push_state("EXIT_number")?;
        Ok(number_type)
    }

    /// subgraph def_text
    fn handle_text(&mut self, expr: &Expr) -> Result<SubgraphType, ConvertionError> {
        self.try_push_state("text")?;
        match expr {
            Expr::Value(Value::SingleQuotedString(_)) => {
                self.try_push_state("text_literal")?;
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
                let null_type = SubgraphType::from_data_type(data_type);
                if !selected_types.contains(&null_type) {
                    unexpected_subgraph_type!(null_type)
                }
                self.try_push_state(match &null_type {
                    SubgraphType::Integer => "types_select_type_integer",
                    SubgraphType::Numeric => "types_select_type_numeric",
                    SubgraphType::Val3 => "types_select_type_3vl",
                    SubgraphType::Text => "types_select_type_text",
                    SubgraphType::Date => "types_select_type_date",
                    any => unexpected_subgraph_type!(any),
                })?;
                self.try_push_state("types_return_typed_null")?;
                null_type
            },
            expr => {
                let types_before_state_selection = self.get_checkpoint();
                let mut selected_types_iter = selected_types.iter();
                // try out different allowed types to find the actual one (or not)
                loop {
                    match selected_types_iter.next() {
                        Some(subgraph_type) => {
                            self.try_push_state(match subgraph_type {
                                SubgraphType::Integer => "types_select_type_integer",
                                SubgraphType::Numeric => "types_select_type_numeric",
                                SubgraphType::Val3 => "types_select_type_3vl",
                                SubgraphType::Text => "types_select_type_text",
                                SubgraphType::Date => "types_select_type_date",
                                any => unexpected_subgraph_type!(any),
                            })?;
                            let types_after_state_selection = self.get_checkpoint();
                            let ValueSetterValue::TypesTypeValue(allowed_type_list) = self.state_generator.get_named_value::<TypesTypeValue>().unwrap();
                            let allowed_type_list = allowed_type_list.selected_types.clone();
                            match expr {
                                Expr::Subquery(query) => {
                                    if modifiers.contains(&SmolStr::new("no subquery")) {
                                        unexpected_expr!(expr)
                                    }
                                    self.try_push_states(&["types_select_special_expression", "call1_Query"])?;
                                    self.state_generator.set_known_list(allowed_type_list);
                                    match self.handle_query(query) {
                                        Ok(col_type_list) => {
                                            if matches!(col_type_list.as_slice(), [(.., query_subgraph_type)] if query_subgraph_type == subgraph_type) {
                                                break subgraph_type.clone()
                                            } else {
                                                panic!("Query did not return the requested type. Requested: {:?} Got: {:?}", subgraph_type, col_type_list);
                                            }
                                        },
                                        Err(..) => self.restore_checkpoint(&types_before_state_selection),
                                    }
                                }
                                expr => {
                                    match {
                                        if !modifiers.contains(&SmolStr::new("no column spec")) {
                                            match self.try_push_states(&["types_select_special_expression", "types_column_spec"]) {
                                                Ok(..) => {
                                                    if modifiers.contains(&SmolStr::new("check group by")) {
                                                        self.try_push_state("call1_column_spec")?;
                                                    } else {
                                                        self.try_push_state("call0_column_spec")?;
                                                    }
                                                    self.state_generator.set_known_list(allowed_type_list);
                                                    self.handle_column_spec(expr)
                                                },
                                                Err(err) => Err(err),
                                            }
                                        } else {
                                            Err(ConvertionError::new(format!("no column spec is ON")))
                                        }
                                    } {
                                        Ok(col_subgraph_type) => break col_subgraph_type,
                                        Err(..) => {
                                            self.restore_checkpoint(&types_after_state_selection);
                                            match match subgraph_type {
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
                                                        break subgraph_type.clone()
                                                    } else {
                                                        panic!("Subgraph did not return the requested type. Requested: {:?} Got: {:?}", subgraph_type, actual_type);
                                                    }
                                                },
                                                Err(..) => self.restore_checkpoint(&types_before_state_selection),
                                            }
                                        }
                                    }
                                }
                            }
                        },
                        None => return Err(ConvertionError::new(
                            format!("Types didn't find a suitable type for expressionn, among:\n{:#?}\nexpr:\n{:#?}\n", selected_types, expr)
                        )),
                    }
                }
            },
        };
        self.try_push_state("EXIT_types")?;

        // TODO: this code is repeated here and in random query generator.
        // NOTE: Will be solved after out_types are implemented, so that all of this is managed
        // in the state generator
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

    /// subgraph def_column_spec
    fn handle_column_spec(&mut self, expr: &Expr) -> Result<SubgraphType, ConvertionError> {
        self.try_push_state("column_spec")?;
        let column_types = unwrap_variant_or_else!(
            self.state_generator.get_fn_selected_types_unwrapped(), CallTypes::TypeList, || self.state_generator.print_stack()
        );
        let ident_components = match expr {
            Expr::CompoundIdentifier(idents) if idents.len() == 2 => {
                self.try_push_state("qualified_column_name")?;
                idents.clone()
            },
            Expr::Identifier(ident) => {
                self.try_push_state("unqualified_column_name")?;
                vec![ident.clone()]
            },
            any => unexpected_expr!(any),
        };
        let selected_type = self.clause_context.from().get_column_type_by_ident_components(&ident_components);
        if !column_types.contains(&selected_type) {
            return Err(ConvertionError::new(format!(
                "get_column_type_by_ident_components() selected a column ({}) with type {:?}, but expected one of {:?}",
                ObjectName(ident_components), selected_type, column_types
            )))
        }
        self.push_node(PathNode::SelectedColumnName(ident_components.last().unwrap().clone()));
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
}
