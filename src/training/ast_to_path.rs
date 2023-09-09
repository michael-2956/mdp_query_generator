use std::{path::PathBuf, str::FromStr, error::Error, fmt};

use rand::SeedableRng;
use rand_chacha::ChaCha8Rng;
use smol_str::SmolStr;
use sqlparser::{parser::Parser, dialect::PostgreSqlDialect, ast::{Statement, Query, ObjectName, Expr, SetExpr, SelectItem, Value, BinaryOperator}};

use crate::{query_creation::{random_query_generator::query_info::{DatabaseSchema, ClauseContext}, state_generators::{subgraph_type::SubgraphType, state_choosers::MaxProbStateChooser, MarkovChainGenerator, markov_chain_generator::{StateGeneratorConfig, error::SyntaxError, markov_chain::CallModifiers}, dynamic_models::{DeterministicModel, DynamicModel}}}, config::TomlReadable, unwrap_variant};

pub struct SQLTrainer {
    query_asts: Vec<Box<Query>>,
    path_generator: PathGenerator,
}

pub struct TrainingConfig {
    pub training_db_path: PathBuf,
    pub training_schema: PathBuf,
}

impl TomlReadable for TrainingConfig {
    fn from_toml(toml_config: &toml::Value) -> Self {
        let section = &toml_config["training"];
        Self {
            training_db_path: PathBuf::from_str(section["training_db_path"].as_str().unwrap()).unwrap(),
            training_schema: PathBuf::from_str(section["training_schema"].as_str().unwrap()).unwrap(),
        }
    }
}

impl SQLTrainer {
    pub fn with_config(config: TrainingConfig, chain_config: &StateGeneratorConfig) -> Result<Self, SyntaxError> {
        let db = std::fs::read_to_string(config.training_db_path).unwrap();
        Ok(SQLTrainer {
            query_asts: Parser::parse_sql(&PostgreSqlDialect {}, &db).unwrap().into_iter()
                .filter_map(|statement| if let Statement::Query(query) = statement {
                    Some(query)
                } else { None })
                .collect(),
            path_generator: PathGenerator::new(
                DatabaseSchema::parse_schema(&config.training_schema),
                chain_config,
            )?,
        })
    }

    pub fn train(&mut self) -> Result<Vec<Vec<SmolStr>>, ConvertionError> {
        let mut paths = Vec::<_>::new();
        for query in self.query_asts.iter() {
            println!("Converting query {}", query);
            paths.push(self.path_generator.get_query_path(query)?);
        }
        Ok(paths)
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
    fn new(reason: String) -> Self {
        Self { reason }
    }
}

struct PathGenerator {
    current_path: Vec<SmolStr>,
    database_schema: DatabaseSchema,
    state_generator: MarkovChainGenerator<MaxProbStateChooser>,
    state_selector: DeterministicModel,
    clause_context: ClauseContext,
    rng: ChaCha8Rng,
}

impl PathGenerator {
    fn new(database_schema: DatabaseSchema, chain_config: &StateGeneratorConfig) -> Result<Self, SyntaxError> {
        Ok(Self {
            current_path: vec![],
            database_schema,
            state_generator: MarkovChainGenerator::<MaxProbStateChooser>::with_config(chain_config)?,
            state_selector: DeterministicModel::new(),
            clause_context: ClauseContext::new(),
            rng: ChaCha8Rng::seed_from_u64(1),
        })
    }

    fn get_query_path(&mut self, query: &Box<Query>) -> Result<Vec<SmolStr>, ConvertionError> {
        self.handle_query(query)?;
        // reset the generator
        if let Some(state) = self.next_state_opt() {
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

impl PathGenerator {
    fn expect_compat(&self, target: &SubgraphType, compat_with: &SubgraphType) {
        if !target.is_compat_with(compat_with) {
            self.state_generator.print_stack();
            panic!("Incompatible types: expected compatible with {:?}, got {:?}", compat_with, target);
        }
    }

    fn next_state_opt(&mut self) -> Option<SmolStr> {
        self.state_generator.next(&mut self.rng, &self.clause_context, &mut self.state_selector)
    } 

    fn push_state(&mut self, state: &str) {
        let state = SmolStr::new(state);
        self.state_selector.set_state(state.clone());
        if let Some(actual_state) = self.next_state_opt() {
            if actual_state != state {
                panic!("State generator returned {actual_state}, expected {state}");
            }
        } else {
            panic!("State generator stopped prematurely");
        }
        self.current_path.push(state);
    }

    fn push_states(&mut self, state_names: &[&str]) {
        for state_name in state_names {
            self.push_state(state_name);
        }
    }

    /// subgraph def_Query
    fn handle_query(&mut self, query: &Box<Query>) -> Result<Vec<(Option<ObjectName>, SubgraphType)>, ConvertionError> {
        self.clause_context.on_query_begin();
        self.push_state("Query");

        match self.state_generator.get_fn_modifiers() {
            CallModifiers::StaticList(list) if list.contains(&SmolStr::new("single row")) => {
                if query.limit != Some(Expr::Value(Value::Number("1".to_string(), false))) {
                    return Err(ConvertionError::new(format!(
                        "Expected query to have \"LIMIT 1\" because of \'single row\', got {:#?}", query.limit
                    )))
                }
                self.push_state("single_row_true");
            }
            _ => {
                self.push_state("single_row_false");
                if let Some(ref limit) = query.limit {
                    self.push_states(&["limit", "call52_types"]);
                    self.handle_types(limit, Some(&[SubgraphType::Numeric]), None)?;
                }
            }
        }
        self.push_state("FROM");

        let select_body = unwrap_variant!(&*query.body, SetExpr::Select);

        for table_with_joins in select_body.from.iter() {
            match &table_with_joins.relation {
                sqlparser::ast::TableFactor::Table { name, .. } => {
                    self.push_state("Table");
                    let create_table_st = self.database_schema.get_table_def_by_name(name);
                    self.clause_context.from_mut().append_table(create_table_st);
                },
                sqlparser::ast::TableFactor::Derived { subquery, .. } => {
                    self.push_state("call0_Query");
                    let column_idents_and_graph_types = self.handle_query(&subquery)?;
                    self.clause_context.from_mut().append_query(column_idents_and_graph_types);
                },
                any => unexpected_expr!(any)
            }
            self.push_state("FROM_multiple_relations");
        }
        self.push_state("EXIT_FROM");

        if let Some(ref selection) = select_body.selection {
            self.push_state("WHERE");
            self.push_state("call53_types");
            self.handle_types(selection, Some(&[SubgraphType::Val3]), None)?;
        }
        self.push_state("EXIT_WHERE");

        self.push_state("SELECT");
        if select_body.distinct {
            self.push_state("SELECT_DISTINCT");
        }
        self.push_state("SELECT_distinct_end");

        let mut column_idents_and_graph_types = vec![];

        self.push_state("SELECT_projection");

        let single_column_mod = match self.state_generator.get_fn_modifiers() {
            CallModifiers::StaticList(list) if list.contains(&SmolStr::new("single column")) => true,
            _ => false,
        };

        for (i, select_item) in select_body.projection.iter().enumerate() {
            self.push_state("SELECT_list");
            if i > 0 {
                if single_column_mod {
                    return Err(ConvertionError::new(format!(
                        "single column modifier is ON but the projection contains multiple columns: {:#?}",
                        select_body.projection
                    )))
                } else {
                    self.push_state("SELECT_list_multiple_values");
                }
            }
            match (select_item, single_column_mod) {
                (SelectItem::UnnamedExpr(expr), _) => {
                    self.push_states(&["SELECT_unnamed_expr", "call54_types"]);
                    column_idents_and_graph_types.push((
                        None, self.handle_types(expr, None, None)?
                    ));
                },
                (SelectItem::ExprWithAlias { expr, alias }, _) => {
                    self.push_states(&["SELECT_expr_with_alias", "call54_types"]);
                    column_idents_and_graph_types.push((
                        Some(ObjectName(vec![alias.clone()])),
                        self.handle_types(expr, None, None)?
                    ));
                },
                (SelectItem::QualifiedWildcard(alias, ..), false) => {
                    self.push_state("SELECT_qualified_wildcard");
                    let relation = self.clause_context.from().get_relation_by_name(alias);
                    column_idents_and_graph_types.extend(relation.get_columns_with_types().into_iter());
                },
                (SelectItem::Wildcard(..), false) => {
                    self.push_state("SELECT_wildcard");
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
        self.push_state("EXIT_SELECT");

        self.push_state("EXIT_Query");
        self.clause_context.on_query_end();

        return Ok(column_idents_and_graph_types)
    }

    /// subgraph def_VAL_3
    fn handle_val_3(&mut self, expr: &Expr) -> Result<SubgraphType, ConvertionError> {
        self.push_state("VAL_3");

        match expr {
            x @ (Expr::IsNull(expr) | Expr::IsNotNull(expr)) => {
                self.push_state("IsNull");
                if matches!(x, Expr::IsNotNull(..)) { self.push_state("IsNull_not"); }
                self.push_state("call55_types");
                self.handle_types(expr, None, None)?;
            },
            x @ (Expr::IsDistinctFrom(expr_1, expr_2) | Expr::IsNotDistinctFrom(expr_1, expr_2)) =>  {
                self.push_states(&["IsDistinctFrom", "call56_types"]);
                let types_selected_type = self.handle_types(expr_1, None, None)?;
                self.state_generator.set_compatible_list(types_selected_type.get_compat_types());
                if matches!(x, Expr::IsNotDistinctFrom(..)) { self.push_state("IsDistinctNOT"); }
                self.push_states(&["DISTINCT", "call21_types"]);
                self.handle_types(expr_2, None, Some(types_selected_type))?;
            },
            Expr::Exists { subquery, negated } => {
                self.push_state("Exists");
                if *negated { self.push_state("Exists_not"); }
                self.push_state("call2_Query");
                self.handle_query(subquery)?;
            },
            Expr::InList { expr, list, negated } => {
                self.push_states(&["InList", "call57_types"]);
                let types_selected_type = self.handle_types(expr, None, None)?;
                self.state_generator.set_compatible_list(types_selected_type.get_compat_types());
                if *negated { self.push_state("InListNot"); }
                self.push_states(&["InListIn", "call1_list_expr"]);
                self.handle_list_expr(list)?;
            },
            Expr::InSubquery { expr, subquery, negated } => {
                self.push_states(&["InSubquery", "call58_types"]);
                let types_selected_type = self.handle_types(expr, None, None)?;
                self.state_generator.set_compatible_list(types_selected_type.get_compat_types());
                if *negated { self.push_state("InSubqueryNot"); }
                self.push_states(&["InSubqueryIn", "call3_Query"]);
                self.handle_query(subquery)?;
            },
            Expr::Between { expr, negated, low, high } => {
                self.push_states(&["Between", "call59_types"]);
                let types_selected_type = self.handle_types(expr, None, None)?;
                self.state_generator.set_compatible_list(types_selected_type.get_compat_types());
                if *negated { self.push_state("BetweenBetweenNot"); }
                self.push_states(&["BetweenBetween", "call22_types"]);
                self.handle_types(low, None, Some(types_selected_type.clone()))?;
                self.push_states(&["BetweenBetweenAnd", "call23_types"]);
                self.handle_types(high, None, Some(types_selected_type))?;
            },
            Expr::BinaryOp { left, op, right } => {
                self.push_states(&["BinaryComp", "call60_types"]);
                let types_selected_type = self.handle_types(left, None, None)?;
                self.state_generator.set_compatible_list(types_selected_type.get_compat_types());
                self.push_state(match op {
                    BinaryOperator::Eq => "BinaryCompEqual",
                    BinaryOperator::Lt => "BinaryCompLess",
                    BinaryOperator::LtEq => "BinaryCompLessEqual",
                    BinaryOperator::NotEq => "BinaryCompUnEqual",
                    any => unexpected_expr!(any),
                });
                self.push_state("call24_types");
                self.handle_types(right, None, Some(types_selected_type))?;
            },
            any => unexpected_expr!(any),
        }

        self.push_state("EXIT_VAL_3");

        Ok(SubgraphType::Val3)
        //     "AnyAll" => {
        //         self.expect_state("call61_types");
        //         let (types_selected_type, types_value) = self.handle_types(None, None);
        //         self.state_generator.set_compatible_list(types_selected_type.get_compat_types());
        //         self.expect_state("AnyAllSelectOp");
        //         let any_all_op = match self.next_state().as_str() {
        //             "AnyAllEqual" => BinaryOperator::Eq,
        //             "AnyAllLess" => BinaryOperator::Lt,
        //             "AnyAllLessEqual" => BinaryOperator::LtEq,
        //             "AnyAllUnEqual" => BinaryOperator::NotEq,
        //             any => self.panic_unexpected(any)
        //         };
        //         self.expect_state("AnyAllSelectIter");
        //         let iterable = Box::new(match self.next_state().as_str() {
        //             "call4_Query" => Expr::Subquery(Box::new(self.handle_query().0)),
        //             any => self.panic_unexpected(any)
        //         });
        //         self.expect_state("AnyAllAnyAll");
        //         let iterable = Box::new(match self.next_state().as_str() {
        //             "AnyAllAnyAllAll" => Expr::AllOp(iterable),
        //             "AnyAllAnyAllAny" => Expr::AnyOp(iterable),
        //             any => self.panic_unexpected(any),
        //         });
        //         Expr::BinaryOp {
        //             left: Box::new(types_value),
        //             op: any_all_op,
        //             right: iterable,
        //         }
        //     },
        //     "BinaryBooleanOpV3" => {
        //         self.expect_state("call27_types");
        //         let types_value_1 = self.handle_types(Some(&[SubgraphType::Val3]), None).1;
        //         let binary_bool_op = match self.next_state().as_str() {
        //             "BinaryBooleanOpV3AND" => BinaryOperator::And,
        //             "BinaryBooleanOpV3OR" => BinaryOperator::Or,
        //             "BinaryBooleanOpV3XOR" => BinaryOperator::Xor,
        //             any => self.panic_unexpected(any)
        //         };
        //         self.expect_state("call28_types");
        //         let types_value_2 = self.handle_types(Some(&[SubgraphType::Val3]), None).1;
        //         Expr::BinaryOp {
        //             left: Box::new(types_value_1),
        //             op: binary_bool_op,
        //             right: Box::new(types_value_2)
        //         }
        //     },
        //     "BinaryStringLike" => {
        //         self.expect_state("call25_types");
        //         let types_value_1 = self.handle_types(Some(&[SubgraphType::Text]), None).1;
        //         let string_like_not_flag = match self.next_state().as_str() {
        //             "BinaryStringLikeNot" => {
        //                 self.expect_state("BinaryStringLikeIn");
        //                 true
        //             }
        //             "BinaryStringLikeIn" => false,
        //             any => self.panic_unexpected(any)
        //         };
        //         self.expect_state("call26_types");
        //         let types_value_2 = self.handle_types(Some(&[SubgraphType::Text]), None).1;
        //         Expr::Like {
        //             negated: string_like_not_flag,
        //             expr: Box::new(types_value_1),
        //             pattern: Box::new(types_value_2),
        //             escape_char: None
        //         }
        //     },
        //     "true" => Expr::Value(Value::Boolean(true)),
        //     "false" => Expr::Value(Value::Boolean(false)),
        //     "Nested_VAL_3" => {
        //         self.expect_state("call29_types");
        //         Expr::Nested(Box::new(self.handle_types(Some(&[SubgraphType::Val3]), None).1))
        //     },
        //     "UnaryNot_VAL_3" => {
        //         self.expect_state("call30_types");
        //         Expr::UnaryOp { op: UnaryOperator::Not, expr: Box::new( self.handle_types(
        //             Some(&[SubgraphType::Val3]), None
        //         ).1) }
        //     },
        //     any => self.panic_unexpected(any)
        // };
        // self.expect_state("EXIT_VAL_3");
        // (SubgraphType::Val3, val3)
    }

    /// subgraph def_types
    fn handle_types(
        &mut self,
        _expr: &Expr,
        check_generated_by_one_of: Option<&[SubgraphType]>,
        check_compatible_with: Option<SubgraphType>,
    ) -> Result<SubgraphType, ConvertionError> {
        let selected_type = SubgraphType::Undetermined;

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

        return Err(ConvertionError::new(format!("subgraph def_types not yet implemented")))
    }

    /// subgraph def_list_expr
    fn handle_list_expr(&mut self, list: &Vec<Expr>) -> Result<SubgraphType, ConvertionError> {
        self.push_states(&["list_expr", "call16_types"]);
        let inner_type = self.handle_types(&list[0], None, None)?;
        self.push_state("list_expr_multiple_values");
        self.state_generator.set_compatible_list(inner_type.get_compat_types());
        for expr in list.iter().skip(1) {
            self.push_state("call49_types");
            self.handle_types(expr, None, Some(inner_type.clone()))?;
        }
        self.push_state("EXIT_list_expr");
        Ok(SubgraphType::ListExpr(Box::new(inner_type)))
    }
}
