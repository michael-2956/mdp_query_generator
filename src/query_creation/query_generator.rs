#[macro_use]
pub mod query_info;
pub mod call_modifiers;
pub mod value_choosers;
pub mod expr_precedence;
mod aggregate_function_settings;

use std::path::PathBuf;

use rand::SeedableRng;
use rand_chacha::ChaCha8Rng;
use smol_str::SmolStr;
use sqlparser::ast::{
    Expr, Ident, Query, Select, SetExpr, TableFactor,
    TableWithJoins, Value, BinaryOperator, UnaryOperator, TrimWhereField, SelectItem, WildcardAdditionalOptions, DataType, ObjectName,
    FunctionArg, self, FunctionArgExpr,
};

use crate::config::TomlReadable;

use super::{super::{unwrap_variant, unwrap_variant_or_else}, state_generator::{CallTypes, markov_chain_generator::subgraph_type::SubgraphType}};
use self::{query_info::{DatabaseSchema, ClauseContext}, aggregate_function_settings::{AggregateFunctionDistribution, AggregateFunctionAgruments}, expr_precedence::ExpressionPriority, call_modifiers::{TypesTypeValue, ValueSetterValue, WildcardRelationsValue}, value_choosers::QueryValueChooser};

use super::state_generator::{MarkovChainGenerator, dynamic_models::DynamicModel, state_choosers::StateChooser};

#[derive(Debug, Clone)]
pub struct QueryGeneratorConfig {
    pub print_queries: bool,
    pub print_schema: bool,
    pub table_schema_path: PathBuf,
    pub dynamic_model_name: String,
    pub aggregate_functions_distribution: AggregateFunctionDistribution,
}

impl TomlReadable for QueryGeneratorConfig {
    fn from_toml(toml_config: &toml::Value) -> Self {
        let section = &toml_config["query_generator"];
        Self {
            print_queries: section["print_queries"].as_bool().unwrap(),
            print_schema: section["print_schema"].as_bool().unwrap(),
            table_schema_path: PathBuf::from(section["table_schema_path"].as_str().unwrap()),
            dynamic_model_name: section["dynamic_model"].as_str().unwrap().to_string(),
            aggregate_functions_distribution: AggregateFunctionDistribution::parse_file(
                PathBuf::from(section["aggregate_functions_distribution_map_file"].as_str().unwrap()),
            ),
        }
    }
}

pub struct QueryGenerator<DynMod: DynamicModel, StC: StateChooser, QVC: QueryValueChooser> {
    config: QueryGeneratorConfig,
    state_generator: MarkovChainGenerator<StC>,
    dynamic_model: Box<DynMod>,
    value_chooser: Box<QVC>,
    database_schema: DatabaseSchema,
    clause_context: ClauseContext,
    free_projection_alias_index: u32,
    rng: ChaCha8Rng,
}

pub trait Unnested {
    fn unnested(&self) -> &Expr;
}

impl Unnested for Expr {
    fn unnested(&self) -> &Expr {
        match self {
            Expr::Nested(expr) => expr.unnested(),
            any => any,
        }
    }
}

impl<DynMod: DynamicModel, StC: StateChooser, QVC: QueryValueChooser> QueryGenerator<DynMod, StC, QVC> {
    pub fn from_state_generator_and_schema(state_generator: MarkovChainGenerator<StC>, config: QueryGeneratorConfig) -> Self {
        let mut _self = QueryGenerator::<DynMod, StC, QVC> {
            state_generator,
            dynamic_model: Box::new(DynMod::new()),
            database_schema: DatabaseSchema::parse_schema(&config.table_schema_path),
            value_chooser: Box::new(QVC::new()),
            config,
            clause_context: ClauseContext::new(),
            free_projection_alias_index: 1,
            rng: ChaCha8Rng::seed_from_u64(1),
        };

        if _self.config.print_schema {
            println!("Relations:\n{}", _self.database_schema);
        }

        _self
    }

    fn next_state_opt(&mut self) -> Option<SmolStr> {
        self.state_generator.next(&mut self.rng, &mut self.clause_context, &mut *self.dynamic_model).unwrap()
    }

    fn next_state(&mut self) -> SmolStr {
        // let r = self.next_state_opt().unwrap();
        // println!("{r}");
        // r
        self.next_state_opt().unwrap()
    }

    fn panic_unexpected(&mut self, state: &str) -> ! {
        self.state_generator.print_stack();
        panic!("Unexpected state: {state}");
    }

    fn expect_state(&mut self, state: &str) {
        let new_state = self.next_state();
        if new_state.as_str() != state {
            self.state_generator.print_stack();
            panic!("Expected {state}, got {new_state}")
        }
    }

    fn expect_states(&mut self, states: &[&str]) {
        for state in states {
            self.expect_state(*state)
        }
    }

    fn expect_compat(&self, target: &SubgraphType, compat_with: &SubgraphType) {
        if !target.is_compat_with(compat_with) {
            self.state_generator.print_stack();
            panic!("Incompatible types: expected compatible with {:?}, got {:?}", compat_with, target);
        }
    }

    fn gen_select_alias(&mut self) -> Ident {
        let name = format!("C{}", self.free_projection_alias_index);
        self.free_projection_alias_index += 1;
        Ident { value: name.clone(), quote_style: None }
    }

    /// subgraph def_Query
    fn handle_query(&mut self) -> (Query, Vec<(Option<Ident>, SubgraphType)>) {
        self.dynamic_model.notify_subquery_creation_begin();
        self.clause_context.on_query_begin();
        self.expect_state("Query");

        let mut select_body = Select {
            distinct: false,
            top: None,
            projection: vec![],
            into: None,
            from: vec![],
            lateral_views: vec![],
            selection: None,
            group_by: vec![],
            cluster_by: vec![],
            distribute_by: vec![],
            sort_by: vec![],
            having: None,
            qualify: None,
        };

        self.expect_state("call0_FROM");
        select_body.from = self.handle_from();
        match self.next_state().as_str() {
            "call0_WHERE" => {
                select_body.selection = Some(self.handle_where().1);
                match self.next_state().as_str() {
                    "call0_SELECT" => {},
                    "call0_GROUP_BY" => {
                        select_body.group_by = self.handle_group_by(); 
                        match self.next_state().as_str() {
                            "call0_SELECT" => {},
                            "call0_HAVING" => {
                                select_body.having = Some(self.handle_having().1);
                                self.expect_state("call0_SELECT");
                            },
                            any => self.panic_unexpected(any),  
                        }
                    },
                    any => self.panic_unexpected(any),  
                }

            },
            "call0_SELECT" => {},
            "call0_GROUP_BY" => {
                select_body.group_by = self.handle_group_by();
                match self.next_state().as_str() {
                    "call0_SELECT" => {},
                    "call0_HAVING" => {
                        select_body.having = Some(self.handle_having().1);
                        self.expect_state("call0_SELECT");
                    },
                    any => self.panic_unexpected(any),  
                }

            }
            any => self.panic_unexpected(any),
        }

        let (
            mut column_idents_and_graph_types,
            (distinct, mut projection)
        ) = self.handle_select();
        select_body.distinct = distinct;
        std::mem::swap(&mut select_body.projection, &mut projection);

        let select_limit = match self.next_state().as_str() {
            "query_can_skip_limit" => None,
            "call0_LIMIT" => Some(self.handle_limit().1),
            any => self.panic_unexpected(any),
        };

        self.expect_state("EXIT_Query");
        self.dynamic_model.notify_subquery_creation_end();
        self.clause_context.on_query_end();

        // select pg_typeof((select null)); -- returns text
        for (_, column_type) in column_idents_and_graph_types.iter_mut() {
            if *column_type == SubgraphType::Undetermined {
                *column_type = SubgraphType::Text;
            }
        }

        (Query {
            with: None,
            body: Box::new(SetExpr::Select(Box::new(select_body))),
            order_by: vec![],
            limit: select_limit,
            offset: None,
            fetch: None,
            locks: vec![],
        }, column_idents_and_graph_types)
    }

    fn handle_from(&mut self) -> Vec<TableWithJoins> {
        self.expect_state("FROM");

        let mut from: Vec<TableWithJoins> = vec![];

        loop {
            from.push(TableWithJoins { relation: match self.next_state().as_str() {
                "Table" => {
                    let create_table_st = self.value_chooser.choose_table(&self.database_schema);
                    let alias = self.clause_context.from_mut().append_table(create_table_st);
                    TableFactor::Table {
                        name: create_table_st.name.clone(),
                        alias: Some(alias),
                        args: None,
                        with_hints: vec![],
                        columns_definition: None,
                    }
                },
                "call0_Query" => {
                    let (query, column_idents_and_graph_types) = self.handle_query();
                    let alias = self.clause_context.from_mut().append_query(column_idents_and_graph_types);
                    TableFactor::Derived {
                        lateral: false,
                        subquery: Box::new(query),
                        alias: Some(alias)
                    }
                },
                "EXIT_FROM" => break,
                any => self.panic_unexpected(any)
            }, joins: vec![] });
            self.expect_state("FROM_multiple_relations");
        }

        from
    }

    fn handle_where(&mut self) -> (SubgraphType, Expr) {
        self.expect_state("WHERE");
        self.expect_state("call53_types");
        let (selection_type, selection) = self.handle_types(Some(&[SubgraphType::Val3]), None);
        self.expect_state("EXIT_WHERE");
        (selection_type, selection)
    }

    /// subgraph def_SELECT
    fn handle_select(&mut self) -> (Vec<(Option<Ident>, SubgraphType)>, (bool, Vec<SelectItem>)) {
        self.expect_state("SELECT");
        let distinct = match self.next_state().as_str() {
            "SELECT_DISTINCT" => {
                self.expect_state("SELECT_list");
                true
            },
            "SELECT_list" => false,
            any => self.panic_unexpected(any)
        };

        let mut column_idents_and_graph_types = vec![];
        let mut projection = vec![];

        loop {
            match self.next_state().as_str() {
                "SELECT_tables_eligible_for_wildcard" => {
                    match self.next_state().as_str() {
                        "SELECT_wildcard" => {
                            column_idents_and_graph_types.extend(self.clause_context
                                .from().get_wildcard_columns().into_iter()
                            );
                            projection.push(SelectItem::Wildcard(WildcardAdditionalOptions {
                                opt_exclude: None, opt_except: None, opt_rename: None,
                            }));
                            continue;
                        },
                        "SELECT_qualified_wildcard" => {
                            let from_contents = self.clause_context.from();
                            let wildcard_relations = unwrap_variant!(self.state_generator.get_named_value::<WildcardRelationsValue>().unwrap(), ValueSetterValue::WildcardRelations);
                            let (alias, relation) = self.value_chooser.choose_qualified_wildcard_relation(
                                from_contents, wildcard_relations
                            );
                            column_idents_and_graph_types.extend(relation.get_columns_with_types().into_iter());
                            projection.push(SelectItem::QualifiedWildcard(
                                ObjectName(vec![alias]),
                                WildcardAdditionalOptions {
                                    opt_exclude: None, opt_except: None, opt_rename: None,
                                }
                            ));
                        },
                        any => self.panic_unexpected(any)
                    }
                },
                alias_node @ ("SELECT_unnamed_expr" | "SELECT_expr_with_alias") => {
                    self.expect_state("select_expr");
                    match self.next_state().as_str() {
                        "call73_types" => { },
                        "call54_types" => { },
                        any => self.panic_unexpected(any)
                    };
                    let (subgraph_type, expr) = self.handle_types(None, None);
                    self.expect_state("select_expr_done");
                    let (alias, select_item) = match alias_node {
                        "SELECT_unnamed_expr" => {
                            let alias = match &expr.unnested() {
                                Expr::Identifier(ident) => Some(ident.clone()),
                                Expr::CompoundIdentifier(idents) => Some(idents.last().unwrap().clone()),
                                // unnnamed aggregation can be referred to by function name in postgres
                                Expr::Function(func) => Some(func.name.0.last().unwrap().clone()),
                                _ => None,
                            };
                            (alias, SelectItem::UnnamedExpr(expr))
                        },
                        "SELECT_expr_with_alias" => {
                            let select_alias = self.gen_select_alias();
                            (Some(select_alias.clone()), SelectItem::ExprWithAlias {
                                expr, alias: select_alias.clone(),
                            })
                        },
                        any => self.panic_unexpected(any)
                    };
                    projection.push(select_item);
                    column_idents_and_graph_types.push((alias, subgraph_type));
                },
                any => self.panic_unexpected(any)
            };
            match self.next_state().as_str() {
                "SELECT_list_multiple_values" => self.expect_state("SELECT_list"),
                "EXIT_SELECT" => break,
                any => self.panic_unexpected(any)
            };
        }

        (column_idents_and_graph_types, (distinct, projection))
    }

    /// subgraph def_LIMIT
    fn handle_limit(&mut self) -> (SubgraphType, Expr) {
        self.expect_state("LIMIT");

        let (limit_type, limit) = match self.next_state().as_str() {
            "single_row_true" => {
                (SubgraphType::Integer, Expr::Value(Value::Number("1".to_string(), false)))
            },
            "limit_num" => {
                self.expect_state("call52_types");
                self.handle_types(Some(&[SubgraphType::Numeric, SubgraphType::Integer]), None)
            },
            any => self.panic_unexpected(any)
        };
        self.expect_state("EXIT_LIMIT");

        (limit_type, limit)
    }

    /// subgraph def_VAL_3
    fn handle_val_3(&mut self) -> (SubgraphType, Expr) {
        self.expect_state("VAL_3");
        let val3 = match self.next_state().as_str() {
            "IsNull" => {
                let is_null_not_flag = match self.next_state().as_str() {
                    "IsNull_not" => {
                        self.expect_state("call55_types");
                        true
                    }
                    "call55_types" => false,
                    any => self.panic_unexpected(any)
                };
                let types_value = Box::new(self.handle_types(None, None).1);
                if is_null_not_flag {
                    Expr::IsNotNull(types_value)
                } else {
                    Expr::IsNull(types_value)
                }
            },
            "IsDistinctFrom" => {
                self.expect_state("call56_types");
                let (types_selected_type, types_value_1) = self.handle_types(None, None);
                self.state_generator.set_compatible_list(types_selected_type.get_compat_types());
                let is_distinct_not_flag = match self.next_state().as_str() {
                    "IsDistinctNOT" => {
                        self.expect_state("DISTINCT");
                        true
                    }
                    "DISTINCT" => false,
                    any => self.panic_unexpected(any)
                };
                self.expect_state("call21_types");
                let types_value_2 = self.handle_types(None, Some(types_selected_type)).1;
                if is_distinct_not_flag {
                    Expr::IsNotDistinctFrom(Box::new(types_value_1), Box::new(types_value_2))
                } else {
                    Expr::IsDistinctFrom(Box::new(types_value_1), Box::new(types_value_2))
                }
            },
            "Exists" => {
                let exists_not_flag = match self.next_state().as_str() {
                    "Exists_not" => {
                        self.expect_state("call2_Query");
                        true
                    },
                    "call2_Query" => false,
                    any => self.panic_unexpected(any)
                };
                Expr::Exists {
                    subquery: Box::new(self.handle_query().0),
                    negated: exists_not_flag
                }
            },
            "InList" => {
                self.expect_state("call57_types");
                let (types_selected_type, types_value) = self.handle_types(None, None);
                self.state_generator.set_compatible_list(types_selected_type.get_compat_types());
                let in_list_not_flag = match self.next_state().as_str() {
                    "InListNot" => {
                        self.expect_state("InListIn");
                        true
                    },
                    "InListIn" => false,
                    any => self.panic_unexpected(any)
                };
                self.expect_state("call1_list_expr");
                Expr::InList {
                    expr: Box::new(types_value),
                    list: self.handle_list_expr().1,
                    negated: in_list_not_flag
                }
            },
            "InSubquery" => {
                self.expect_state("call58_types");
                let (types_selected_type, types_value) = self.handle_types(None, None);
                self.state_generator.set_compatible_list(types_selected_type.get_compat_types());
                let in_subquery_not_flag = match self.next_state().as_str() {
                    "InSubqueryNot" => {
                        self.expect_state("InSubqueryIn");
                        true
                    },
                    "InSubqueryIn" => false,
                    any => self.panic_unexpected(any)
                };
                self.expect_state("call3_Query");
                let query = self.handle_query().0;
                Expr::InSubquery {
                    expr: Box::new(types_value),
                    subquery: Box::new(query),
                    negated: in_subquery_not_flag
                }
            },
            "Between" => {
                self.expect_state("call59_types");
                let (types_selected_type, types_value_1) = self.handle_types(None, None);
                self.state_generator.set_compatible_list(types_selected_type.get_compat_types());
                let between_not_flag = match self.next_state().as_str() {
                    "BetweenBetweenNot" => {
                        self.expect_state("BetweenBetween");
                        true
                    },
                    "BetweenBetween" => false,
                    any => self.panic_unexpected(any)
                };
                self.expect_state("call22_types");
                let types_value_2 = self.handle_types(None, Some(types_selected_type.clone())).1;
                self.expect_state("BetweenBetweenAnd");
                self.expect_state("call23_types");
                let types_value_3 = self.handle_types(None, Some(types_selected_type)).1;
                Expr::Between {
                    expr: Box::new(types_value_1),
                    negated: between_not_flag,
                    low: Box::new(types_value_2),
                    high: Box::new(types_value_3)
                }
            },
            "BinaryComp" => {
                self.expect_state("call60_types");
                let (types_selected_type, types_value_1) = self.handle_types(None, None);
                self.state_generator.set_compatible_list(types_selected_type.get_compat_types());
                let binary_comp_op = match self.next_state().as_str() {
                    "BinaryCompEqual" => BinaryOperator::Eq,
                    "BinaryCompLess" => BinaryOperator::Lt,
                    "BinaryCompLessEqual" => BinaryOperator::LtEq,
                    "BinaryCompUnEqual" => BinaryOperator::NotEq,
                    any => self.panic_unexpected(any)
                };
                self.expect_state("call24_types");
                let types_value_2 = self.handle_types(None, Some(types_selected_type)).1;
                Expr::BinaryOp {
                    left: Box::new(types_value_1),
                    op: binary_comp_op,
                    right: Box::new(types_value_2)
                }
            },
            "AnyAll" => {
                self.expect_state("call61_types");
                let (types_selected_type, types_value) = self.handle_types(None, None);
                self.state_generator.set_compatible_list(types_selected_type.get_compat_types());
                self.expect_state("AnyAllSelectOp");
                let any_all_op = match self.next_state().as_str() {
                    "AnyAllEqual" => BinaryOperator::Eq,
                    "AnyAllLess" => BinaryOperator::Lt,
                    "AnyAllLessEqual" => BinaryOperator::LtEq,
                    "AnyAllUnEqual" => BinaryOperator::NotEq,
                    any => self.panic_unexpected(any)
                };
                self.expect_state("AnyAllSelectIter");
                let iterable = Box::new(match self.next_state().as_str() {
                    "call4_Query" => Expr::Subquery(Box::new(self.handle_query().0)),
                    any => self.panic_unexpected(any)
                });
                self.expect_state("AnyAllAnyAll");
                let iterable = Box::new(match self.next_state().as_str() {
                    "AnyAllAnyAllAll" => Expr::AllOp(iterable),
                    "AnyAllAnyAllAny" => Expr::AnyOp(iterable),
                    any => self.panic_unexpected(any),
                });
                Expr::BinaryOp {
                    left: Box::new(types_value),
                    op: any_all_op,
                    right: iterable,
                }
            },
            "BinaryStringLike" => {
                self.expect_state("call25_types");
                let types_value_1 = self.handle_types(Some(&[SubgraphType::Text]), None).1;
                let string_like_not_flag = match self.next_state().as_str() {
                    "BinaryStringLikeNot" => {
                        self.expect_state("BinaryStringLikeIn");
                        true
                    }
                    "BinaryStringLikeIn" => false,
                    any => self.panic_unexpected(any)
                };
                self.expect_state("call26_types");
                let types_value_2 = self.handle_types(Some(&[SubgraphType::Text]), None).1;
                Expr::Like {
                    negated: string_like_not_flag,
                    expr: Box::new(types_value_1),
                    pattern: Box::new(types_value_2),
                    escape_char: None
                }
            },
            "BinaryBooleanOpV3" => {
                self.expect_state("call27_types");
                let types_value_1 = self.handle_types(Some(&[SubgraphType::Val3]), None).1;
                let binary_bool_op = match self.next_state().as_str() {
                    "BinaryBooleanOpV3AND" => BinaryOperator::And,
                    "BinaryBooleanOpV3OR" => BinaryOperator::Or,
                    "BinaryBooleanOpV3XOR" => BinaryOperator::Xor,
                    any => self.panic_unexpected(any)
                };
                self.expect_state("call28_types");
                let types_value_2 = self.handle_types(Some(&[SubgraphType::Val3]), None).1;
                Expr::BinaryOp {
                    left: Box::new(types_value_1),
                    op: binary_bool_op,
                    right: Box::new(types_value_2)
                }
            },
            "true" => Expr::Value(Value::Boolean(true)),
            "false" => Expr::Value(Value::Boolean(false)),
            "Nested_VAL_3" => {
                self.expect_state("call29_types");
                Expr::Nested(Box::new(self.handle_types(Some(&[SubgraphType::Val3]), None).1))
            },
            "UnaryNot_VAL_3" => {
                self.expect_state("call30_types");
                Expr::UnaryOp { op: UnaryOperator::Not, expr: Box::new( self.handle_types(
                    Some(&[SubgraphType::Val3]), None
                ).1) }
            },
            any => self.panic_unexpected(any)
        };
        self.expect_state("EXIT_VAL_3");
        (SubgraphType::Val3, val3)
    }

    /// subgraph def_numeric
    fn handle_number(&mut self) -> (SubgraphType, Expr) {
        self.expect_state("number");
        if unwrap_variant!(self.state_generator.get_fn_selected_types_unwrapped(), CallTypes::TypeList).len() == 2 {
            todo!(
                "The number subgraph cannot yet choose between types / perform mixed type \
                operations / type casts. It only accepts either integer or numeric, but not both"
            );
        }
        let (number_type, number) = match self.next_state().as_str() {
            "number_literal" => {
                let (number_type, number_str) = match self.next_state().as_str() {
                    "number_literal_integer" => {
                        (SubgraphType::Integer, self.value_chooser.choose_integer())
                    },
                    "number_literal_numeric" => {
                        (SubgraphType::Numeric, (self.value_chooser.choose_numeric()))
                    },
                    any => self.panic_unexpected(any)
                };
                (number_type, Expr::Value(Value::Number(number_str, false)))
            },
            "BinaryNumberOp" => {
                self.expect_state("call48_types");
                let (number_type, types_value_1) = self.handle_types(None, None);
                let numeric_binary_op = match self.next_state().as_str() {
                    "binary_number_bin_and" => BinaryOperator::BitwiseAnd,
                    "binary_number_bin_or" => BinaryOperator::BitwiseOr,
                    "binary_number_bin_xor" => BinaryOperator::PGBitwiseXor,
                    "binary_number_exp" => BinaryOperator::PGExp,
                    "binary_number_div" => BinaryOperator::Divide,
                    "binary_number_minus" => BinaryOperator::Minus,
                    "binary_number_mul" => BinaryOperator::Multiply,
                    "binary_number_plus" => BinaryOperator::Plus,
                    any => self.panic_unexpected(any),
                };
                self.expect_state("call47_types");
                let types_value_2 = self.handle_types(None, None).1;
                (number_type, Expr::BinaryOp {
                    left: Box::new(types_value_1),
                    op: numeric_binary_op,
                    right: Box::new(types_value_2)
                })
            },
            "UnaryNumberOp" => {
                let numeric_unary_op = match self.next_state().as_str() {
                    "unary_number_abs" => UnaryOperator::PGAbs,
                    "unary_number_bin_not" => UnaryOperator::PGBitwiseNot,
                    "unary_number_cub_root" => UnaryOperator::PGCubeRoot,
                    "unary_number_minus" => UnaryOperator::Minus,
                    "unary_number_plus" => UnaryOperator::Plus,
                    "unary_number_sq_root" => UnaryOperator::PGSquareRoot,
                    any => self.panic_unexpected(any),
                };
                self.expect_state("call1_types");
                let (number_type, number) = self.handle_types(None, None);
                (number_type, Expr::UnaryOp {
                    op: numeric_unary_op,
                    expr: Box::new(number)
                })
            },
            "number_string_position" => {
                self.expect_state("call2_types");
                let types_value_1 = self.handle_types(Some(&[SubgraphType::Text]), None).1;
                self.expect_state("string_position_in");
                self.expect_state("call3_types");
                let types_value_2 = self.handle_types(Some(&[SubgraphType::Text]), None).1;
                (SubgraphType::Integer, Expr::Position {
                    expr: Box::new(types_value_1),
                    r#in: Box::new(types_value_2)
                })
            },
            "nested_number" => {
                self.expect_state("call4_types");
                let (number_type, number) = self.handle_types(
                    Some(&[SubgraphType::Numeric, SubgraphType::Integer]), None
                );
                (number_type, Expr::Nested(Box::new(number)))
            },
            any => self.panic_unexpected(any)
        };
        self.expect_state("EXIT_number");
        (number_type, number)
    }

    /// subgraph def_text
    fn handle_text(&mut self) -> (SubgraphType, Expr) {
        self.expect_state("text");
        let string = match self.next_state().as_str() {
            "text_literal" => Expr::Value(Value::SingleQuotedString("HJeihfbwei".to_string())),  // TODO: hardcoded
            "text_nested" => {
                self.expect_state("call62_types");
                Expr::Nested(Box::new(
                    self.handle_types(Some(&[SubgraphType::Text]), None).1
                ))
            },
            "text_trim" => {
                let (trim_where, trim_what) = match self.next_state().as_str() {
                    "call6_types" => {
                        let types_value = self.handle_types(Some(&[SubgraphType::Text]), None).1;
                        let spec_mode = match self.next_state().as_str() {
                            "BOTH" => TrimWhereField::Both,
                            "LEADING" => TrimWhereField::Leading,
                            "TRAILING" => TrimWhereField::Trailing,
                            any => self.panic_unexpected(any)
                        };
                        self.expect_state("call5_types");
                        (Some(spec_mode), Some(Box::new(types_value)))
                    },
                    "call5_types" => (None, None),
                    any => self.panic_unexpected(any)
                };
                let types_value = self.handle_types(Some(&[SubgraphType::Text]), None).1;
                Expr::Trim {
                    expr: Box::new(types_value), trim_where, trim_what
                }
            },
            "text_concat" => {
                self.expect_state("call7_types");
                let types_value_1 = self.handle_types(Some(&[SubgraphType::Text]), None).1;
                self.expect_state("text_concat_concat");
                self.expect_state("call8_types");
                let types_value_2 = self.handle_types(Some(&[SubgraphType::Text]), None).1;
                Expr::BinaryOp {
                    left: Box::new(types_value_1),
                    op: BinaryOperator::StringConcat,
                    right: Box::new(types_value_2)
                }
            },
            "text_substring" => {
                self.expect_state("call9_types");
                let target_string = self.handle_types(Some(&[SubgraphType::Text]), None).1;
                let mut substring_from = None;
                let mut substring_for = None;
                loop {
                    match self.next_state().as_str() {
                        "text_substring_from" => {
                            self.expect_state("call10_types");
                            substring_from = Some(Box::new(self.handle_types(Some(&[SubgraphType::Integer]), None).1));
                        },
                        "text_substring_for" => {
                            self.expect_state("call11_types");
                            substring_for = Some(Box::new(self.handle_types(Some(&[SubgraphType::Integer]), None).1));
                            self.expect_state("text_substring_end");
                            break;
                        },
                        "text_substring_end" => break,
                        any => self.panic_unexpected(any),
                    }
                }
                Expr::Substring {
                    expr: Box::new(target_string),
                    substring_from,
                    substring_for,
                }
            },
            any => self.panic_unexpected(any)
        };
        self.expect_state("EXIT_text");
        (SubgraphType::Text, string)
    }

    /// subgarph def_date
    fn handle_date(&mut self) -> (SubgraphType, Expr) {
        self.expect_state("date");
        self.expect_state("date_literal");
        let date = Expr::TypedString {
            data_type: DataType::Date,
            value: "2023-08-27".to_string(),
        };
        self.expect_state("EXIT_date");
        (SubgraphType::Date, date)
    }

    /// subgraph def_types
    fn handle_types(
        &mut self, check_generated_by_one_of: Option<&[SubgraphType]>, check_compatible_with: Option<SubgraphType>
    ) -> (SubgraphType, Expr) {
        self.expect_state("types");
        match self.next_state().as_str() {
            "types_select_type_integer" |
            "types_select_type_numeric" |
            "types_select_type_3vl" |
            "types_select_type_text" |
            "types_select_type_date" => {},
            "types_null" => {
                self.expect_state("EXIT_types");
                return (SubgraphType::Undetermined, Expr::Value(Value::Null));
            },
            any => self.panic_unexpected(any),
        };

        let allowed_type_list = unwrap_variant!(
            self.state_generator.get_named_value::<TypesTypeValue>().unwrap(),
            ValueSetterValue::TypesType
        );
        let allowed_type_list = allowed_type_list.selected_types.clone();

        let (selected_type, types_value) = match self.next_state().as_str() {
            "types_return_typed_null" => {
                let null_type = {
                    let allowed_type_list = allowed_type_list.into_iter()
                        .filter(|x| !x.has_inner()).collect::<Vec<_>>();
                    match allowed_type_list.as_slice() {
                        [tp] => tp.clone(),
                        any => panic!("allowed_type_list must have single element here (got {:?})", any)
                    }
                };
                (null_type.clone(), Expr::Cast {
                    expr: Box::new(Expr::Value(Value::Null)),
                    data_type: null_type.to_data_type(),
                })
            }
            "types_select_special_expression" => {
                match self.next_state().as_str() {
                    "call0_column_spec" => {
                        self.state_generator.set_known_list(allowed_type_list);
                        self.handle_column_spec()
                    }
                    "call1_Query" => {
                        self.state_generator.set_known_list(allowed_type_list);
                        let (subquery, column_types) = self.handle_query();
                        let selected_type = match column_types.len() {
                            1 => column_types[0].1.clone(),
                            any => panic!("Subquery should have selected a single column, but selected {any}. Subquery: {subquery}"),
                        };
                        (selected_type, Expr::Subquery(Box::new(subquery)))
                    },
                    "call0_aggregate_function" => {
                        self.state_generator.set_known_list(allowed_type_list);
                        let return_val = self.handle_aggregate_function();
                        (return_val.0, return_val.1)
                    }
                    any => self.panic_unexpected(any)
                }
            },
            "call1_number" => self.handle_number(),
            "call0_number" => self.handle_number(),
            "call1_VAL_3" => self.handle_val_3(),
            "call0_text" => self.handle_text(),
            "call0_date" => self.handle_date(),
            any => self.panic_unexpected(any)
        };
        self.expect_state("EXIT_types");

        if let Some(generators) = check_generated_by_one_of {
            if !generators.iter().any(|as_what| selected_type.is_same_or_more_determined_or_undetermined(&as_what)) {
                self.state_generator.print_stack();
                // panic!("Unexpected type: expected one of {:?}, got {:?}", generators, selected_type);
                panic!("Unexpected type: expected one of {:?}, got {:?}, {:?}", generators, selected_type, types_value);
            }
        }
        if let Some(with) = check_compatible_with {
            self.expect_compat(&selected_type, &with);
        }
        (selected_type, types_value.nest_children_if_needed())
    }

    /// subgraph def_column_spec
    fn handle_column_spec(&mut self) -> (SubgraphType, Expr) {
        self.expect_state("column_spec");
        let column_types = unwrap_variant_or_else!(
            self.state_generator.get_fn_selected_types_unwrapped(), CallTypes::TypeList, || self.state_generator.print_stack()
        );
        self.expect_state("column_spec_choose_source");
        let (selected_type, ident) = match self.next_state().as_str() {
            "get_column_spec_from_group_by" => {
                self.expect_state("column_spec_choose_qualified");
                // println!("flag_before_valuechooser");
                // println!("context group_by: {:#?}", self.clause_context.group_by());
                // println!("column_types: {:#?}", column_types);
                
                let (selected_type, mut column_name) = self.value_chooser.choose_column_group_by(
                    self.clause_context.group_by(), &column_types
                );
                // println!("flag_after_valuechooser");
                match self.next_state().as_str() {
                    "unqualified_column_name" => {
                        (selected_type, Expr::Identifier(column_name.last().unwrap().clone()))
                    },
                    "qualified_column_name" => {
                        if let &[ref col_ident] = column_name.as_slice() {
                            let rel_ident = self.clause_context.from().get_relation_alias_by_column_name(col_ident);
                            column_name = vec![rel_ident, col_ident.clone()];
                        }
                        (selected_type, Expr::CompoundIdentifier(column_name))
                    },
                    any => self.panic_unexpected(any)
                }
            },
            "get_column_spec_from_from" => {
                self.expect_state("column_spec_choose_qualified");
                match self.next_state().as_str() {
                    "unqualified_column_name" => {
                        let (selected_type, ident_components) = self.value_chooser.choose_column_from(
                            self.clause_context.from(), &column_types, false
                        );
                        (selected_type, Expr::Identifier(ident_components.last().unwrap().clone()))
                    },
                    "qualified_column_name" => {
                        let (selected_type, ident_components) = self.value_chooser.choose_column_from(
                            self.clause_context.from(), &column_types, true
                        );
                        (selected_type, Expr::CompoundIdentifier(ident_components))
                    },
                    any => self.panic_unexpected(any)
                }
            },
            any => self.panic_unexpected(any),
        };
        self.expect_state("EXIT_column_spec");
        (selected_type, ident)
    }

    /// subgraph def_list_expr
    fn handle_list_expr(&mut self) -> (SubgraphType, Vec<Expr>) {
        self.expect_state("list_expr");
        self.expect_state("call16_types");
        let (inner_type, types_value) = self.handle_types(None, None);
        match self.next_state().as_str() {
            "list_expr_multiple_values" => {
                self.state_generator.set_compatible_list(inner_type.get_compat_types());
                let mut list_expr: Vec<Expr> = vec![types_value];
                loop {
                    match self.next_state().as_str() {
                        "call49_types" => {
                            list_expr.push(self.handle_types(None, Some(inner_type.clone())).1);
                        },
                        "EXIT_list_expr" => break,
                        any => self.panic_unexpected(any)
                    }
                }
                (SubgraphType::ListExpr(Box::new(inner_type)), list_expr)
            }
            any => self.panic_unexpected(any)
        }
    }

    /// subgraph def_aggregate_function
    fn handle_aggregate_function(&mut self) -> (SubgraphType, Expr) {
        self.expect_state("aggregate_function");
        let distinct = match self.next_state().as_str() {
            "aggregate_select_return_type" => false,
            "aggregate_distinct" => {
                self.expect_state("aggregate_select_return_type");
                true
            },
            any => self.panic_unexpected(any),  
        };

        let (
            aggr_args_type, aggr_arg_expr_v, aggr_return_type
        ): (AggregateFunctionAgruments, Vec<FunctionArg>, SubgraphType) = match self.next_state().as_str() {
            "aggregate_select_type_integer" => {
                let return_type = SubgraphType::Integer;
                let (args_type, args_expr) = match self.next_state().as_str() {
                    "COUNT" => {
                        self.expect_state("COUNT_wildcard");
                        (AggregateFunctionAgruments::Wildcard, vec![FunctionArg::Unnamed(FunctionArgExpr::Wildcard)])
                    },
                    "arg_integer_any" => {
                        self.expect_state("call65_types");
                        (AggregateFunctionAgruments::AnyType, vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(
                            self.handle_types(None, None).1
                        ))])
                    },
                    "arg_integer" => {
                        self.expect_state("call71_types");
                        (AggregateFunctionAgruments::TypeList(vec![SubgraphType::Integer]), vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(
                            self.handle_types(Some(&[SubgraphType::Integer]), None).1
                        ))])
                    },
                    any => self.panic_unexpected(any),
                };
                (args_type, args_expr, return_type)
            },
            arm @ ("aggregate_select_type_numeric" | "aggregate_select_type_text") => {
                let (return_type, states) = match arm {
                    "aggregate_select_type_text" => (SubgraphType::Text, &["arg_double_text", "call74_types", "arg_single_text", "call63_types"]),
                    "aggregate_select_type_numeric" => (SubgraphType::Numeric, &["arg_double_numeric", "call68_types", "arg_single_numeric", "call66_types"]),
                    any => self.panic_unexpected(any),
                };
                let mut args_type_v = vec![];
                let mut args_expr_v = vec![];
                match self.next_state().as_str() {
                    arm if arm == states[0] => {
                        self.expect_state(states[1]);
                        let arg_expr = self.handle_types(Some(&[return_type.clone()]), None).1;
                        args_type_v.push(return_type.clone());
                        args_expr_v.push(FunctionArg::Unnamed(FunctionArgExpr::Expr(arg_expr)));
                    }
                    arm if arm == states[2] => { },
                    any => self.panic_unexpected(any),
                };
                self.expect_state(states[3]);
                let arg_expr = self.handle_types(Some(&[return_type.clone()]), None).1;
                args_type_v.push(return_type.clone());
                args_expr_v.push(FunctionArg::Unnamed(FunctionArgExpr::Expr(arg_expr)));
                let args_type = AggregateFunctionAgruments::TypeList(args_type_v);
                (args_type, args_expr_v, return_type)
            },
            arm @ ("aggregate_select_type_bool" | "aggregate_select_type_date") => {
                let (return_type, states) = match arm {
                    "aggregate_select_type_bool" => (SubgraphType::Val3, &["arg_single_3vl", "call64_types"]),
                    "aggregate_select_type_date" => (SubgraphType::Date, &["arg_date", "call72_types"]),
                    any => self.panic_unexpected(any),
                };
                let args_type = AggregateFunctionAgruments::TypeList(vec![return_type.clone()]);
                self.expect_states(states);
                let args_expr = vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(
                        self.handle_types(Some(&[return_type.clone()]), None).1,
                ))];
                (args_type, args_expr, return_type)
            },
            any => self.panic_unexpected(any),
        };

        let aggr_name = self.config.aggregate_functions_distribution.get_func_name(
            aggr_args_type, aggr_return_type.clone()
        );

        let expr = Expr::Function(ast::Function {
            name: aggr_name,
            args: aggr_arg_expr_v,
            over: None,
            distinct: distinct,
            special: false,
        });

        self.expect_state("EXIT_aggregate_function");
        (aggr_return_type,  expr)
    }

    /// subgraph def_group_by
    fn handle_group_by(&mut self) -> Vec<Expr> {
        self.expect_state("GROUP_BY");
        match self.next_state().as_str() {
            "group_by_single_group" => {
                self.clause_context.group_by_mut().set_single_group_grouping();
                self.expect_state("EXIT_GROUP_BY");
                return vec![]
            },
            "has_accessible_columns" => {
                self.expect_state("grouping_column_list");
            },
            any => self.panic_unexpected(any),
        }
        let mut result: Vec<Expr> = Vec::new();
        loop {
            let mut return_result = false;
            match self.next_state().as_str() {
                "call70_types" => {
                    let (column_type, column_expr) = self.handle_types(
                        None, None
                    );
                    result.push(column_expr.clone());
                    let chosen_column_ident = match column_expr {
                        Expr::Identifier(ident) => vec![ident],
                        Expr::CompoundIdentifier(vec_of_ident) => vec_of_ident,
                        any => panic!("Unexpected expression for GROUP BY column: {:#?}", any),
                    };
                    self.clause_context.group_by_mut().append_column(chosen_column_ident, column_type);

                    match self.next_state().as_str() {
                        "grouping_column_list" => { },
                        "EXIT_GROUP_BY" => return_result = true,
                        any => self.panic_unexpected(any),
                    }
                },
                "special_grouping" => {
                    let groupping_type_str = match self.next_state().as_str() {
                        arm @ ("grouping_set" | "grouping_rollup" | "grouping_cube") => arm.to_string(),
                        any => self.panic_unexpected(any)
                    };
                    let mut set_list: Vec<Vec<Expr>> = Vec::new();
                    self.expect_state("set_list");
                    loop {
                        let mut current_set = Vec::new();
                        let mut finish_grouping_sets = false;
                        loop {
                            match self.next_state().as_str() {
                                "call69_types" => {
                                    let (column_type, column_expr) = self.handle_types(None, None);
                                    current_set.push(column_expr.clone());
                                    let column_name = match column_expr {
                                        Expr::Identifier(ident) => vec![ident],
                                        Expr::CompoundIdentifier(ident_components) => ident_components,
                                        any => panic!("Unexpected expression for GROUP BY column: {:#?}", any),
                                    };
                                    self.clause_context.group_by_mut().append_column(column_name, column_type);
                                    match self.next_state().as_str() {
                                        "set_multiple" => { },
                                        "set_list" => break,
                                        any => self.panic_unexpected(any),
                                    }
                                },
                                "set_list_empty_allowed" => {
                                    self.expect_state("set_list");
                                    break
                                },
                                "grouping_column_list" => {
                                    finish_grouping_sets = true;
                                    break;
                                },
                                "EXIT_GROUP_BY" => {
                                    finish_grouping_sets = true;
                                    return_result = true;
                                    break;
                                },
                                any => self.panic_unexpected(any),
                            }
                        }
                        set_list.push(current_set);
                        if finish_grouping_sets {
                            result.push(match groupping_type_str.as_str() {
                                "grouping_set" => Expr::GroupingSets(set_list),
                                "grouping_rollup" => Expr::Rollup(set_list),
                                "grouping_cube" => Expr::Cube(set_list),
                                any => self.panic_unexpected(any),
                            });
                            break
                        }
                    }
                },
                any => self.panic_unexpected(any)
            }
            if return_result {
                break result
            }
        }
    }

    /// subgraph def_having
    fn handle_having(&mut self) -> (SubgraphType, Expr) {
        self.expect_state("HAVING");
        self.expect_state("call45_types");
        // println!("\nclause_context.group_by at HAVING generation \n{:#?}\n", self.clause_context.group_by());
        let (selection_type, selection) = self.handle_types(Some(&[SubgraphType::Val3]), None);
        self.expect_state("EXIT_HAVING");
        (selection_type, selection)
    }
    /// starting point; calls handle_query for the first time
    fn generate(&mut self) -> Query {
        let query = self.handle_query().0;
        if self.config.print_queries {
            println!("\n{};\n", query);
        }
        self.free_projection_alias_index = 1;
        // reset the generator
        if let Some(state) = self.next_state_opt() {
            panic!("Couldn't reset state_generator: Received {state}");
        }
        self.dynamic_model = Box::new(DynMod::new());
        query
    }

    pub fn generate_with_dynamic_model_and_value_chooser(&mut self, dynamic_model: Box<DynMod>, value_chooser: Box<QVC>) -> Query {
        self.dynamic_model = dynamic_model;
        self.value_chooser = value_chooser;
        self.generate()
    }
}

impl<DynMod: DynamicModel, StC: StateChooser, QVC: QueryValueChooser> Iterator for QueryGenerator<DynMod, StC, QVC> {
    type Item = Query;

    fn next(&mut self) -> Option<Self::Item> {
        Some(self.generate())
    }
}
