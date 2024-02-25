#[macro_use]
pub mod query_info;
pub mod call_modifiers;
pub mod value_choosers;
pub mod expr_precedence;
pub mod aggregate_function_settings;

use std::path::PathBuf;

use rand::SeedableRng;
use rand_chacha::ChaCha8Rng;
use smol_str::SmolStr;
use sqlparser::ast::{
    self, BinaryOperator, DataType, DateTimeField, Expr, FunctionArg, FunctionArgExpr, Join, ObjectName, OrderByExpr, Query, Select, SelectItem, SetExpr, TableFactor, TableWithJoins, TimezoneInfo, TrimWhereField, UnaryOperator, Value, WildcardAdditionalOptions
};

use crate::{config::TomlReadable, training::models::PathwayGraphModel};

use super::{
    super::{unwrap_variant, unwrap_variant_or_else},
    state_generator::{CallTypes, markov_chain_generator::subgraph_type::SubgraphType}
};
use self::{
    aggregate_function_settings::{AggregateFunctionAgruments, AggregateFunctionDistribution},
    call_modifiers::{SelectAccessibleColumnsValue, ValueSetterValue, WildcardRelationsValue},
    expr_precedence::ExpressionPriority, query_info::{CheckAccessibility, ClauseContext, DatabaseSchema, IdentName, QueryProps}, value_choosers::QueryValueChooser
};

use super::state_generator::{MarkovChainGenerator, substitute_models::SubstituteModel, state_choosers::StateChooser};

#[derive(Debug, Clone)]
pub struct QueryGeneratorConfig {
    pub use_model: bool,
    pub print_queries: bool,
    pub print_schema: bool,
    pub table_schema_path: PathBuf,
    pub substitute_model_name: String,
    pub aggregate_functions_distribution: AggregateFunctionDistribution,
}

impl TomlReadable for QueryGeneratorConfig {
    fn from_toml(toml_config: &toml::Value) -> Self {
        let section = &toml_config["query_generator"];
        Self {
            use_model: section["use_model"].as_bool().unwrap(),
            print_queries: section["print_queries"].as_bool().unwrap(),
            print_schema: section["print_schema"].as_bool().unwrap(),
            table_schema_path: PathBuf::from(section["table_schema_path"].as_str().unwrap()),
            substitute_model_name: section["substitute_model"].as_str().unwrap().to_string(),
            aggregate_functions_distribution: AggregateFunctionDistribution::parse_file(
                PathBuf::from(section["aggregate_functions_distribution_map_file"].as_str().unwrap()),
            ),
        }
    }
}

pub struct QueryGenerator<DynMod: SubstituteModel, StC: StateChooser, QVC: QueryValueChooser> {
    config: QueryGeneratorConfig,
    state_generator: MarkovChainGenerator<StC>,
    substitute_model: Box<DynMod>,
    predictor_model: Option<Box<dyn PathwayGraphModel>>,
    value_chooser: Box<QVC>,
    database_schema: DatabaseSchema,
    clause_context: ClauseContext,
    train_model: Option<Box<dyn PathwayGraphModel>>,
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

#[derive(Debug, Clone)]
pub enum TypeAssertion<'a> {
    None,
    GeneratedBy(SubgraphType),
    CompatibleWith(SubgraphType),
    GeneratedByOneOf(&'a [SubgraphType]),
    CompatibleWithOneOf(&'a [SubgraphType]),
}

impl<'a> TypeAssertion<'a> {
    pub fn check(&'a self, selected_type: &SubgraphType, types_value: &Expr) {
        if !self.assertion_passed(selected_type) {
            panic!("types got an unexpected type: expected {:?}, got {:?}.\nExpr: {:?}", self, selected_type, types_value);
        }
    }

    pub fn check_type(&'a self, selected_type: &SubgraphType) {
        if !self.assertion_passed(selected_type) {
            panic!("types got an unexpected type: expected {:?}, got {:?}", self, selected_type);
        }
    }

    fn assertion_passed(&'a self, selected_type: &SubgraphType) -> bool {
        match self {
            TypeAssertion::None => true,
            TypeAssertion::GeneratedBy(by) => selected_type.is_same_or_more_determined_or_undetermined(by),
            TypeAssertion::CompatibleWith(with) => selected_type.is_compat_with(with),
            TypeAssertion::GeneratedByOneOf(by_one_of) => {
                by_one_of.iter().any(|by| selected_type.is_same_or_more_determined_or_undetermined(by))
            },
            TypeAssertion::CompatibleWithOneOf(with_one_of) => {
                with_one_of.iter().any(|with| selected_type.is_compat_with(with))
            },
        }
    }
}

impl<SubMod: SubstituteModel, StC: StateChooser, QVC: QueryValueChooser> QueryGenerator<SubMod, StC, QVC> {
    pub fn from_state_generator_and_schema(state_generator: MarkovChainGenerator<StC>, config: QueryGeneratorConfig) -> Self {
        let mut _self = QueryGenerator::<SubMod, StC, QVC> {
            state_generator,
            predictor_model: None,
            substitute_model: Box::new(SubMod::new()),
            database_schema: DatabaseSchema::parse_schema(&config.table_schema_path),
            value_chooser: Box::new(QVC::new()),
            config,
            clause_context: ClauseContext::new(),
            train_model: None,
            rng: ChaCha8Rng::seed_from_u64(1),
        };

        if _self.config.print_schema {
            println!("Relations:\n{}", _self.database_schema);
        }

        _self
    }

    fn next_state_opt(&mut self) -> Option<SmolStr> {
        self.state_generator.next_node_name(
            &mut self.rng,
            &mut self.clause_context,
            &mut *self.substitute_model,
            self.predictor_model.as_mut()
        ).unwrap()
    }

    fn next_state(&mut self) -> SmolStr {
        let next_state = self.next_state_opt().unwrap();
        // println!("{next_state}");
        if let Some(ref mut model) = self.train_model {
            model.process_state(
                self.state_generator.call_stack_ref(),
                self.state_generator.get_last_popped_stack_frame_ref(),
            );
        }
        next_state
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

    fn assert_single_type_argument(&self) -> SubgraphType {
        let arg_types = unwrap_variant!(self.state_generator.get_fn_selected_types_unwrapped(), CallTypes::TypeList);
        if let [tp] = arg_types.as_slice() {
            tp.clone()
        } else {
            panic!("This subgraph does not accept multiple types as argument. Got: {:?}", arg_types);
        }
    }

    /// subgraph def_Query
    fn handle_query(&mut self) -> (Query, Vec<(Option<IdentName>, SubgraphType)>) {
        self.substitute_model.notify_subquery_creation_begin();
        self.clause_context.on_query_begin(self.state_generator.get_fn_modifiers_opt());
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

        select_body.projection = self.handle_select();
        select_body.distinct = self.clause_context.query().is_distinct();

        if self.clause_context.group_by().is_grouping_active() && !self.clause_context.query().is_aggregation_indicated() {
            if select_body.group_by.is_empty() {
                // Grouping is active but wasn't indicated in any way. Add GROUP BY true
                // instead of GROUP BY (), because unsupported by parser
                select_body.group_by = vec![Expr::Value(Value::Boolean(true))]
            }
        }

        self.expect_state("call0_ORDER_BY");
        let order_by = self.handle_order_by();

        self.expect_state("call0_LIMIT");
        let select_limit = self.handle_limit().1;

        self.expect_state("EXIT_Query");
        let output_type = self.clause_context.query_mut().pop_output_type();
        // if output_type.iter().filter_map(|(o, _)| o.as_ref()).any(|o| o.value == "case") {
        //     eprintln!("output: {:?}", output_type);
        // }
        self.substitute_model.notify_subquery_creation_end();
        self.clause_context.on_query_end();

        (Query {
            with: None,
            body: Box::new(SetExpr::Select(Box::new(select_body))),
            order_by,
            limit: select_limit,
            offset: None,
            fetch: None,
            locks: vec![],
        }, output_type)
    }

    /// subgraph def_ORDER_BY
    fn handle_order_by(&mut self) -> Vec<OrderByExpr> {
        self.expect_state("ORDER_BY");
        let mut order_by = vec![];
        match self.next_state().as_str() {
            "EXIT_ORDER_BY" => return order_by,
            "order_by_list" => { },
            any => self.panic_unexpected(any),
        };
        loop {
            let expr = match self.next_state().as_str() {
                "order_by_select_reference" => {
                    self.expect_state("order_by_select_reference_by_alias");
                    let aliases = &unwrap_variant!(self.state_generator
                        .get_named_value::<SelectAccessibleColumnsValue>().unwrap(),
                        ValueSetterValue::SelectAccessibleColumns
                    ).accessible_columns.iter().collect::<Vec<_>>();
                    let alias = self.value_chooser.choose_select_alias_order_by(aliases);
                    Expr::Identifier(alias)
                },
                "order_by_expr" => {
                    match self.next_state().as_str() {
                        "call84_types" => { },
                        "call85_types" => { },
                        any => self.panic_unexpected(any)
                    };
                    self.handle_types(TypeAssertion::None).1
                },
                any => self.panic_unexpected(any),
            };
            self.expect_state("order_by_expr_done");
            let mut order_by_expr = OrderByExpr {
                expr: expr,
                asc: None,
                nulls_first: None,
            };
            match self.next_state().as_str() {
                "order_by_order_selected" => { },
                "order_by_asc" => {
                    order_by_expr.asc = Some(true);
                    self.expect_state("order_by_order_selected");
                },
                "order_by_desc" => {
                    order_by_expr.asc = Some(false);
                    self.expect_state("order_by_order_selected");
                },
                any => self.panic_unexpected(any),
            }
            match self.next_state().as_str() {
                "order_by_nulls_first_selected" => { },
                "order_by_nulls_first" => {
                    order_by_expr.nulls_first = Some(true);
                    self.expect_state("order_by_nulls_first_selected");
                },
                "order_by_nulls_last" => {
                    order_by_expr.nulls_first = Some(false);
                    self.expect_state("order_by_nulls_first_selected");
                },
                any => self.panic_unexpected(any),
            }
            order_by.push(order_by_expr);
            match self.next_state().as_str() {
                "order_by_list" => {},
                "EXIT_ORDER_BY" => break,
                any => self.panic_unexpected(any),
            }
        }
        order_by
    }

    /// subgraph def_FROM
    fn handle_from(&mut self) -> Vec<TableWithJoins> {
        self.expect_state("FROM");

        let mut from: Vec<TableWithJoins> = vec![];

        loop {
            self.clause_context.from_mut().add_subfrom();
            from.push(TableWithJoins { relation: match self.next_state().as_str() {
                "FROM_table" => self.handle_from_table(),
                "call0_Query" => self.handle_from_query(),
                "EXIT_FROM" => {
                    self.clause_context.from_mut().delete_subfrom();
                    break
                },
                any => self.panic_unexpected(any)
            }, joins: vec![] });

            match self.next_state().as_str() {
                "FROM_cartesian_product" => { },
                "FROM_join_by" => {
                    let joins = &mut from.last_mut().unwrap().joins;
                    loop {
                        let join_type = match self.next_state().as_str() {
                            s @ ( "FROM_join_join" | "FROM_left_join" | "FROM_right_join" | "FROM_full_join" ) => s.to_string(),
                            any => self.panic_unexpected(any)
                        };
                        self.expect_state("FROM_join_to");
                        let relation = match self.next_state().as_str() {
                            "FROM_join_table" => self.handle_from_table(),
                            "call5_Query" => self.handle_from_query(),
                            any => self.panic_unexpected(any)
                        };
                        // only activate sub-from for JOIN ON
                        self.clause_context.from_mut().activate_subfrom();
                        self.expect_states(&["FROM_join_on", "call83_types"]);
                        let join_on = ast::JoinConstraint::On(
                            self.handle_types(TypeAssertion::GeneratedBy(SubgraphType::Val3)).1
                        );
                        self.clause_context.from_mut().deactivate_subfrom();
                        joins.push(Join {
                            relation,
                            join_operator: match join_type.as_str() {
                                "FROM_join_join" => ast::JoinOperator::Inner(join_on),
                                "FROM_left_join" => ast::JoinOperator::LeftOuter(join_on),
                                "FROM_right_join" => ast::JoinOperator::RightOuter(join_on),
                                "FROM_full_join" => ast::JoinOperator::FullOuter(join_on),
                                any => self.panic_unexpected(any)
                            },
                        });
                        match self.next_state().as_str() {
                            "FROM_join_by" => { },
                            "FROM_cartesian_product" => break,
                            any => self.panic_unexpected(any)
                        }
                    }
                },
                any => self.panic_unexpected(any)
            }

            self.clause_context.from_mut().delete_subfrom();
        }
        self.clause_context.from_mut().activate_from();
        from
    }

    fn handle_from_table(&mut self) -> TableFactor {
        let create_table_st = self.value_chooser.choose_table(&self.database_schema);
        let alias = self.clause_context.from_mut().append_table(create_table_st);
        TableFactor::Table {
            name: create_table_st.name.clone(),
            alias: Some(alias),
            args: None,
            with_hints: vec![],
            columns_definition: None,
        }
    }

    fn handle_from_query(&mut self) -> TableFactor {
        let (query, column_idents_and_graph_types) = self.handle_query();
        let alias = self.clause_context.from_mut().append_query(column_idents_and_graph_types);
        TableFactor::Derived {
            lateral: false,
            subquery: Box::new(query),
            alias: Some(alias)
        }
    }

    /// subgraph def_WHERE
    fn handle_where(&mut self) -> (SubgraphType, Expr) {
        self.expect_state("WHERE");
        self.expect_state("call53_types");
        let (selection_type, selection) = self.handle_types(TypeAssertion::GeneratedBy(SubgraphType::Val3));
        self.expect_state("EXIT_WHERE");
        (selection_type, selection)
    }

    /// subgraph def_SELECT
    fn handle_select(&mut self) -> Vec<SelectItem> {
        self.expect_state("SELECT");
        match self.next_state().as_str() {
            "SELECT_DISTINCT" => {
                self.clause_context.query_mut().set_distinct();
                self.expect_state("SELECT_list");
            },
            "SELECT_list" => { },
            any => self.panic_unexpected(any)
        }

        let mut column_idents_and_graph_types: Vec<(Option<IdentName>, SubgraphType)> = vec![];
        let mut projection = vec![];

        loop {
            match self.next_state().as_str() {
                "SELECT_tables_eligible_for_wildcard" => {
                    match self.next_state().as_str() {
                        "SELECT_wildcard" => {
                            column_idents_and_graph_types.extend(
                                self.clause_context.from().get_wildcard_columns_iter()
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
                            column_idents_and_graph_types.extend(relation.get_all_columns_iter_cloned());
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
                    let (subgraph_type, expr) = self.handle_types(TypeAssertion::None);
                    self.expect_state("select_expr_done");
                    let (alias, select_item) = match alias_node {
                        "SELECT_unnamed_expr" => {
                            let alias = QueryProps::extract_alias(&expr);
                            (alias, SelectItem::UnnamedExpr(expr))
                        },
                        "SELECT_expr_with_alias" => {
                            let select_alias = self.value_chooser.choose_select_alias();
                            (Some(select_alias.clone().into()), SelectItem::ExprWithAlias {
                                expr, alias: select_alias,
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

        self.clause_context.query_mut().set_select_type(column_idents_and_graph_types);

        projection
    }

    /// subgraph def_group_by
    fn handle_group_by(&mut self) -> Vec<Expr> {
        self.expect_state("GROUP_BY");
        match self.next_state().as_str() {
            "group_by_single_group" => {
                self.clause_context.group_by_mut().set_single_group_grouping();
                self.clause_context.group_by_mut().set_single_row_grouping();
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
                    let (column_type, column_expr) = self.handle_types(TypeAssertion::None);
                    let column_name = self.clause_context.retrieve_column_by_column_expr(
                        &column_expr, false
                    ).unwrap().1;
                    result.push(column_expr);
                    self.clause_context.group_by_mut().append_column(column_name, column_type);

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
                                    let (column_type, column_expr) = self.handle_types(TypeAssertion::None);
                                    let column_name = self.clause_context.retrieve_column_by_column_expr(
                                        &column_expr, false
                                    ).unwrap().1;
                                    current_set.push(column_expr);
                                    self.clause_context.group_by_mut().append_column(column_name, column_type);
                                },
                                "set_multiple" => { },
                                "set_list_empty_allowed" => { },  // will break in next iteration
                                "set_list" => break,
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
                break
            }
        }

        // For cases such as: GROUPING SETS ( (), (), () )
        if !self.clause_context.group_by().contains_columns() {
            self.clause_context.group_by_mut().set_single_group_grouping();
            // Check is GROUPING SETS ( () )
            if let &[Expr::GroupingSets(ref set_list)] = result.as_slice() {
                if set_list.len() == 1 {
                    self.clause_context.group_by_mut().set_single_row_grouping();
                }
            }
        }

        self.clause_context.query_mut().set_aggregation_indicated();

        result
    }

    /// subgraph def_aggregate_function
    fn handle_aggregate_function(&mut self) -> (SubgraphType, Expr) {
        self.expect_state("aggregate_function");
        // if any aggregate funciton is present in query, aggregation was succesfully indicated.
        self.clause_context.query_mut().set_aggregation_indicated();
        let distinct = match self.next_state().as_str() {
            "aggregate_not_distinct" => false,
            "aggregate_distinct" => true,
            any => self.panic_unexpected(any),
        };
        self.expect_state("aggregate_select_return_type");

        let (
            aggr_args_type, aggr_arg_expr_v, aggr_return_type
        ): (AggregateFunctionAgruments, Vec<FunctionArg>, SubgraphType) = match self.next_state().as_str() {
            "aggregate_select_type_bigint" => {
                let return_type = SubgraphType::BigInt;
                let (args_type, args_expr) = match self.next_state().as_str() {
                    "arg_star" => {
                        (AggregateFunctionAgruments::Wildcard, vec![FunctionArg::Unnamed(FunctionArgExpr::Wildcard)])
                    },
                    "arg_bigint_any" => {
                        self.expect_state("call65_types");
                        (AggregateFunctionAgruments::AnyType, vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(
                            self.handle_types(TypeAssertion::None).1
                        ))])
                    },
                    "arg_bigint" => {
                        self.expect_state("call75_types");
                        self.state_generator.set_compatible_list(SubgraphType::BigInt.get_compat_types());
                        (AggregateFunctionAgruments::TypeList(vec![SubgraphType::BigInt]), vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(
                            self.handle_types(TypeAssertion::CompatibleWith(SubgraphType::BigInt)).1
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
                self.state_generator.set_compatible_list(return_type.get_compat_types());
                match self.next_state().as_str() {
                    arm if arm == states[0] => {
                        self.expect_state(states[1]);
                        let arg_expr = self.handle_types(TypeAssertion::CompatibleWith(return_type.clone())).1;
                        args_type_v.push(return_type.clone());
                        args_expr_v.push(FunctionArg::Unnamed(FunctionArgExpr::Expr(arg_expr)));
                    },
                    arm if arm == states[2] => { },
                    any => self.panic_unexpected(any),
                };
                self.expect_state(states[3]);
                let arg_expr = self.handle_types(TypeAssertion::CompatibleWith(return_type.clone())).1;
                args_type_v.push(return_type.clone());
                args_expr_v.push(FunctionArg::Unnamed(FunctionArgExpr::Expr(arg_expr)));
                let args_type = AggregateFunctionAgruments::TypeList(args_type_v);
                (args_type, args_expr_v, return_type)
            },
            arm @ ("aggregate_select_type_bool" | "aggregate_select_type_date" | "aggregate_select_type_timestamp" | "aggregate_select_type_interval" | "aggregate_select_type_integer") => {
                let (return_type, states) = match arm {
                    "aggregate_select_type_bool" => (SubgraphType::Val3, &["arg_single_3vl", "call64_types"]),
                    "aggregate_select_type_date" => (SubgraphType::Date, &["arg_date", "call72_types"]),
                    "aggregate_select_type_timestamp" => (SubgraphType::Timestamp, &["arg_timestamp", "call96_types"]),
                    "aggregate_select_type_interval" => (SubgraphType::Interval, &["arg_interval", "call90_types"]),
                    "aggregate_select_type_integer" => (SubgraphType::Integer, &["arg_integer", "call71_types"]),
                    any => self.panic_unexpected(any),
                };
                let args_type = AggregateFunctionAgruments::TypeList(vec![return_type.clone()]);
                self.expect_states(states);
                self.state_generator.set_compatible_list(return_type.get_compat_types());
                let args_expr = vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(
                    self.handle_types(TypeAssertion::CompatibleWith(return_type.clone())).1,
                ))];
                (args_type, args_expr, return_type)
            },
            any => self.panic_unexpected(any),
        };

        let (func_names_iter, dist) = self.config.aggregate_functions_distribution.get_functions_and_dist(&aggr_args_type, &aggr_return_type);
        let aggr_name = self.value_chooser.choose_aggregate_function_name(func_names_iter, dist);

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

    /// subgraph def_having
    fn handle_having(&mut self) -> (SubgraphType, Expr) {
        self.expect_state("HAVING");
        self.expect_state("call45_types");
        let (selection_type, selection) = self.handle_types(TypeAssertion::GeneratedBy(SubgraphType::Val3));
        self.clause_context.query_mut().set_aggregation_indicated();
        self.expect_state("EXIT_HAVING");
        (selection_type, selection)
    }

    /// subgraph def_LIMIT
    fn handle_limit(&mut self) -> (SubgraphType, Option<Expr>) {
        self.expect_state("LIMIT");

        let (limit_type, limit) = match self.next_state().as_str() {
            "query_can_skip_limit_set_val" => {
                self.expect_state("query_can_skip_limit");
                (SubgraphType::Undetermined, None)
            }
            "single_row_true" => {
                (SubgraphType::Integer, Some(Expr::Value(Value::Number("1".to_string(), false))))
            },
            "limit_num" => {
                self.expect_state("call52_types");
                let (limit_type, limit_expr) = self.handle_types(
                    TypeAssertion::GeneratedByOneOf(&[SubgraphType::Numeric, SubgraphType::Integer, SubgraphType::BigInt]),
                );
                (limit_type, Some(limit_expr))
            },
            any => self.panic_unexpected(any)
        };
        self.expect_state("EXIT_LIMIT");

        (limit_type, limit)
    }

    /// subgraph def_types
    fn handle_types(&mut self, type_assertion: TypeAssertion) -> (SubgraphType, Expr) {
        self.expect_state("types");

        let selected_types = unwrap_variant!(self.state_generator.get_fn_selected_types_unwrapped(), CallTypes::TypeList);
        let selected_type = match self.next_state().as_str() {
            "types_select_type_bigint" => SubgraphType::BigInt,
            "types_select_type_integer" => SubgraphType::Integer,
            "types_select_type_numeric" => SubgraphType::Numeric,
            "types_select_type_3vl" => SubgraphType::Val3,
            "types_select_type_text" => SubgraphType::Text,
            "types_select_type_date" => SubgraphType::Date,
            "types_select_type_interval" => SubgraphType::Interval,
            "types_select_type_timestamp" => SubgraphType::Timestamp,
            any => self.panic_unexpected(any),
        };
        let allowed_type_list = SubgraphType::filter_by_selected(&selected_types, selected_type);

        self.state_generator.set_known_list(allowed_type_list);
        self.expect_state("call0_types_value");
        let (selected_type, types_value) = self.handle_types_value(type_assertion);

        self.expect_state("EXIT_types");
        (selected_type, types_value)
    }

    /// subgraph def_types_type
    fn handle_types_type(&mut self) -> SubgraphType {
        self.expect_state("types_type");

        let tp = match self.next_state().as_str() {
            "types_type_bigint" => SubgraphType::BigInt,
            "types_type_integer" => SubgraphType::Integer,
            "types_type_numeric" => SubgraphType::Numeric,
            "types_type_3vl" => SubgraphType::Val3,
            "types_type_text" => SubgraphType::Text,
            "types_type_date" => SubgraphType::Date,
            "types_type_interval" => SubgraphType::Interval,
            "types_type_timestamp" => SubgraphType::Timestamp,
            any => self.panic_unexpected(any),
        };

        self.expect_state("EXIT_types_type");

        tp
    }

    /// subgraph def_types_value
    fn handle_types_value(&mut self, type_assertion: TypeAssertion) -> (SubgraphType, Expr) {
        self.expect_state("types_value");
        let selected_types = unwrap_variant!(self.state_generator.get_fn_selected_types_unwrapped(), CallTypes::TypeList);
        self.state_generator.set_known_list(selected_types.clone());
        let (selected_type, types_value) = match self.next_state().as_str() {
            "types_value_nested" => {
                self.expect_state("call1_types_value");
                let (tp, expr) = self.handle_types_value(TypeAssertion::None);
                (tp, Expr::Nested(Box::new(expr)))
            },
            "types_value_null" => {
                (SubgraphType::Undetermined, Expr::Value(Value::Null))
            },
            "types_value_typed_null" => {
                let null_type = {
                    let types_without_inner = selected_types.into_iter()
                        .filter(|x| !x.has_inner()).collect::<Vec<_>>();
                    match types_without_inner.as_slice() {
                        [tp] => tp.clone(),
                        any => panic!("allowed_type_list must have single element here (got {:?})", any)
                    }
                };
                (null_type.clone(), Expr::Cast {
                    expr: Box::new(Expr::Value(Value::Null)),
                    data_type: null_type.to_data_type(),
                })
            },
            "call0_case" => self.handle_case(),
            "call0_formulas" => self.handle_formulas(),
            "call0_literals" => self.handle_literals(),
            "call0_aggregate_function" => self.handle_aggregate_function(),
            "column_type_available" => {
                self.expect_state("call0_column_spec");
                self.handle_column_spec()
            },
            "call1_Query" => {
                let (subquery, column_types) = self.handle_query();
                let selected_type = match column_types.len() {
                    1 => column_types[0].1.clone(),
                    any => panic!("Subquery should have selected a single column, but selected {any}. Subquery: {subquery}"),
                };
                (selected_type, Expr::Subquery(Box::new(subquery)))
            },
            any => self.panic_unexpected(any),
        };
        type_assertion.check(&selected_type, &types_value);
        self.expect_state("EXIT_types_value");
        (selected_type, types_value.nest_children_if_needed())
    }

    /// subgraph def_formulas
    fn handle_formulas(&mut self) -> (SubgraphType, Expr) {
        self.expect_state("formulas");
        self.assert_single_type_argument();
        let (selected_type, types_value) = match self.next_state().as_str() {
            "call2_number" |
            "call1_number" |
            "call0_number" => self.handle_number(),
            "call1_VAL_3" => self.handle_val_3(),
            "call0_text" => self.handle_text(),
            "call0_date" => self.handle_date(),
            "call0_timestamp" => self.handle_timestamp(),
            "call0_interval" => self.handle_interval(),
            any => self.panic_unexpected(any)
        };
        self.expect_state("EXIT_formulas");
        (selected_type, types_value)
    }

    /// subgraph def_literals
    fn handle_literals(&mut self) -> (SubgraphType, Expr) {
        self.expect_state("literals");

        let (tp, mut expr) = match self.next_state().as_str() {
            "bool_literal" => {
                (SubgraphType::Val3, match self.next_state().as_str() {
                    "true" => Expr::Value(Value::Boolean(true)),
                    "false" => Expr::Value(Value::Boolean(false)),
                    any => self.panic_unexpected(any),
                })
            },
            st @ (
                "number_literal_bigint" | "number_literal_integer" | "number_literal_numeric"
            ) => {
                let (number_type, number_str) = match st {
                    "number_literal_bigint" => {
                        // NOTE:
                        // - if bigint is too small, it will be interp. as an int by postgres.
                        // however, int can be cast to bigint (if required), so no problem for now.
                        // - in AST->path, the extracted 'int' path will produce the same query,
                        // even though the intended type could be bigint, if it is too small.
                        (SubgraphType::BigInt, self.value_chooser.choose_bigint())
                    },
                    "number_literal_integer" => {
                        (SubgraphType::Integer, self.value_chooser.choose_integer())
                    },
                    "number_literal_numeric" => {
                        (SubgraphType::Numeric, (self.value_chooser.choose_numeric()))
                    },
                    _ => unreachable!(),
                };
                (number_type, Expr::Value(Value::Number(number_str, false)))
            },
            "text_literal" => (SubgraphType::Text, Expr::Value(Value::SingleQuotedString(self.value_chooser.choose_string()))),  // TODO: hardcoded
            "date_literal" => (SubgraphType::Date, Expr::TypedString {
                data_type: DataType::Date, value: self.value_chooser.choose_date(),
            }),
            "timestamp_literal" => (SubgraphType::Timestamp, Expr::TypedString {
                data_type: DataType::Timestamp(None, TimezoneInfo::None), value: self.value_chooser.choose_timestamp(),
            }),
            "interval_literal" => (SubgraphType::Interval, {
                let with_field = match self.next_state().as_str() {
                    "interval_literal_format_string" => false,
                    "interval_literal_with_field" => true,
                    any => self.panic_unexpected(any),
                };
                let (str_value, leading_field) = self.value_chooser.choose_interval(with_field);
                Expr::Interval {
                    value: Box::new(Expr::Value(Value::SingleQuotedString(str_value.to_string()))),
                    leading_field,
                    leading_precision: None,
                    last_field: None,
                    fractional_seconds_precision: None
                }
            }),
            any => self.panic_unexpected(any),
        };

        let next_st = match self.next_state().as_str() {
            "literals_explicit_cast" => {
                expr = Expr::Cast { expr: Box::new(expr), data_type: tp.to_data_type() };
                self.next_state()
            },
            any => SmolStr::new(any),
        };

        match next_st.as_str() {
            "number_literal_minus" => {
                expr = Expr::UnaryOp { op: UnaryOperator::Minus, expr: Box::new(expr) };
                self.expect_state("EXIT_literals");
            },
            "EXIT_literals" => { },
            any => self.panic_unexpected(any),
        }

        (tp, expr)
    }

    /// subgraph def_column_spec
    fn handle_column_spec(&mut self) -> (SubgraphType, Expr) {
        self.expect_state("column_spec");
        let column_types = unwrap_variant_or_else!(
            self.state_generator.get_fn_selected_types_unwrapped(), CallTypes::TypeList, || self.state_generator.print_stack()
        );
        self.expect_state("column_spec_choose_source");
        let only_group_by_columns = match self.next_state().as_str() {
            "get_column_spec_from_group_by" => true,
            "get_column_spec_from_from" => false,
            any => self.panic_unexpected(any),
        };
        self.expect_state("column_spec_choose_qualified");
        let check_accessibility = match self.next_state().as_str() {
            "qualified_column_name" => CheckAccessibility::QualifiedColumnName,
            "unqualified_column_name" => CheckAccessibility::ColumnName,
            any => self.panic_unexpected(any)
        };
        let (selected_type, qualified_column_name) = self.value_chooser.choose_column(
            &self.clause_context, column_types, only_group_by_columns, check_accessibility.clone()
        );
        let ident = if check_accessibility == CheckAccessibility::QualifiedColumnName {
            Expr::CompoundIdentifier(qualified_column_name.into_iter().map(IdentName::into).collect())
        } else {
            Expr::Identifier(qualified_column_name.last().unwrap().clone().into())
        };
        self.expect_state("EXIT_column_spec");
        (selected_type, ident)
    }

    /// subgraph def_list_expr
    fn handle_list_expr(&mut self) -> (SubgraphType, Vec<Expr>) {
        self.expect_state("list_expr");
        self.expect_state("call6_types_type");
        let inner_type = self.handle_types_type();
        self.state_generator.set_compatible_list(inner_type.get_compat_types());
        self.expect_state("call16_types");
        let types_value = self.handle_types(TypeAssertion::CompatibleWith(inner_type.clone())).1;
        match self.next_state().as_str() {
            "list_expr_multiple_values" => {
                let mut list_expr: Vec<Expr> = vec![types_value];
                loop {
                    match self.next_state().as_str() {
                        "call49_types" => {
                            list_expr.push(self.handle_types(TypeAssertion::CompatibleWith(inner_type.clone())).1);
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

    /// subgraph def_case
    fn handle_case(&mut self) -> (SubgraphType, Expr) {
        self.expect_state("case");

        self.expect_states(&["case_first_result", "call7_types_type"]);
        let out_type = self.handle_types_type();
        self.expect_state("call82_types");
        self.state_generator.set_compatible_list(out_type.get_compat_types());
        let first_result = self.handle_types(TypeAssertion::CompatibleWith(out_type.clone())).1;
        let mut results = vec![first_result];
        let mut conditions = vec![];

        let operand = match self.next_state().as_str() {
            "simple_case" => {
                self.expect_states(&["simple_case_operand", "call8_types_type"]);
                let operand_type = self.handle_types_type();
                self.expect_state("call78_types");
                self.state_generator.set_compatible_list(operand_type.get_compat_types());
                let operand_expr = self.handle_types(TypeAssertion::CompatibleWith(operand_type.clone())).1;

                loop {
                    self.expect_states(&["simple_case_condition", "call79_types"]);
                    self.state_generator.set_compatible_list(operand_type.get_compat_types());
                    let condition_expr = self.handle_types(TypeAssertion::CompatibleWith(operand_type.clone())).1;
                    conditions.push(condition_expr);

                    match self.next_state().as_str() {
                        "simple_case_result" => {
                            self.expect_state("call80_types");
                            self.state_generator.set_compatible_list(out_type.get_compat_types());
                            let result_expr = self.handle_types(TypeAssertion::CompatibleWith(out_type.clone())).1;
                            results.push(result_expr);
                        },
                        "case_else" => break,
                        any => self.panic_unexpected(any),
                    }
                }

                Some(Box::new(operand_expr))
            },
            "searched_case" => {
                loop {
                    self.expect_state("searched_case_condition");
                    self.expect_state("call76_types");
                    let condition_expr = self.handle_types(TypeAssertion::GeneratedBy(SubgraphType::Val3)).1;
                    conditions.push(condition_expr);

                    match self.next_state().as_str() {
                        "searched_case_result" => {
                            self.expect_state("call77_types");
                            self.state_generator.set_compatible_list(out_type.get_compat_types());
                            let result_expr = self.handle_types(TypeAssertion::CompatibleWith(out_type.clone())).1;
                            results.push(result_expr);
                        },
                        "case_else" => break,
                        any => self.panic_unexpected(any),
                    }
                }
                None
            },
            any => self.panic_unexpected(any),
        };

        let else_result = match self.next_state().as_str() {
            "call81_types" => {
                self.state_generator.set_compatible_list(out_type.get_compat_types());
                let else_expr = self.handle_types(TypeAssertion::CompatibleWith(out_type.clone())).1;
                self.expect_state("EXIT_case");
                Some(Box::new(else_expr))
            },
            "EXIT_case" => None,
            any => self.panic_unexpected(any),
        };

        (out_type, Expr::Case {
            operand, conditions, results, else_result
        })
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
                let types_value = Box::new(self.handle_types(TypeAssertion::None).1);
                if is_null_not_flag {
                    Expr::IsNotNull(types_value)
                } else {
                    Expr::IsNull(types_value)
                }
            },
            "IsDistinctFrom" => {
                self.expect_state("call0_types_type");
                let tp = self.handle_types_type();
                self.expect_state("call56_types");
                self.state_generator.set_compatible_list(tp.get_compat_types());
                let types_value_1 = self.handle_types(TypeAssertion::CompatibleWith(tp.clone())).1;
                let is_distinct_not_flag = match self.next_state().as_str() {
                    "IsDistinctNOT" => {
                        self.expect_state("DISTINCT");
                        true
                    }
                    "DISTINCT" => false,
                    any => self.panic_unexpected(any)
                };
                self.expect_state("call21_types");
                let types_value_2 = self.handle_types(TypeAssertion::CompatibleWith(tp)).1;
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
                self.expect_state("call3_types_type");
                let tp = self.handle_types_type();
                self.state_generator.set_compatible_list(tp.get_compat_types());
                self.expect_state("call57_types");
                let types_value = self.handle_types(TypeAssertion::CompatibleWith(tp)).1;
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
                self.expect_state("call4_types_type");
                let tp = self.handle_types_type();
                self.state_generator.set_compatible_list(tp.get_compat_types());
                self.expect_state("call58_types");
                let types_value = self.handle_types(TypeAssertion::CompatibleWith(tp)).1;
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
                self.expect_state("call5_types_type");
                let tp = self.handle_types_type();
                self.state_generator.set_compatible_list(tp.get_compat_types());
                self.expect_state("call59_types");
                let types_value_1 = self.handle_types(TypeAssertion::CompatibleWith(tp.clone())).1;
                let between_not_flag = match self.next_state().as_str() {
                    "BetweenBetweenNot" => {
                        self.expect_state("BetweenBetween");
                        true
                    },
                    "BetweenBetween" => false,
                    any => self.panic_unexpected(any)
                };
                self.expect_state("call22_types");
                let types_value_2 = self.handle_types(TypeAssertion::CompatibleWith(tp.clone())).1;
                self.expect_state("BetweenBetweenAnd");
                self.expect_state("call23_types");
                let types_value_3 = self.handle_types(TypeAssertion::CompatibleWith(tp)).1;
                Expr::Between {
                    expr: Box::new(types_value_1),
                    negated: between_not_flag,
                    low: Box::new(types_value_2),
                    high: Box::new(types_value_3)
                }
            },
            "BinaryComp" => {
                self.expect_state("call1_types_type");
                let tp = self.handle_types_type();
                self.state_generator.set_compatible_list(tp.get_compat_types());
                self.expect_state("call60_types");
                let types_value_1 = self.handle_types(TypeAssertion::CompatibleWith(tp.clone())).1;
                let binary_comp_op = match self.next_state().as_str() {
                    "BinaryCompEqual" => BinaryOperator::Eq,
                    "BinaryCompLess" => BinaryOperator::Lt,
                    "BinaryCompLessEqual" => BinaryOperator::LtEq,
                    "BinaryCompUnEqual" => BinaryOperator::NotEq,
                    any => self.panic_unexpected(any)
                };
                self.expect_state("call24_types");
                let types_value_2 = self.handle_types(TypeAssertion::CompatibleWith(tp)).1;
                Expr::BinaryOp {
                    left: Box::new(types_value_1),
                    op: binary_comp_op,
                    right: Box::new(types_value_2)
                }
            },
            "AnyAll" => {
                self.expect_state("call2_types_type");
                let tp = self.handle_types_type();
                self.state_generator.set_compatible_list(tp.get_compat_types());
                self.expect_state("call61_types");
                let types_value = self.handle_types(TypeAssertion::CompatibleWith(tp)).1;
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
                let types_value_1 = self.handle_types(TypeAssertion::GeneratedBy(SubgraphType::Text)).1;
                let string_like_not_flag = match self.next_state().as_str() {
                    "BinaryStringLikeNot" => {
                        self.expect_state("BinaryStringLikeIn");
                        true
                    }
                    "BinaryStringLikeIn" => false,
                    any => self.panic_unexpected(any)
                };
                self.expect_state("call26_types");
                let types_value_2 = self.handle_types(TypeAssertion::GeneratedBy(SubgraphType::Text)).1;
                Expr::Like {
                    negated: string_like_not_flag,
                    expr: Box::new(types_value_1),
                    pattern: Box::new(types_value_2),
                    escape_char: None
                }
            },
            "BinaryBooleanOpV3" => {
                self.expect_state("call27_types");
                let types_value_1 = self.handle_types(TypeAssertion::GeneratedBy(SubgraphType::Val3)).1;
                let binary_bool_op = match self.next_state().as_str() {
                    "BinaryBooleanOpV3AND" => BinaryOperator::And,
                    "BinaryBooleanOpV3OR" => BinaryOperator::Or,
                    "BinaryBooleanOpV3XOR" => BinaryOperator::Xor,
                    any => self.panic_unexpected(any)
                };
                self.expect_state("call28_types");
                let types_value_2 = self.handle_types(TypeAssertion::GeneratedBy(SubgraphType::Val3)).1;
                Expr::BinaryOp {
                    left: Box::new(types_value_1),
                    op: binary_bool_op,
                    right: Box::new(types_value_2)
                }
            },
            "UnaryNot_VAL_3" => {
                self.expect_state("call30_types");
                Expr::UnaryOp {
                    op: UnaryOperator::Not,
                    expr: Box::new(self.handle_types(TypeAssertion::GeneratedBy(SubgraphType::Val3)).1)
                }
            },
            any => self.panic_unexpected(any)
        };
        self.expect_state("EXIT_VAL_3");
        (SubgraphType::Val3, val3)
    }

    /// subgraph def_numeric
    fn handle_number(&mut self) -> (SubgraphType, Expr) {
        self.expect_state("number");
        let number_type = self.assert_single_type_argument();
        let (number_type, number) = match self.next_state().as_str() {
            "BinaryNumberOp" => {
                self.expect_state("call48_types");
                self.state_generator.set_compatible_list(number_type.get_compat_types());
                let types_value_1 = self.handle_types(TypeAssertion::CompatibleWith(number_type.clone())).1;
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
                let types_value_2 = self.handle_types(TypeAssertion::CompatibleWith(number_type.clone())).1;
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
                if numeric_unary_op == UnaryOperator::Minus {
                    self.expect_state("call89_types");
                } else {
                    self.expect_state("call1_types");
                }
                self.state_generator.set_compatible_list(number_type.get_compat_types());
                let number = self.handle_types(TypeAssertion::CompatibleWith(number_type.clone())).1;
                (number_type, Expr::UnaryOp {
                    op: numeric_unary_op,
                    expr: Box::new(number)
                })
            },
            "number_string_position" => {
                self.expect_state("call2_types");
                let types_value_1 = self.handle_types(TypeAssertion::GeneratedBy(SubgraphType::Text)).1;
                self.expect_state("string_position_in");
                self.expect_state("call3_types");
                let types_value_2 = self.handle_types(TypeAssertion::GeneratedBy(SubgraphType::Text)).1;
                (SubgraphType::Integer, Expr::Position {
                    expr: Box::new(types_value_1),
                    r#in: Box::new(types_value_2)
                })
            },
            "number_extract_field_from_date" => {
                self.expect_state("call0_select_datetime_field");
                let field = self.handle_select_datetime_field();
                self.expect_state("call97_types");
                self.state_generator.set_compatible_list([
                    SubgraphType::Interval.get_compat_types(),
                    SubgraphType::Timestamp.get_compat_types(),
                ].concat());
                let date = self.handle_types(TypeAssertion::CompatibleWithOneOf(&[SubgraphType::Interval, SubgraphType::Timestamp])).1;
                (SubgraphType::Numeric, Expr::Extract { field, expr: Box::new(date) })
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
            "text_trim" => {
                let (trim_where, trim_what) = match self.next_state().as_str() {
                    "call6_types" => {
                        let types_value = self.handle_types(TypeAssertion::GeneratedBy(SubgraphType::Text)).1;
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
                let types_value = self.handle_types(TypeAssertion::GeneratedBy(SubgraphType::Text)).1;
                Expr::Trim {
                    expr: Box::new(types_value), trim_where, trim_what
                }
            },
            "text_concat" => {
                self.expect_state("call7_types");
                let types_value_1 = self.handle_types(TypeAssertion::GeneratedBy(SubgraphType::Text)).1;
                self.expect_state("text_concat_concat");
                self.expect_state("call8_types");
                let types_value_2 = self.handle_types(TypeAssertion::GeneratedBy(SubgraphType::Text)).1;
                Expr::BinaryOp {
                    left: Box::new(types_value_1),
                    op: BinaryOperator::StringConcat,
                    right: Box::new(types_value_2)
                }
            },
            "text_substring" => {
                self.expect_state("call9_types");
                let target_string = self.handle_types(TypeAssertion::GeneratedBy(SubgraphType::Text)).1;
                let mut substring_from = None;
                let mut substring_for = None;
                loop {
                    match self.next_state().as_str() {
                        "text_substring_from" => {
                            self.expect_state("call10_types");
                            substring_from = Some(Box::new(self.handle_types(TypeAssertion::GeneratedBy(SubgraphType::Integer)).1));
                        },
                        "text_substring_for" => {
                            self.expect_state("call11_types");
                            substring_for = Some(Box::new(self.handle_types(TypeAssertion::GeneratedBy(SubgraphType::Integer)).1));
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

        let expr = match self.next_state().as_str() {
            "date_binary" => {
                let (mut date, op, mut integer) = match self.next_state().as_str() {
                    "date_add_subtract" => {
                        self.expect_state("call86_types");
                        let date = self.handle_types(TypeAssertion::GeneratedBy(SubgraphType::Date)).1;
                        self.expect_state("call88_types");
                        let integer = self.handle_types(TypeAssertion::GeneratedBy(SubgraphType::Integer)).1;
                        let op = match self.next_state().as_str() {
                            "date_add_subtract_plus" => BinaryOperator::Plus,
                            "date_add_subtract_minus" => BinaryOperator::Minus,
                            any => self.panic_unexpected(any),
                        };
                        (date, op, integer)
                    },
                    any => self.panic_unexpected(any),
                };
                match self.next_state().as_str() {
                    "date_swap_arguments" => {
                        assert!(op == BinaryOperator::Plus);
                        std::mem::swap(&mut date, &mut integer);
                        self.expect_state("EXIT_date");
                    },
                    "EXIT_date" => { },
                    any => self.panic_unexpected(any),
                }
                Expr::BinaryOp {
                    left: Box::new(date),
                    op,
                    right: Box::new(integer)
                }
            },
            any => self.panic_unexpected(any),
        };
        
        (SubgraphType::Date, expr)
    }

    /// subgraph def_timestamp
    fn handle_timestamp(&mut self) -> (SubgraphType, Expr) {
        self.expect_state("timestamp");

        let expr = match self.next_state().as_str() {
            "timestamp_binary" => {
                let (mut date, op, mut interval) = match self.next_state().as_str() {
                    "timestamp_add_subtract" => {
                        self.expect_state("call94_types");
                        let date = self.handle_types(TypeAssertion::GeneratedBy(SubgraphType::Date)).1;
                        self.expect_state("call95_types");
                        let interval = self.handle_types(TypeAssertion::GeneratedBy(SubgraphType::Interval)).1;
                        let op = match self.next_state().as_str() {
                            "timestamp_add_subtract_plus" => BinaryOperator::Plus,
                            "timestamp_add_subtract_minus" => BinaryOperator::Minus,
                            any => self.panic_unexpected(any),
                        };
                        (date, op, interval)
                    },
                    any => self.panic_unexpected(any),
                };
                match self.next_state().as_str() {
                    "timestamp_swap_arguments" => {
                        assert!(op == BinaryOperator::Plus);
                        std::mem::swap(&mut date, &mut interval);
                        self.expect_state("EXIT_timestamp");
                    },
                    "EXIT_timestamp" => { },
                    any => self.panic_unexpected(any),
                }
                Expr::BinaryOp {
                    left: Box::new(date),
                    op,
                    right: Box::new(interval)
                }
            },
            any => self.panic_unexpected(any),
        };
        
        (SubgraphType::Timestamp, expr)
    }

    /// subgraph def_select_datetime_field
    fn handle_select_datetime_field(&mut self) -> DateTimeField {
        self.expect_state("select_datetime_field");
        let field = match self.next_state().as_str() {
            "select_datetime_field_microseconds" => DateTimeField::Microseconds,
            "select_datetime_field_milliseconds" => DateTimeField::Milliseconds,
            "select_datetime_field_second" => DateTimeField::Second,
            "select_datetime_field_minute" => DateTimeField::Minute,
            "select_datetime_field_hour" => DateTimeField::Hour,
            "select_datetime_field_day" => DateTimeField::Day,
            "select_datetime_field_isodow" => DateTimeField::Isodow,
            "select_datetime_field_week" => DateTimeField::Week,
            "select_datetime_field_month" => DateTimeField::Month,
            "select_datetime_field_quarter" => DateTimeField::Quarter,
            "select_datetime_field_year" => DateTimeField::Year,
            "select_datetime_field_isoyear" => DateTimeField::Isoyear,
            "select_datetime_field_decade" => DateTimeField::Decade,
            "select_datetime_field_century" => DateTimeField::Century,
            "select_datetime_field_millennium" => DateTimeField::Millennium,
            any => self.panic_unexpected(any),
        };
        self.expect_state("EXIT_select_datetime_field");
        field
    }

    /// subgarph def_interval
    fn handle_interval(&mut self) -> (SubgraphType, Expr) {
        self.expect_state("interval");

        let expr = match self.next_state().as_str() {
            "interval_binary" => {
                let (left, op, right) = match self.next_state().as_str() {
                    "interval_add_subtract" => {
                        self.expect_state("call91_types");
                        let interval_1 = self.handle_types(TypeAssertion::GeneratedBy(SubgraphType::Interval)).1;
                        let op = match self.next_state().as_str() {
                            "interval_add_subtract_plus" => BinaryOperator::Plus,
                            "interval_add_subtract_minus" => BinaryOperator::Minus,
                            any => self.panic_unexpected(any),
                        };
                        self.expect_state("call92_types");
                        let interval_2 = self.handle_types(TypeAssertion::GeneratedBy(SubgraphType::Interval)).1;
                        (interval_1, op, interval_2)
                    },
                    any => self.panic_unexpected(any),
                };
                Expr::BinaryOp {
                    left: Box::new(left),
                    op,
                    right: Box::new(right)
                }
            },
            "interval_unary_minus" => {
                self.expect_state("call93_types");
                let interval = self.handle_types(TypeAssertion::GeneratedBy(SubgraphType::Interval)).1;
                Expr::UnaryOp {
                    op: UnaryOperator::Minus,
                    expr: Box::new(interval)
                }
            },
            any => self.panic_unexpected(any),
        };

        self.expect_state("EXIT_interval");
        
        (SubgraphType::Interval, expr)
    }

    /// starting point; calls handle_query for the first time.\
    /// NOTE: If you use this function without a predictor model,\
    /// it will use uniform distributions 
    pub fn generate(&mut self) -> Query {
        if let Some(model) = self.predictor_model.as_mut() {
            model.start_inference();
        }
        let query = self.handle_query().0;
        self.value_chooser.reset();
        // reset the generator
        if let Some(state) = self.next_state_opt() {
            panic!("Couldn't reset state_generator: Received {state}");
        }
        self.substitute_model = Box::new(SubMod::new());
        if let Some(model) = self.predictor_model.as_mut() {
            model.end_inference();
        }
        query
    }

    /// generate the next query with the provided model generating the probabilities
    pub fn generate_with_model(&mut self, model: Box<dyn PathwayGraphModel>) -> (Box<dyn PathwayGraphModel>, Query) {
        self.predictor_model = Some(model);
        let query = self.generate();
        (self.predictor_model.take().unwrap(), query)
    }

    /// generate the next query with the provided dynamic model and value choosers
    pub fn generate_with_substitute_model_and_value_chooser(&mut self, dynamic_model: Box<SubMod>, value_chooser: Box<QVC>) -> Query {
        self.substitute_model = dynamic_model;
        self.value_chooser = value_chooser;
        self.generate()
    }

    /// generate the query and feed it to the model. Useful for training.
    pub fn generate_and_feed_to_model(
            &mut self, dynamic_model: Box<SubMod>, value_chooser: Box<QVC>, model: Box<dyn PathwayGraphModel>
        ) -> Box<dyn PathwayGraphModel> {
        self.train_model = Some(model);
        self.generate_with_substitute_model_and_value_chooser(dynamic_model, value_chooser);
        self.train_model.take().unwrap()
    }
}
