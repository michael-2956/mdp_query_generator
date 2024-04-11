#[macro_use]
pub mod query_info;
pub mod ast_builder;
pub mod call_modifiers;
pub mod value_choosers;
pub mod expr_precedence;
pub mod aggregate_function_settings;

use std::path::PathBuf;

use rand::SeedableRng;
use rand_chacha::ChaCha8Rng;
use smol_str::SmolStr;
use sqlparser::ast::{
    self, BinaryOperator, DataType, DateTimeField, Expr, FunctionArg, FunctionArgExpr, Ident, Query, TimezoneInfo, TrimWhereField, UnaryOperator, Value
};

use crate::{config::TomlReadable, training::models::PathwayGraphModel, unwrap_pat};

use super::{
    super::{unwrap_variant, unwrap_variant_or_else},
    state_generator::{markov_chain_generator::subgraph_type::SubgraphType, subgraph_type::ContainsSubgraphType, CallTypes}
};
use self::{
    aggregate_function_settings::{AggregateFunctionAgruments, AggregateFunctionDistribution}, ast_builder::{aggregate_function::AggregateFunctionBuilder, query::QueryBuilder, types::TypesBuilder}, expr_precedence::ExpressionPriority, query_info::{CheckAccessibility, ClauseContext, ColumnRetrievalOptions, DatabaseSchema, IdentName}, value_choosers::QueryValueChooser
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
    clause_context: ClauseContext,
    train_model: Option<Box<dyn PathwayGraphModel>>,
    rng: ChaCha8Rng,
    /// this is used to give a model a look at the whole query\
    /// while it is still being constructed (in other words,\
    /// some builder has the mutable borrow to it)
    /// 
    /// Since there's no multithreading for now, I used unsafe\
    /// code to achieve that.
    /// 
    /// Basically we have to be able to have a look at the whole\
    /// tree while also holding an &mut to some component of it.
    current_query_raw_ptr: Option<*const Query>,
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

macro_rules! match_next_state {
    ($generator:expr, { $($state:pat $(if $guard:expr)? => $body:block),* $(,)? }) => {
        match $generator.next_state().as_str() {
            $(
                $state $(if $guard)? => $body,
            )*
            _any => $generator.panic_unexpected(_any),
        }
    };
    ($generator:expr, { $($state:pat $(if $guard:expr)? => $body:expr),* $(,)? }) => {
        match $generator.next_state().as_str() {
            $(
                $state $(if $guard)? => { $body },
            )*
            _any => $generator.panic_unexpected(_any),
        }
    };
}

pub(crate) use match_next_state;

pub fn empty_ident() -> Ident {
    Ident::new("[?]")
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
                by_one_of.contains_generator_of(selected_type)
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
            substitute_model: Box::new(SubMod::empty()),
            value_chooser: Box::new(QVC::new()),
            clause_context: ClauseContext::new(DatabaseSchema::parse_schema(&config.table_schema_path)),
            config,
            train_model: None,
            rng: ChaCha8Rng::seed_from_u64(1),
            current_query_raw_ptr: None,
        };

        if _self.config.print_schema {
            eprintln!("Relations:\n{}", _self.clause_context.schema_ref());
        }

        _self
    }

    fn next_state_opt(&mut self) -> Option<SmolStr> {
        self.state_generator.next_node_name(
            &mut self.rng,
            &mut self.clause_context,
            &mut *self.substitute_model,
            self.predictor_model.as_mut(),
            self.current_query_raw_ptr.map(|p| unsafe { &*p }),
        ).unwrap()
    }

    fn next_state(&mut self) -> SmolStr {
        let next_state = self.next_state_opt().unwrap();
        // eprintln!("{next_state}");
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

    /// subgraph def_aggregate_function
    fn handle_aggregate_function(&mut self) -> (SubgraphType, Expr) {
        // TODO: refactor this call
        let mut expr = AggregateFunctionBuilder::empty();
        let tp = AggregateFunctionBuilder::build(self, &mut expr);
        (tp, expr)
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

        let (limit_type, limit) = match_next_state!(self, {
            "query_can_skip_limit_set_val" => {
                self.expect_state("query_can_skip_limit");
                (SubgraphType::Undetermined, None)
            },
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
        });
        self.expect_state("EXIT_LIMIT");

        (limit_type, limit)
    }

    /// subgraph def_types
    fn handle_types(&mut self, type_assertion: TypeAssertion) -> (SubgraphType, Expr) {
        self.expect_state("types");

        let selected_types = unwrap_variant!(self.state_generator.get_fn_selected_types_unwrapped(), CallTypes::TypeList);
        let selected_type = match_next_state!(self, {
            "types_select_type_bigint" => SubgraphType::BigInt,
            "types_select_type_integer" => SubgraphType::Integer,
            "types_select_type_numeric" => SubgraphType::Numeric,
            "types_select_type_3vl" => SubgraphType::Val3,
            "types_select_type_text" => SubgraphType::Text,
            "types_select_type_date" => SubgraphType::Date,
            "types_select_type_interval" => SubgraphType::Interval,
            "types_select_type_timestamp" => SubgraphType::Timestamp,
        });
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

        let tp = match_next_state!(self, {
            "types_type_bigint" => SubgraphType::BigInt,
            "types_type_integer" => SubgraphType::Integer,
            "types_type_numeric" => SubgraphType::Numeric,
            "types_type_3vl" => SubgraphType::Val3,
            "types_type_text" => SubgraphType::Text,
            "types_type_date" => SubgraphType::Date,
            "types_type_interval" => SubgraphType::Interval,
            "types_type_timestamp" => SubgraphType::Timestamp,
        });

        self.expect_state("EXIT_types_type");

        tp
    }

    /// subgraph def_types_value
    fn handle_types_value(&mut self, type_assertion: TypeAssertion) -> (SubgraphType, Expr) {
        self.expect_state("types_value");
        let selected_types = unwrap_variant!(self.state_generator.get_fn_selected_types_unwrapped(), CallTypes::TypeList);
        self.state_generator.set_known_list(selected_types.clone());
        let (selected_type, types_value) = match_next_state!(self, {
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
                let mut types_value = Expr::Subquery(Box::new(QueryBuilder::empty()));
                let subquery = &mut **unwrap_variant!(&mut types_value, Expr::Subquery);
                let column_types = QueryBuilder::build(self, subquery);
                let selected_type = match column_types.len() {
                    1 => column_types.into_iter().next().unwrap().1,
                    any => panic!(
                        "Subquery should have selected a single column, \
                        but selected {any} columns. Subquery: {subquery}"
                    ),
                };
                (selected_type, types_value)
            },
        });
        type_assertion.check(&selected_type, &types_value);
        self.expect_state("EXIT_types_value");
        (selected_type, types_value.nest_children_if_needed())
    }

    /// subgraph def_formulas
    fn handle_formulas(&mut self) -> (SubgraphType, Expr) {
        self.expect_state("formulas");
        self.assert_single_type_argument();
        let (selected_type, types_value) = match_next_state!(self, {
            "call2_number" |
            "call1_number" |
            "call0_number" => self.handle_number(),
            "call1_VAL_3" => self.handle_val_3(),
            "call0_text" => self.handle_text(),
            "call0_date" => self.handle_date(),
            "call0_timestamp" => self.handle_timestamp(),
            "call0_interval" => self.handle_interval(),
        });
        self.expect_state("EXIT_formulas");
        (selected_type, types_value)
    }

    /// subgraph def_literals
    fn handle_literals(&mut self) -> (SubgraphType, Expr) {
        self.expect_state("literals");

        let (tp, mut expr) = match_next_state!(self, {
            "bool_literal" => {
                (SubgraphType::Val3, match_next_state!(self, {
                    "true" => Expr::Value(Value::Boolean(true)),
                    "false" => Expr::Value(Value::Boolean(false)),
                }))
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
                let with_field = match_next_state!(self, {
                    "interval_literal_format_string" => false,
                    "interval_literal_with_field" => true,
                });
                let (str_value, leading_field) = self.value_chooser.choose_interval(with_field);
                Expr::Interval {
                    value: Box::new(Expr::Value(Value::SingleQuotedString(str_value.to_string()))),
                    leading_field,
                    leading_precision: None,
                    last_field: None,
                    fractional_seconds_precision: None
                }
            }),
        });

        loop { match_next_state!(self, {
            "literals_explicit_cast" => {
                expr = Expr::Cast { expr: Box::new(expr), data_type: tp.to_data_type() };
            }, // then "number_literal_minus" | "EXIT_literals"
            "number_literal_minus" => {
                expr = Expr::UnaryOp { op: UnaryOperator::Minus, expr: Box::new(expr) };
            }, // then "EXIT_literals"
            "EXIT_literals" => break
        }); }

        (tp, expr)
    }

    /// subgraph def_column_spec
    fn handle_column_spec(&mut self) -> (SubgraphType, Expr) {
        self.expect_state("column_spec");
        let column_types = unwrap_variant_or_else!(
            self.state_generator.get_fn_selected_types_unwrapped(), CallTypes::TypeList, || self.state_generator.print_stack()
        );
        self.expect_state("column_spec_mentioned_in_group_by");
        let only_group_by_columns = match_next_state!(self, {
            "column_spec_mentioned_in_group_by_yes" => true,
            "column_spec_mentioned_in_group_by_no" => false,
        });
        self.expect_state("column_spec_shaded_by_select");
        let shade_by_select_aliases = match_next_state!(self, {
            "column_spec_shaded_by_select_yes" => true,
            "column_spec_shaded_by_select_no" => false,
        });
        self.expect_state("column_spec_aggregatable_columns");
        let only_columns_that_can_be_aggregated = match_next_state!(self, {
            "column_spec_aggregatable_columns_yes" => true,
            "column_spec_aggregatable_columns_no" => false,
        });
        self.expect_state("column_spec_choose_qualified");
        let check_accessibility = match_next_state!(self, {
            "qualified_column_name" => CheckAccessibility::QualifiedColumnName,
            "unqualified_column_name" => CheckAccessibility::ColumnName,
        });
        let column_retrieval_options = ColumnRetrievalOptions::new(
            only_group_by_columns, shade_by_select_aliases, only_columns_that_can_be_aggregated
        );
        let (selected_type, qualified_column_name) = self.value_chooser.choose_column(
            &self.clause_context, column_types, check_accessibility.clone(), column_retrieval_options
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
        match_next_state!(self, {
            "list_expr_multiple_values" => {
                let mut list_expr: Vec<Expr> = vec![types_value];
                loop {
                    match_next_state!(self, {
                        "call49_types" => {
                            list_expr.push(self.handle_types(TypeAssertion::CompatibleWith(inner_type.clone())).1);
                        },
                        "EXIT_list_expr" => break,
                    })
                }
                (SubgraphType::ListExpr(Box::new(inner_type)), list_expr)
            },
        })
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

        let operand = match_next_state!(self, {
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

                    match_next_state!(self, {
                        "simple_case_result" => {
                            self.expect_state("call80_types");
                            self.state_generator.set_compatible_list(out_type.get_compat_types());
                            let result_expr = self.handle_types(TypeAssertion::CompatibleWith(out_type.clone())).1;
                            results.push(result_expr);
                        },
                        "case_else" => break,
                    })
                }

                Some(Box::new(operand_expr))
            },
            "searched_case" => {
                loop {
                    self.expect_state("searched_case_condition");
                    self.expect_state("call76_types");
                    let condition_expr = self.handle_types(TypeAssertion::GeneratedBy(SubgraphType::Val3)).1;
                    conditions.push(condition_expr);

                    match_next_state!(self, {
                        "searched_case_result" => {
                            self.expect_state("call77_types");
                            self.state_generator.set_compatible_list(out_type.get_compat_types());
                            let result_expr = self.handle_types(TypeAssertion::CompatibleWith(out_type.clone())).1;
                            results.push(result_expr);
                        },
                        "case_else" => break,
                    })
                }
                None
            },
        });

        let else_result = match_next_state!(self, {
            "call81_types" => {
                self.state_generator.set_compatible_list(out_type.get_compat_types());
                let else_expr = self.handle_types(TypeAssertion::CompatibleWith(out_type.clone())).1;
                self.expect_state("EXIT_case");
                Some(Box::new(else_expr))
            },
            "EXIT_case" => None,
        });

        (out_type, Expr::Case {
            operand, conditions, results, else_result
        })
    }

    /// subgraph def_VAL_3
    fn handle_val_3(&mut self) -> (SubgraphType, Expr) {
        self.expect_state("VAL_3");
        let val3 = match_next_state!(self, {
            "IsNull" => {
                let is_null_not_flag = match_next_state!(self, {
                    "IsNull_not" => {
                        self.expect_state("call55_types");
                        true
                    },
                    "call55_types" => false,
                });

                // let val3 = &mut *Box::new(Expr::Identifier(Ident::new("[?]")));

                // let types_value = Box::new(Expr::Identifier(Ident::new("[?]")));  // Box::new(ExprBuilder::empty())
                // *val3 = if is_null_not_flag {
                //     Expr::IsNotNull(types_value)
                // } else {
                //     Expr::IsNull(types_value)
                // };
                // let expr = unwrap_pat!(val3, Expr::IsNotNull(expr) | Expr::IsNull(expr), expr);
                // Box::new(ExprBuilder::build(expr))

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
                let is_distinct_not_flag = match_next_state!(self, {
                    "IsDistinctNOT" => {
                        self.expect_state("DISTINCT");
                        true
                    },
                    "DISTINCT" => false,
                });
                self.expect_state("call21_types");
                let types_value_2 = self.handle_types(TypeAssertion::CompatibleWith(tp)).1;
                if is_distinct_not_flag {
                    Expr::IsNotDistinctFrom(Box::new(types_value_1), Box::new(types_value_2))
                } else {
                    Expr::IsDistinctFrom(Box::new(types_value_1), Box::new(types_value_2))
                }
            },
            "Exists" => {
                let exists_not_flag = match_next_state!(self, {
                    "Exists_not" => {
                        self.expect_state("call2_Query");
                        true
                    },
                    "call2_Query" => false,
                });
                let mut expr = Expr::Exists {
                    subquery: Box::new(QueryBuilder::empty()),
                    negated: exists_not_flag
                };
                let subquery = &mut **unwrap_pat!(&mut expr, Expr::Exists { subquery, .. }, subquery);
                QueryBuilder::build(self, subquery);
                expr
            },
            "InList" => {
                self.expect_state("call3_types_type");
                let tp = self.handle_types_type();
                self.state_generator.set_compatible_list(tp.get_compat_types());
                self.expect_state("call57_types");
                let types_value = self.handle_types(TypeAssertion::CompatibleWith(tp)).1;
                let in_list_not_flag = match_next_state!(self, {
                    "InListNot" => {
                        self.expect_state("InListIn");
                        true
                    },
                    "InListIn" => false,
                });
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
                let in_subquery_not_flag = match_next_state!(self, {
                    "InSubqueryNot" => {
                        self.expect_state("InSubqueryIn");
                        true
                    },
                    "InSubqueryIn" => false,
                });
                self.expect_state("call3_Query");
                let mut expr = Expr::InSubquery {
                    expr: Box::new(types_value),
                    subquery: Box::new(QueryBuilder::empty()),
                    negated: in_subquery_not_flag
                };
                let subquery = &mut **unwrap_pat!(&mut expr, Expr::InSubquery { subquery, .. }, subquery);
                QueryBuilder::build(self, subquery);
                expr
            },
            "Between" => {
                self.expect_state("call5_types_type");
                let tp = self.handle_types_type();
                self.state_generator.set_compatible_list(tp.get_compat_types());
                self.expect_state("call59_types");
                let types_value_1 = self.handle_types(TypeAssertion::CompatibleWith(tp.clone())).1;
                let between_not_flag = match_next_state!(self, {
                    "BetweenBetweenNot" => {
                        self.expect_state("BetweenBetween");
                        true
                    },
                    "BetweenBetween" => false,
                });
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
                let binary_comp_op = match_next_state!(self, {
                    "BinaryCompEqual" => BinaryOperator::Eq,
                    "BinaryCompUnEqual" => BinaryOperator::NotEq,
                    "BinaryCompLess" => BinaryOperator::Lt,
                    "BinaryCompLessEqual" => BinaryOperator::LtEq,
                    "BinaryCompGreater" => BinaryOperator::Gt,
                    "BinaryCompGreaterEqual" => BinaryOperator::GtEq,
                });
                self.expect_state("call24_types");
                let types_value_2 = self.handle_types(TypeAssertion::CompatibleWith(tp)).1;
                Expr::BinaryOp {
                    left: Box::new(types_value_1),
                    op: binary_comp_op,
                    right: Box::new(types_value_2)
                }
            },
            "AnyAll" => {
                let mut val3_expr;
                
                self.expect_state("AnyAllSelectOp");
                let any_all_op = match_next_state!(self, {
                    "AnyAllEqual" => BinaryOperator::Eq,
                    "AnyAllUnEqual" => BinaryOperator::NotEq,
                    "AnyAllLess" => BinaryOperator::Lt,
                    "AnyAllLessEqual" => BinaryOperator::LtEq,
                    "AnyAllGreater" => BinaryOperator::Gt,
                    "AnyAllGreaterEqual" => BinaryOperator::GtEq,
                });
                val3_expr = Expr::BinaryOp {
                    left: Box::new(TypesBuilder::empty()),
                    op: any_all_op,
                    right: Box::new(TypesBuilder::empty()),
                };

                self.expect_state("call2_types_type");
                let tp = self.handle_types_type();
                self.state_generator.set_compatible_list(tp.get_compat_types());
                
                self.expect_state("call61_types");
                let left = &mut **unwrap_pat!(&mut val3_expr, Expr::BinaryOp { left, .. }, left);
                *left = self.handle_types(TypeAssertion::CompatibleWith(tp)).1;

                let right = &mut **unwrap_pat!(&mut val3_expr, Expr::BinaryOp { right, .. }, right);
                self.expect_state("AnyAllAnyAll");
                *right = match_next_state!(self, {
                    "AnyAllAnyAllAll" => Expr::AllOp(Box::new(TypesBuilder::empty())),
                    "AnyAllAnyAllAny" => Expr::AnyOp(Box::new(TypesBuilder::empty())),
                });

                let right_inner = &mut **unwrap_variant!(right, Expr::AllOp);
                self.expect_state("AnyAllSelectIter");
                match_next_state!(self, {
                    "call4_Query" => {
                        *right_inner = Expr::Subquery(Box::new(QueryBuilder::empty()));
                        let subquery = &mut **unwrap_variant!(right_inner, Expr::Subquery);
                        QueryBuilder::build(self, subquery);
                    },
                });

                val3_expr
            },
            "BinaryStringLike" => {
                self.expect_state("call25_types");
                let types_value_1 = self.handle_types(TypeAssertion::GeneratedBy(SubgraphType::Text)).1;
                let string_like_not_flag = match_next_state!(self, {
                    "BinaryStringLikeNot" => {
                        self.expect_state("BinaryStringLikeIn");
                        true
                    },
                    "BinaryStringLikeIn" => false,
                });
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
                let binary_bool_op = match_next_state!(self, {
                    "BinaryBooleanOpV3AND" => BinaryOperator::And,
                    "BinaryBooleanOpV3OR" => BinaryOperator::Or,
                    "BinaryBooleanOpV3XOR" => BinaryOperator::Xor,
                });
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
        });
        self.expect_state("EXIT_VAL_3");
        (SubgraphType::Val3, val3)
    }

    /// subgraph def_numeric
    fn handle_number(&mut self) -> (SubgraphType, Expr) {
        self.expect_state("number");
        let requested_number_type = self.assert_single_type_argument();
        let (number_type, number) = match_next_state!(self, {
            "BinaryNumberOp" => {
                self.expect_state("call48_types");
                self.state_generator.set_compatible_list(requested_number_type.get_compat_types());
                let types_value_1 = self.handle_types(TypeAssertion::CompatibleWith(requested_number_type.clone())).1;
                let numeric_binary_op = match_next_state!(self, {
                    "binary_number_bin_and" => BinaryOperator::BitwiseAnd,
                    "binary_number_bin_or" => BinaryOperator::BitwiseOr,
                    "binary_number_bin_xor" => BinaryOperator::PGBitwiseXor,
                    "binary_number_exp" => BinaryOperator::PGExp,
                    "binary_number_div" => BinaryOperator::Divide,
                    "binary_number_minus" => BinaryOperator::Minus,
                    "binary_number_mul" => BinaryOperator::Multiply,
                    "binary_number_plus" => BinaryOperator::Plus,
                });
                self.expect_state("call47_types");
                let types_value_2 = self.handle_types(TypeAssertion::CompatibleWith(requested_number_type.clone())).1;
                (requested_number_type, Expr::BinaryOp {
                    left: Box::new(types_value_1),
                    op: numeric_binary_op,
                    right: Box::new(types_value_2)
                })
            },
            "UnaryNumberOp" => {
                let numeric_unary_op = match_next_state!(self, {
                    "unary_number_abs" => UnaryOperator::PGAbs,
                    "unary_number_bin_not" => UnaryOperator::PGBitwiseNot,
                    "unary_number_cub_root" => UnaryOperator::PGCubeRoot,
                    "unary_number_minus" => UnaryOperator::Minus,
                    "unary_number_plus" => UnaryOperator::Plus,
                    "unary_number_sq_root" => UnaryOperator::PGSquareRoot,
                });
                if numeric_unary_op == UnaryOperator::Minus {
                    self.expect_state("call89_types");
                } else {
                    self.expect_state("call1_types");
                }
                self.state_generator.set_compatible_list(requested_number_type.get_compat_types());
                let number = self.handle_types(TypeAssertion::CompatibleWith(requested_number_type.clone())).1;
                (requested_number_type, Expr::UnaryOp {
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
        });
        self.expect_state("EXIT_number");
        (number_type, number)
    }

    /// subgraph def_text
    fn handle_text(&mut self) -> (SubgraphType, Expr) {
        self.expect_state("text");
        let string = match_next_state!(self, {
            "text_trim" => {
                let (trim_where, trim_what) = match_next_state!(self, {
                    "call6_types" => {
                        let types_value = self.handle_types(TypeAssertion::GeneratedBy(SubgraphType::Text)).1;
                        let spec_mode = match_next_state!(self, {
                            "BOTH" => TrimWhereField::Both,
                            "LEADING" => TrimWhereField::Leading,
                            "TRAILING" => TrimWhereField::Trailing,
                        });
                        self.expect_state("call5_types");
                        (Some(spec_mode), Some(Box::new(types_value)))
                    },
                    "call5_types" => (None, None),
                });
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
                    match_next_state!(self, {
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
                    })
                }
                Expr::Substring {
                    expr: Box::new(target_string),
                    substring_from,
                    substring_for,
                }
            },
        });
        self.expect_state("EXIT_text");
        (SubgraphType::Text, string)
    }

    /// subgarph def_date
    fn handle_date(&mut self) -> (SubgraphType, Expr) {
        self.expect_state("date");

        let expr = match_next_state!(self, {
            "date_binary" => {
                let (mut date, op, mut integer) = match_next_state!(self, {
                    "date_add_subtract" => {
                        self.expect_state("call86_types");
                        let date = self.handle_types(TypeAssertion::GeneratedBy(SubgraphType::Date)).1;
                        self.expect_state("call88_types");
                        let integer = self.handle_types(TypeAssertion::GeneratedBy(SubgraphType::Integer)).1;
                        let op = match_next_state!(self, {
                            "date_add_subtract_plus" => BinaryOperator::Plus,
                            "date_add_subtract_minus" => BinaryOperator::Minus,
                        });
                        (date, op, integer)
                    },
                });
                match_next_state!(self, {
                    "date_swap_arguments" => {
                        assert!(op == BinaryOperator::Plus);
                        std::mem::swap(&mut date, &mut integer);
                        self.expect_state("EXIT_date");
                    },
                    "EXIT_date" => { },
                });
                Expr::BinaryOp {
                    left: Box::new(date),
                    op,
                    right: Box::new(integer)
                }
            },
        });
        
        (SubgraphType::Date, expr)
    }

    /// subgraph def_timestamp
    fn handle_timestamp(&mut self) -> (SubgraphType, Expr) {
        self.expect_state("timestamp");

        let expr = match_next_state!(self, {
            "timestamp_binary" => {
                let (mut date, op, mut interval) = match_next_state!(self, {
                    "timestamp_add_subtract" => {
                        self.expect_state("call94_types");
                        let date = self.handle_types(TypeAssertion::GeneratedBy(SubgraphType::Date)).1;
                        self.expect_state("call95_types");
                        let interval = self.handle_types(TypeAssertion::GeneratedBy(SubgraphType::Interval)).1;
                        let op = match_next_state!(self, {
                            "timestamp_add_subtract_plus" => BinaryOperator::Plus,
                            "timestamp_add_subtract_minus" => BinaryOperator::Minus,
                        });
                        (date, op, interval)
                    },
                });
                match_next_state!(self, {
                    "timestamp_swap_arguments" => {
                        assert!(op == BinaryOperator::Plus);
                        std::mem::swap(&mut date, &mut interval);
                        self.expect_state("EXIT_timestamp");
                    },
                    "EXIT_timestamp" => { },
                });
                Expr::BinaryOp {
                    left: Box::new(date),
                    op,
                    right: Box::new(interval)
                }
            },
        });
        
        (SubgraphType::Timestamp, expr)
    }

    /// subgraph def_select_datetime_field
    fn handle_select_datetime_field(&mut self) -> DateTimeField {
        self.expect_state("select_datetime_field");
        let field = match_next_state!(self, {
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
        });
        self.expect_state("EXIT_select_datetime_field");
        field
    }

    /// subgarph def_interval
    fn handle_interval(&mut self) -> (SubgraphType, Expr) {
        self.expect_state("interval");

        let expr = match_next_state!(self, {
            "interval_binary" => {
                let (left, op, right) = match_next_state!(self, {
                    "interval_add_subtract" => {
                        self.expect_state("call91_types");
                        let interval_1 = self.handle_types(TypeAssertion::GeneratedBy(SubgraphType::Interval)).1;
                        let op = match_next_state!(self, {
                            "interval_add_subtract_plus" => BinaryOperator::Plus,
                            "interval_add_subtract_minus" => BinaryOperator::Minus,
                        });
                        self.expect_state("call92_types");
                        let interval_2 = self.handle_types(TypeAssertion::GeneratedBy(SubgraphType::Interval)).1;
                        (interval_1, op, interval_2)
                    },
                });
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
        });

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
        let mut query = QueryBuilder::empty();

        self.current_query_raw_ptr = Some(&query);  // scary!
        QueryBuilder::build(self, &mut query);
        self.current_query_raw_ptr.take();

        self.value_chooser.reset();
        // reset the generator
        if let Some(state) = self.next_state_opt() {
            panic!("Couldn't reset state_generator: Received {state}");
        }
        self.substitute_model = Box::new(SubMod::empty());
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
