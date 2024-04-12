use sqlparser::ast::{DataType, Expr, TimezoneInfo, UnaryOperator, Value};

use crate::query_creation::{query_generator::{match_next_state, value_choosers::QueryValueChooser, QueryGenerator}, state_generator::{state_choosers::StateChooser, subgraph_type::SubgraphType, substitute_models::SubstituteModel}};

use super::types::TypesBuilder;

/// subgraph def_literals
pub struct LiteralsBuilder { }

impl LiteralsBuilder {
    pub fn empty() -> Expr {
        TypesBuilder::empty()
    }

    pub fn build<SubMod: SubstituteModel, StC: StateChooser, QVC: QueryValueChooser>(
        generator: &mut QueryGenerator<SubMod, StC, QVC>, expr: &mut Expr
    ) -> SubgraphType {
        generator.expect_state("literals");

        let tp = match_next_state!(generator, {
            "bool_literal" => {
                *expr = match_next_state!(generator, {
                    "true" => Expr::Value(Value::Boolean(true)),
                    "false" => Expr::Value(Value::Boolean(false)),
                });
                SubgraphType::Val3
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
                        (SubgraphType::BigInt, generator.value_chooser.choose_bigint())
                    },
                    "number_literal_integer" => {
                        (SubgraphType::Integer, generator.value_chooser.choose_integer())
                    },
                    "number_literal_numeric" => {
                        (SubgraphType::Numeric, (generator.value_chooser.choose_numeric()))
                    },
                    _ => unreachable!(),
                };
                if let Some(number_str) = number_str.strip_prefix('-') {
                    *expr = Expr::UnaryOp { op: UnaryOperator::Minus, expr: Box::new(
                        Expr::Value(Value::Number(number_str.to_string(), false))
                    ) };
                } else {
                    *expr = Expr::Value(Value::Number(number_str.clone(), false));
                }
                number_type
            },
            "text_literal" => {
                *expr = Expr::Value(Value::SingleQuotedString(generator.value_chooser.choose_string()));
                SubgraphType::Text
            },
            "date_literal" => {
                *expr = Expr::TypedString {
                    data_type: DataType::Date, value: generator.value_chooser.choose_date(),
                };
                SubgraphType::Date
            },
            "timestamp_literal" => {
                *expr = Expr::TypedString {
                    data_type: DataType::Timestamp(None, TimezoneInfo::None), value: generator.value_chooser.choose_timestamp(),
                };
                SubgraphType::Timestamp
            },
            "interval_literal" => {
                let with_field = match_next_state!(generator, {
                    "interval_literal_format_string" => false,
                    "interval_literal_with_field" => true,
                });
                let (str_value, leading_field) = generator.value_chooser.choose_interval(with_field);
                *expr = Expr::Interval {
                    value: Box::new(Expr::Value(Value::SingleQuotedString(str_value.to_string()))),
                    leading_field,
                    leading_precision: None,
                    last_field: None,
                    fractional_seconds_precision: None
                };
                SubgraphType::Interval
            },
        });

        match_next_state!(generator, {
            "literals_explicit_cast" => {
                *expr = Expr::Cast { expr: Box::new(expr.clone()), data_type: tp.to_data_type() };
                generator.expect_state("EXIT_literals");
            }, // then 
            "EXIT_literals" => { }
        });

        tp
    }
}
