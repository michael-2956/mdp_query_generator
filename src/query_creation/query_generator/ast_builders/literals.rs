use sqlparser::ast::{DataType, Expr, Interval, TimezoneInfo, UnaryOperator, Value};

use crate::{query_creation::{query_generator::{highlight_str, match_next_state, value_chooser, QueryGenerator}, state_generator::{state_choosers::StateChooser, subgraph_type::SubgraphType}}, unwrap_pat};

use super::types::TypesBuilder;

/// subgraph def_literals
pub struct LiteralsBuilder { }

impl LiteralsBuilder {
    pub fn highlight() -> Expr {
        TypesBuilder::highlight()
    }

    pub fn build<StC: StateChooser>(
        generator: &mut QueryGenerator<StC>, expr: &mut Expr
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
                        (SubgraphType::BigInt, value_chooser!(generator).choose_bigint())
                    },
                    "number_literal_integer" => {
                        (SubgraphType::Integer, value_chooser!(generator).choose_integer())
                    },
                    "number_literal_numeric" => {
                        (SubgraphType::Numeric, value_chooser!(generator).choose_numeric())
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
                *expr = Expr::Value(Value::SingleQuotedString(highlight_str()));
                *unwrap_pat!(expr, Expr::Value(Value::SingleQuotedString(s)), s) = value_chooser!(generator).choose_text();
                SubgraphType::Text
            },
            "date_literal" => {
                *expr = Expr::TypedString { data_type: DataType::Date, value: highlight_str() };
                *unwrap_pat!(expr, Expr::TypedString { value, .. }, value) = value_chooser!(generator).choose_date();
                SubgraphType::Date
            },
            "timestamp_literal" => {
                *expr = Expr::TypedString { data_type: DataType::Timestamp(None, TimezoneInfo::None), value: highlight_str() };
                *unwrap_pat!(expr, Expr::TypedString { value, .. }, value) = value_chooser!(generator).choose_timestamp();
                SubgraphType::Timestamp
            },
            "interval_literal" => {
                let with_field = match_next_state!(generator, {
                    "interval_literal_format_string" => false,
                    "interval_literal_with_field" => true,
                });

                *expr = Expr::Interval(Interval {
                    value: Box::new(Expr::Value(Value::SingleQuotedString(highlight_str()))),
                    leading_field: None,
                    leading_precision: None,
                    last_field: None,
                    fractional_seconds_precision: None
                });

                let value = unwrap_pat!(&mut **unwrap_pat!(
                    expr, Expr::Interval(Interval { value, .. }), value
                ), Expr::Value(Value::SingleQuotedString(s)), s);

                let leading_field;
                (*value, leading_field) = value_chooser!(generator).choose_interval(with_field);

                *unwrap_pat!(expr, Expr::Interval(Interval { leading_field, .. }), leading_field) = leading_field;

                SubgraphType::Interval
            },
        });

        match_next_state!(generator, {
            "literals_explicit_cast" => {
                *expr = Expr::Cast { expr: Box::new(expr.clone()), data_type: tp.to_data_type(), format: None };
                generator.expect_state("EXIT_literals");
            }, // then 
            "EXIT_literals" => { }
        });

        tp
    }
}
