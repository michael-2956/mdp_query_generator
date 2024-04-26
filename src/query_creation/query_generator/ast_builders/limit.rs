use sqlparser::ast::{Expr, Value};

use crate::query_creation::{query_generator::{match_next_state, QueryGenerator, ast_builders::types_value::TypeAssertion}, state_generator::{state_choosers::StateChooser, subgraph_type::SubgraphType}};

use super::types::TypesBuilder;

/// subgraph def_LIMIT
pub struct LimitBuilder { }

impl LimitBuilder {
    pub fn highlight() -> Option<Expr> {
        Some(TypesBuilder::highlight())
    }

    pub fn build<StC: StateChooser>(
        generator: &mut QueryGenerator<StC>, limit: &mut Option<Expr>
    ) {
        generator.expect_state("LIMIT");

        match_next_state!(generator, {
            "query_can_skip_limit_set_val" => {
                generator.expect_state("query_can_skip_limit");
                *limit = None;
            },
            "single_row_true" => {
                *limit = Some(Expr::Value(Value::Number("1".to_string(), false)));
            },
            "limit_num" => {
                generator.expect_state("call52_types");
                let limit_expr = limit.as_mut().unwrap();
                TypesBuilder::build(generator, limit_expr, TypeAssertion::GeneratedByOneOf(
                    &[SubgraphType::Numeric, SubgraphType::Integer, SubgraphType::BigInt]
                ));
            },
        });

        generator.expect_state("EXIT_LIMIT");
    }
}
