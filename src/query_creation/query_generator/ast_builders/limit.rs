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
            "is_limit_present" => {
                generator.expect_state("limit_not_present");
                match_next_state!(generator, {
                    "single_row_true" => {
                        *limit = Some(Expr::Value(Value::Number("1".to_string(), false)));
                        generator.clause_context.query_mut().set_limit_present();
                    },
                    "limit_num" => {
                        generator.expect_state("call52_types");
                        let limit_expr = limit.as_mut().unwrap();
                        generator.state_generator.set_compatible_list(SubgraphType::Numeric.get_compat_types());
                        TypesBuilder::build(generator, limit_expr, TypeAssertion::CompatibleWith(SubgraphType::Numeric));
                        generator.clause_context.query_mut().set_limit_present();
                    },
                });
            },
        });

        generator.expect_state("EXIT_LIMIT");
    }
}
