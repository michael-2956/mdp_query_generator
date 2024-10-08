use sqlparser::ast::SetExpr;

use crate::{query_creation::{query_generator::{match_next_state, QueryGenerator}, state_generator::state_choosers::StateChooser}, unwrap_variant};

use super::select_query::SelectQueryBuilder;

/// subgraph def_set_expression
pub struct SetExpressionBuilder { }

impl SetExpressionBuilder {
    pub fn nothing() -> SetExpr {
        SetExpr::Select(Box::new(SelectQueryBuilder::nothing()))
    }

    /// highlights the output type. Select query does not\
    /// have the usual "highlight" function. Instead,\
    /// use "nothing" before generation.
    /// 
    /// This function is only needed to highlight that \
    /// some decision will be affecting the single \
    /// output column of the query.
    pub fn highlight_type() -> SetExpr {
        SetExpr::Select(Box::new(SelectQueryBuilder::highlight_type()))
    }

    pub fn build<StC: StateChooser>(
        generator: &mut QueryGenerator<StC>, body: &mut SetExpr
    ) {
        generator.expect_state("set_expression");

        match_next_state!(generator, {
            "call0_SELECT_query" => {
                let select_body = &mut **unwrap_variant!(body, SetExpr::Select);
                SelectQueryBuilder::build(generator, select_body);
            }
        });

        generator.expect_state("EXIT_set_expression");
    }
}
