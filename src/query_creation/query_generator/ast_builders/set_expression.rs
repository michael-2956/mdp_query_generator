use sqlparser::ast::{SetExpr, SetOperator, SetQuantifier};

use crate::{query_creation::{query_generator::{ast_builders::query::QueryBuilder, match_next_state, QueryGenerator}, state_generator::{markov_chain_generator::markov_chain::QueryTypes, state_choosers::StateChooser, CallTypes}}, unwrap_pat, unwrap_variant};

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
            },
            "call7_Query" => {
                *body = SetExpr::Query(Box::new(QueryBuilder::nothing()));
                QueryBuilder::build(generator, &mut **unwrap_variant!(body, SetExpr::Query));
            },
            "set_expression_set_operation" => {
                let arg = unwrap_variant!(generator.state_generator.get_fn_selected_types_unwrapped(), CallTypes::QueryTypes);
                assert!(matches!(arg, QueryTypes::ColumnTypeLists { .. }));  // needed for us to work properly
                // *body = SetExpr::;
                let op = match_next_state!(generator, {
                    "query_set_op_intersect" => SetOperator::Intersect,
                    "query_set_op_union" => SetOperator::Union,
                    "query_set_op_except" => SetOperator::Except,
                });
                *body = SetExpr::SetOperation {
                    op,
                    set_quantifier: SetQuantifier::None,
                    left: Box::new(Self::nothing()),
                    right: Box::new(Self::nothing()),
                };
                let left = &mut **unwrap_pat!(body, SetExpr::SetOperation { left, .. }, left);
                match_next_state!(generator, {
                    "call3_set_expression" => Self::build(generator, left),
                    "call2_set_expression" => Self::build(generator, left),
                    "call5_Query" => {
                        *left = SetExpr::Query(Box::new(QueryBuilder::nothing()));
                        QueryBuilder::build(generator, &mut **unwrap_variant!(left, SetExpr::Query));
                    }
                });
                let right = &mut **unwrap_pat!(body, SetExpr::SetOperation { right, .. }, right);
                match_next_state!(generator, {
                    "call4_set_expression" => Self::build(generator, right),
                    "call5_set_expression" => Self::build(generator, right),
                    "call6_Query" => {
                        *right = SetExpr::Query(Box::new(QueryBuilder::nothing()));
                        QueryBuilder::build(generator, &mut **unwrap_variant!(right, SetExpr::Query));
                    }
                });
            },
        });

        generator.expect_state("EXIT_set_expression");
    }
}
