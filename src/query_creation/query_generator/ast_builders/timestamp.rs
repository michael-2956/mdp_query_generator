use sqlparser::ast::{BinaryOperator, Expr};

use crate::{query_creation::{query_generator::{match_next_state, QueryGenerator, ast_builders::types_value::TypeAssertion}, state_generator::{state_choosers::StateChooser, subgraph_type::SubgraphType}}, unwrap_pat};

use super::types::TypesBuilder;

/// subgraph def_timestamp
pub struct TimestampBuilder { }

impl TimestampBuilder {
    pub fn highlight() -> Expr {
        TypesBuilder::highlight()
    }

    pub fn build<StC: StateChooser>(
        generator: &mut QueryGenerator<StC>, timestamp: &mut Expr
    ) -> SubgraphType {
        generator.expect_state("timestamp");

        match_next_state!(generator, {
            "timestamp_binary" => {
                generator.expect_state("timestamp_add_subtract");
                
                let op = match_next_state!(generator, {
                    "timestamp_add_subtract_plus" => BinaryOperator::Plus,
                    "timestamp_add_subtract_minus" => BinaryOperator::Minus,
                });

                *timestamp = Expr::BinaryOp {
                    left: Box::new(TypesBuilder::nothing()),
                    op,
                    right: Box::new(TypesBuilder::nothing())
                };

                let do_swap = match_next_state!(generator, {
                    "timestamp_swap_arguments" => {
                        generator.expect_state("call94_types");
                        true
                    },
                    "call94_types" => false,
                });

                let expr = if do_swap {
                    &mut **unwrap_pat!(timestamp, Expr::BinaryOp { right, .. }, right)
                } else {
                    &mut **unwrap_pat!(timestamp, Expr::BinaryOp { left, .. }, left)
                };
                *expr = TypesBuilder::highlight();
                TypesBuilder::build(generator, expr, TypeAssertion::GeneratedBy(SubgraphType::Date));

                generator.expect_state("call95_types");
                let expr = if do_swap {
                    &mut **unwrap_pat!(timestamp, Expr::BinaryOp { left, .. }, left)
                } else {
                    &mut **unwrap_pat!(timestamp, Expr::BinaryOp { right, .. }, right)
                };
                *expr = TypesBuilder::highlight();
                TypesBuilder::build(generator, expr, TypeAssertion::GeneratedBy(SubgraphType::Interval));
            },
        });

        generator.expect_state("EXIT_timestamp");
        
        SubgraphType::Timestamp
    }
}
