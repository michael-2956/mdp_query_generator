use sqlparser::ast::{BinaryOperator, Expr};

use crate::{query_creation::{query_generator::{match_next_state, QueryGenerator, ast_builders::types_value::TypeAssertion}, state_generator::{state_choosers::StateChooser, subgraph_type::SubgraphType}}, unwrap_pat};

use super::types::TypesBuilder;

/// subgarph def_date
pub struct DateBuilder { }

impl DateBuilder {
    pub fn highlight() -> Expr {
        TypesBuilder::highlight()
    }

    pub fn build<StC: StateChooser>(
        generator: &mut QueryGenerator<StC>, date: &mut Expr
    ) -> SubgraphType {
        generator.expect_state("date");

        match_next_state!(generator, {
            "date_binary" => {
                generator.expect_state("date_add_subtract");

                let op = match_next_state!(generator, {
                    "date_add_subtract_plus" => BinaryOperator::Plus,
                    "date_add_subtract_minus" => BinaryOperator::Minus,
                });
                *date = Expr::BinaryOp {
                    left: Box::new(TypesBuilder::nothing()),
                    op,
                    right: Box::new(TypesBuilder::nothing())
                };

                let do_swap = match_next_state!(generator, {
                    "date_swap_arguments" => {
                        generator.expect_state("call86_types");
                        true
                    },
                    "call86_types" => false,
                });

                let expr = if do_swap {
                    &mut **unwrap_pat!(date, Expr::BinaryOp { right, .. }, right)
                } else {
                    &mut **unwrap_pat!(date, Expr::BinaryOp { left, .. }, left)
                };
                *expr = TypesBuilder::highlight();
                TypesBuilder::build(generator, expr, TypeAssertion::GeneratedBy(SubgraphType::Date));

                generator.expect_state("call88_types");
                let expr = if do_swap {
                    &mut **unwrap_pat!(date, Expr::BinaryOp { left, .. }, left)
                } else {
                    &mut **unwrap_pat!(date, Expr::BinaryOp { right, .. }, right)
                };
                *expr = TypesBuilder::highlight();
                TypesBuilder::build(generator, expr, TypeAssertion::GeneratedBy(SubgraphType::Integer));
            },
        });

        generator.expect_state("EXIT_date");
        
        SubgraphType::Date
    }
}
