use sqlparser::ast::{BinaryOperator, Expr, UnaryOperator};

use crate::{query_creation::{query_generator::{match_next_state, QueryGenerator, TypeAssertion}, state_generator::{state_choosers::StateChooser, subgraph_type::SubgraphType, substitute_models::SubstituteModel}}, unwrap_pat};

use super::types::TypesBuilder;

/// subgraph def_interval
pub struct IntervalBuilder { }

impl IntervalBuilder {
    pub fn highlight() -> Expr {
        TypesBuilder::highlight()
    }

    pub fn build<SubMod: SubstituteModel, StC: StateChooser>(
        generator: &mut QueryGenerator<SubMod, StC>, interval: &mut Expr
    ) -> SubgraphType {
        generator.expect_state("interval");

        match_next_state!(generator, {
            "interval_binary" => {
                generator.expect_state("interval_add_subtract");
                let op = match_next_state!(generator, {
                    "interval_add_subtract_plus" => BinaryOperator::Plus,
                    "interval_add_subtract_minus" => BinaryOperator::Minus,
                });
                *interval = Expr::BinaryOp {
                    left: Box::new(TypesBuilder::highlight()),
                    op,
                    right: Box::new(TypesBuilder::nothing())
                };

                generator.expect_state("call91_types");
                let left = &mut **unwrap_pat!(interval, Expr::BinaryOp { left, .. }, left);
                TypesBuilder::build(generator, left, TypeAssertion::GeneratedBy(SubgraphType::Interval));

                generator.expect_state("call92_types");
                let right = &mut **unwrap_pat!(interval, Expr::BinaryOp { right, .. }, right);
                *right = TypesBuilder::highlight();
                TypesBuilder::build(generator, right, TypeAssertion::GeneratedBy(SubgraphType::Interval));
            },
            "interval_unary_minus" => {
                *interval = Expr::UnaryOp {
                    op: UnaryOperator::Minus,
                    expr: Box::new(TypesBuilder::highlight())
                };
                generator.expect_state("call93_types");
                let expr = &mut **unwrap_pat!(interval, Expr::UnaryOp { expr, .. }, expr);
                TypesBuilder::build(generator, expr, TypeAssertion::GeneratedBy(SubgraphType::Interval));
            },
        });

        generator.expect_state("EXIT_interval");
        
        SubgraphType::Interval
    }
}
