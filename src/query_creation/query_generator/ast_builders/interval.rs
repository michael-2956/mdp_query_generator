use sqlparser::ast::{BinaryOperator, Expr, UnaryOperator};

use crate::{query_creation::{query_generator::{match_next_state, QueryGenerator, ast_builders::types_value::TypeAssertion}, state_generator::{state_choosers::StateChooser, subgraph_type::SubgraphType}}, unwrap_pat};

use super::types::TypesBuilder;

/// subgraph def_interval
pub struct IntervalBuilder { }

impl IntervalBuilder {
    pub fn highlight() -> Expr {
        TypesBuilder::highlight()
    }

    pub fn build<StC: StateChooser>(
        generator: &mut QueryGenerator<StC>, interval: &mut Expr
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

                let left = &mut **unwrap_pat!(interval, Expr::BinaryOp { left, .. }, left);
                match_next_state!(generator, {
                    "call98_types" => { // only timestamp - timestamp
                        TypesBuilder::build(generator, left, TypeAssertion::GeneratedByOneOf(&[SubgraphType::Timestamp]));
                    },
                    "call91_types" => {
                        TypesBuilder::build(generator, left, TypeAssertion::GeneratedByOneOf(&[SubgraphType::Interval]));
                    },
                });

                let right = &mut **unwrap_pat!(interval, Expr::BinaryOp { right, .. }, right);
                *right = TypesBuilder::highlight();
                match_next_state!(generator, {
                    "call99_types" => { // only timestamp - timestamp
                        TypesBuilder::build(generator, right, TypeAssertion::GeneratedByOneOf(&[SubgraphType::Timestamp]));
                    },
                    "call92_types" => {
                        TypesBuilder::build(generator, right, TypeAssertion::GeneratedByOneOf(&[SubgraphType::Interval]));
                    },
                });
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
