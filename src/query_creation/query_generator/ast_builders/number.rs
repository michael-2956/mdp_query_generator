use sqlparser::ast::{BinaryOperator, Expr, UnaryOperator};

use crate::{query_creation::{query_generator::{match_next_state, QueryGenerator, ast_builders::types_value::TypeAssertion}, state_generator::{state_choosers::StateChooser, subgraph_type::SubgraphType}}, unwrap_pat};

use super::{select_datetime_field::SelectDatetimeFieldBuilder, types::TypesBuilder};

/// subgraph def_numeric
pub struct NumberBuilder { }

impl NumberBuilder {
    pub fn highlight() -> Expr {
        TypesBuilder::highlight()
    }

    pub fn build<StC: StateChooser>(
        generator: &mut QueryGenerator<StC>, number: &mut Expr
    ) -> SubgraphType {
        generator.expect_state("number");
        let requested_number_type = generator.assert_single_type_argument();
        let number_type = match_next_state!(generator, {
            "BinaryNumberOp" => {
                let numeric_binary_op = match_next_state!(generator, {
                    "binary_number_bin_and" => BinaryOperator::BitwiseAnd,
                    "binary_number_bin_or" => BinaryOperator::BitwiseOr,
                    "binary_number_bin_xor" => BinaryOperator::PGBitwiseXor,
                    "binary_number_exp" => BinaryOperator::PGExp,
                    "binary_number_div" => BinaryOperator::Divide,
                    "binary_number_minus" => BinaryOperator::Minus,
                    "binary_number_mul" => BinaryOperator::Multiply,
                    "binary_number_plus" => BinaryOperator::Plus,
                });
                *number = Expr::BinaryOp {
                    left: Box::new(TypesBuilder::highlight()),
                    op: numeric_binary_op,
                    right: Box::new(TypesBuilder::nothing())
                };
                generator.state_generator.set_compatible_list(requested_number_type.get_compat_types());

                generator.expect_state("call47_types");
                let left = &mut **unwrap_pat!(number, Expr::BinaryOp { left, .. }, left);
                TypesBuilder::build(generator, left, TypeAssertion::CompatibleWith(requested_number_type.clone()));

                generator.expect_state("call48_types");
                let right = &mut **unwrap_pat!(number, Expr::BinaryOp { right, .. }, right);
                *right = TypesBuilder::highlight();
                TypesBuilder::build(generator, right, TypeAssertion::CompatibleWith(requested_number_type.clone()));

                requested_number_type
            },
            "UnaryNumberOp" => {
                let numeric_unary_op = match_next_state!(generator, {
                    "unary_number_abs" => UnaryOperator::PGAbs,
                    "unary_number_bin_not" => UnaryOperator::PGBitwiseNot,
                    "unary_number_cub_root" => UnaryOperator::PGCubeRoot,
                    "unary_number_minus" => UnaryOperator::Minus,
                    "unary_number_plus" => UnaryOperator::Plus,
                    "unary_number_sq_root" => UnaryOperator::PGSquareRoot,
                });
                *number = Expr::UnaryOp {
                    op: numeric_unary_op,
                    expr: Box::new(TypesBuilder::highlight())
                };
                generator.state_generator.set_compatible_list(requested_number_type.get_compat_types());

                if numeric_unary_op == UnaryOperator::Minus {
                    generator.expect_state("call89_types");
                } else {
                    generator.expect_state("call1_types");
                }
                let expr = &mut **unwrap_pat!(number, Expr::UnaryOp { expr, .. }, expr);
                TypesBuilder::build(generator, expr, TypeAssertion::CompatibleWith(requested_number_type.clone()));

                requested_number_type
            },
            "number_string_position" => {
                *number = Expr::Position {
                    expr: Box::new(TypesBuilder::highlight()),
                    r#in: Box::new(TypesBuilder::nothing())
                };

                generator.expect_state("call2_types");
                let expr = &mut **unwrap_pat!(number, Expr::Position { expr, .. }, expr);
                TypesBuilder::build(generator, expr, TypeAssertion::GeneratedBy(SubgraphType::Text));

                generator.expect_states(&["string_position_in", "call3_types"]);
                let r#in = &mut **unwrap_pat!(number, Expr::Position { r#in, .. }, r#in);
                *r#in = TypesBuilder::highlight();
                TypesBuilder::build(generator, r#in, TypeAssertion::GeneratedBy(SubgraphType::Text));
                
                SubgraphType::Integer
            },
            "number_extract_field_from_date" => {
                generator.expect_state("call0_select_datetime_field");
                *number = Expr::Extract {
                    field: SelectDatetimeFieldBuilder::build(generator),
                    expr: Box::new(TypesBuilder::highlight())
                };

                generator.expect_state("call97_types");
                generator.state_generator.set_compatible_list([
                    SubgraphType::Interval.get_compat_types(),
                    SubgraphType::Timestamp.get_compat_types(),
                ].concat());
                let expr = &mut **unwrap_pat!(number, Expr::Extract { expr, .. }, expr);
                TypesBuilder::build(generator, expr, TypeAssertion::CompatibleWithOneOf(&[SubgraphType::Interval, SubgraphType::Timestamp]));

                SubgraphType::Numeric
            },
        });
        generator.expect_state("EXIT_number");
        number_type
    }
}
