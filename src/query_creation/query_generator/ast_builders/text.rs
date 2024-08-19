use sqlparser::ast::{BinaryOperator, Expr, TrimWhereField};

use crate::{query_creation::{query_generator::{match_next_state, QueryGenerator, ast_builders::types_value::TypeAssertion}, state_generator::{state_choosers::StateChooser, subgraph_type::SubgraphType}}, unwrap_pat};

use super::types::TypesBuilder;

/// subgraph def_text
pub struct TextBuilder { }

impl TextBuilder {
    pub fn highlight() -> Expr {
        TypesBuilder::highlight()
    }

    pub fn build<StC: StateChooser>(
        generator: &mut QueryGenerator<StC>, text: &mut Expr
    ) -> SubgraphType {
        generator.expect_state("text");
        match_next_state!(generator, {
            "text_trim" => {
                *text = Expr::Trim {
                    expr: Box::new(TypesBuilder::highlight()), trim_where: None, trim_what: None, trim_characters: None
                };

                generator.expect_state("call6_types");
                let expr = &mut **unwrap_pat!(text, Expr::Trim { expr, .. }, expr);
                TypesBuilder::build(generator, expr, TypeAssertion::GeneratedBy(SubgraphType::Text));

                match_next_state!(generator, {
                    trim_where_state @ ("BOTH" | "LEADING" | "TRAILING") => {
                        let trim_where = &mut *unwrap_pat!(text, Expr::Trim { trim_where, .. }, trim_where);
                        *trim_where = Some(match trim_where_state {
                            "BOTH" => TrimWhereField::Both,
                            "LEADING" => TrimWhereField::Leading,
                            "TRAILING" => TrimWhereField::Trailing,
                            any => generator.panic_unexpected(any),
                        });

                        generator.expect_state("call5_types");
                        let trim_what = &mut *unwrap_pat!(text, Expr::Trim { trim_what, .. }, trim_what);
                        *trim_what = Some(Box::new(TypesBuilder::highlight()));
                        let trim_what = &mut **trim_what.as_mut().unwrap();
                        TypesBuilder::build(generator, trim_what, TypeAssertion::GeneratedBy(SubgraphType::Text));
                        
                        generator.expect_state("text_trim_done");
                    },
                    "text_trim_done" => { },
                });
            },
            "text_concat" => {
                *text = Expr::BinaryOp {
                    left: Box::new(TypesBuilder::highlight()),
                    op: BinaryOperator::StringConcat,
                    right: Box::new(TypesBuilder::nothing())
                };

                generator.expect_state("call7_types");
                let left = &mut **unwrap_pat!(text, Expr::BinaryOp { left, .. }, left);
                TypesBuilder::build(generator, left, TypeAssertion::GeneratedBy(SubgraphType::Text));

                generator.expect_state("text_concat_concat");
                generator.expect_state("call8_types");
                let right = &mut **unwrap_pat!(text, Expr::BinaryOp { right, .. }, right);
                *right = TypesBuilder::highlight();
                TypesBuilder::build(generator, right, TypeAssertion::GeneratedBy(SubgraphType::Text));
            },
            "text_substring" => {
                *text = Expr::Substring {
                    expr: Box::new(TypesBuilder::highlight()),
                    substring_from: None,
                    substring_for: None,
                    special: false,
                };

                generator.expect_state("call9_types");
                let expr = &mut **unwrap_pat!(text, Expr::Substring { expr, .. }, expr);
                TypesBuilder::build(generator, expr, TypeAssertion::GeneratedBy(SubgraphType::Text));

                loop {
                    match_next_state!(generator, {
                        "text_substring_from" => {
                            generator.expect_state("call10_types");
                            let substring_from = &mut *unwrap_pat!(text, Expr::Substring { substring_from, .. }, substring_from);
                            *substring_from = Some(Box::new(TypesBuilder::highlight()));
                            let substring_from = &mut *substring_from.as_mut().unwrap();
                            TypesBuilder::build(generator, substring_from, TypeAssertion::GeneratedBy(SubgraphType::Integer));
                        },
                        "text_substring_for" => {
                            generator.expect_state("call11_types");
                            let substring_for = &mut *unwrap_pat!(text, Expr::Substring { substring_for, .. }, substring_for);
                            *substring_for = Some(Box::new(TypesBuilder::highlight()));
                            let substring_for = &mut *substring_for.as_mut().unwrap();
                            TypesBuilder::build(generator, substring_for, TypeAssertion::GeneratedBy(SubgraphType::Integer));
                        },
                        "text_substring_end" => break,
                    })
                }
            },
        });
        generator.expect_state("EXIT_text");
        SubgraphType::Text
    }
}
