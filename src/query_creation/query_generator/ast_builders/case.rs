use sqlparser::ast::Expr;

use crate::{query_creation::{query_generator::{ast_builders::{types_type::TypesTypeBuilder, types_value::TypeAssertion}, match_next_state, QueryGenerator}, state_generator::{state_choosers::StateChooser, subgraph_type::SubgraphType}}, unwrap_pat};

use super::types::TypesBuilder;

/// subgraph def_case
pub struct CaseBuilder { }

impl CaseBuilder {
    pub fn highlight() -> Expr {
        TypesBuilder::highlight()
    }

    pub fn build<StC: StateChooser>(
        generator: &mut QueryGenerator<StC>, expr: &mut Expr
    ) -> SubgraphType {
        generator.expect_state("case");

        *expr = Expr::Case {
            operand: None,
            // the nothing is needed so that the result can be
            // displayed before condition is in place
            conditions: vec![TypesBuilder::nothing()],
            results: vec![TypesBuilder::highlight()],
            else_result: None
        };

        generator.expect_states(&["case_first_result", "call7_types_type"]);
        let out_type = TypesTypeBuilder::build(generator);
        generator.expect_state("call82_types");
        generator.state_generator.set_compatible_list(out_type.get_compat_types());
        let results = unwrap_pat!(expr, Expr::Case { results, .. }, results);
        TypesBuilder::build(generator, results.last_mut().unwrap(), TypeAssertion::CompatibleWith(out_type.clone()));

        *unwrap_pat!(expr, Expr::Case { operand, .. }, operand) = Some(Box::new(TypesBuilder::highlight()));
        *unwrap_pat!(expr, Expr::Case { conditions, .. }, conditions).last_mut().unwrap() = TypesBuilder::highlight();

        match_next_state!(generator, {
            "simple_case" => {
                generator.expect_states(&["simple_case_operand", "call8_types_type"]);
                let operand_type = TypesTypeBuilder::build(generator);

                *unwrap_pat!(expr, Expr::Case { conditions, .. }, conditions).last_mut().unwrap() = TypesBuilder::nothing();
                
                generator.expect_state("call78_types");
                generator.state_generator.set_compatible_list(operand_type.get_compat_types());
                let operand = &mut **unwrap_pat!(expr, Expr::Case { operand, .. }, operand).as_mut().unwrap();
                TypesBuilder::build(generator, operand, TypeAssertion::CompatibleWith(operand_type.clone()));

                loop {
                    generator.expect_states(&["simple_case_condition", "call79_types"]);
                    generator.state_generator.set_compatible_list(operand_type.get_compat_types());

                    let condition = unwrap_pat!(expr, Expr::Case { conditions, .. }, conditions).last_mut().unwrap();
                    *condition = TypesBuilder::highlight();
                    TypesBuilder::build(generator, condition, TypeAssertion::CompatibleWith(operand_type.clone()));

                    unwrap_pat!(expr, Expr::Case { conditions, .. }, conditions).push(TypesBuilder::highlight());
                    unwrap_pat!(expr, Expr::Case { results, .. }, results).push(TypesBuilder::nothing());

                    match_next_state!(generator, {
                        "simple_case_result" => {
                            *unwrap_pat!(expr, Expr::Case { conditions, .. }, conditions).last_mut().unwrap() = TypesBuilder::nothing();
                            generator.expect_state("call80_types");
                            generator.state_generator.set_compatible_list(out_type.get_compat_types());
                            let result = unwrap_pat!(expr, Expr::Case { results, .. }, results).last_mut().unwrap();
                            *result = TypesBuilder::highlight();
                            TypesBuilder::build(generator, result, TypeAssertion::CompatibleWith(out_type.clone()));
                        },
                        "case_else" => {
                            unwrap_pat!(expr, Expr::Case { conditions, .. }, conditions).pop();
                            unwrap_pat!(expr, Expr::Case { results, .. }, results).pop();
                            break
                        },
                    })
                }
            },
            "searched_case" => {
                loop {
                    *unwrap_pat!(expr, Expr::Case { operand, .. }, operand) = None;
                    generator.expect_states(&["searched_case_condition", "call76_types"]);

                    let condition = unwrap_pat!(expr, Expr::Case { conditions, .. }, conditions).last_mut().unwrap();
                    TypesBuilder::build(generator, condition, TypeAssertion::GeneratedBy(SubgraphType::Val3));

                    unwrap_pat!(expr, Expr::Case { conditions, .. }, conditions).push(TypesBuilder::highlight());
                    unwrap_pat!(expr, Expr::Case { results, .. }, results).push(TypesBuilder::nothing());

                    match_next_state!(generator, {
                        "searched_case_result" => {
                            *unwrap_pat!(expr, Expr::Case { conditions, .. }, conditions).last_mut().unwrap() = TypesBuilder::nothing();
                            generator.expect_state("call77_types");
                            generator.state_generator.set_compatible_list(out_type.get_compat_types());
                            let result = unwrap_pat!(expr, Expr::Case { results, .. }, results).last_mut().unwrap();
                            *result = TypesBuilder::highlight();
                            TypesBuilder::build(generator, result, TypeAssertion::CompatibleWith(out_type.clone()));
                        },
                        "case_else" => {
                            unwrap_pat!(expr, Expr::Case { conditions, .. }, conditions).pop();
                            unwrap_pat!(expr, Expr::Case { results, .. }, results).pop();
                            break
                        },
                    })
                }
            },
        });

        let else_result = unwrap_pat!(expr, Expr::Case { else_result, .. }, else_result);
        *else_result = Some(Box::new(TypesBuilder::highlight()));
        match_next_state!(generator, {
            "call81_types" => {
                generator.state_generator.set_compatible_list(out_type.get_compat_types());
                TypesBuilder::build(generator, &mut **else_result.as_mut().unwrap(), TypeAssertion::CompatibleWith(out_type.clone()));
                generator.expect_state("EXIT_case");
            },
            "EXIT_case" => {
                *else_result = None;
            },
        });

        out_type
    }
}
