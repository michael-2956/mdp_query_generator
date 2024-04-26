use sqlparser::ast::Expr;

use crate::query_creation::{query_generator::{ast_builders::types_type::TypesTypeBuilder, match_next_state, QueryGenerator, ast_builders::types_value::TypeAssertion}, state_generator::{state_choosers::StateChooser, subgraph_type::SubgraphType}};

use super::types::TypesBuilder;

/// subgraph def_list_expr
pub struct ListExprBuilder { }

impl ListExprBuilder {
    pub fn highlight() -> Vec<Expr> {
        vec![TypesBuilder::highlight()]
    }

    pub fn nothing() -> Vec<Expr> {
        vec![]
    }

    pub fn build<StC: StateChooser>(
        generator: &mut QueryGenerator<StC>, list_expr: &mut Vec<Expr>
    ) -> SubgraphType {
        generator.expect_state("list_expr");
        generator.expect_state("call6_types_type");
        let inner_type = TypesTypeBuilder::build(generator);
        generator.state_generator.set_compatible_list(inner_type.get_compat_types());
        generator.expect_state("call16_types");
        
        let expr = list_expr.last_mut().unwrap();
        TypesBuilder::build(generator, expr, TypeAssertion::CompatibleWith(inner_type.clone()));
        generator.expect_state("list_expr_multiple_values");

        list_expr.push(TypesBuilder::highlight());
        loop {
            match_next_state!(generator, {
                "call49_types" => {
                    let expr = list_expr.last_mut().unwrap();
                    TypesBuilder::build(generator, expr, TypeAssertion::CompatibleWith(inner_type.clone()));
                    list_expr.push(TypesBuilder::highlight());
                },
                "EXIT_list_expr" => {
                    list_expr.pop();
                    break
                },
            })
        }

        SubgraphType::ListExpr(Box::new(inner_type))
    }
}
