use sqlparser::ast::Expr;

use crate::query_creation::{query_generator::{QueryGenerator, ast_builders::types_value::TypeAssertion}, state_generator::{state_choosers::StateChooser, subgraph_type::SubgraphType}};

use super::types::TypesBuilder;

/// subgraph def_HAVING
pub struct HavingBuilder { }

impl HavingBuilder {
    pub fn highlight() -> Expr {
        TypesBuilder::highlight()
    }

    pub fn build<StC: StateChooser>(
        generator: &mut QueryGenerator<StC>, having: &mut Expr
    ) {
        generator.expect_state("HAVING");
        generator.expect_state("call45_types");
        TypesBuilder::build(generator, having, TypeAssertion::GeneratedBy(SubgraphType::Val3));
        generator.clause_context.query_mut().set_aggregation_indicated();
        generator.expect_state("EXIT_HAVING");
    }
}
