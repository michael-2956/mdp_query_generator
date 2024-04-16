use sqlparser::ast::Expr;

use crate::query_creation::{query_generator::{QueryGenerator, TypeAssertion}, state_generator::{state_choosers::StateChooser, subgraph_type::SubgraphType, substitute_models::SubstituteModel}};

use super::types::TypesBuilder;

/// subgraph def_HAVING
pub struct HavingBuilder { }

impl HavingBuilder {
    pub fn highlight() -> Expr {
        TypesBuilder::highlight()
    }

    pub fn build<SubMod: SubstituteModel, StC: StateChooser>(
        generator: &mut QueryGenerator<SubMod, StC>, having: &mut Expr
    ) {
        generator.expect_state("HAVING");
        generator.expect_state("call45_types");
        TypesBuilder::build(generator, having, TypeAssertion::GeneratedBy(SubgraphType::Val3));
        generator.clause_context.query_mut().set_aggregation_indicated();
        generator.expect_state("EXIT_HAVING");
    }
}
