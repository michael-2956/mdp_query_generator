use sqlparser::ast::Expr;

use crate::query_creation::{query_generator::{QueryGenerator, TypeAssertion}, state_generator::{state_choosers::StateChooser, subgraph_type::SubgraphType, substitute_models::SubstituteModel}};

use super::types::TypesBuilder;

/// subgraph def_WHERE
pub struct WhereBuilder { }

impl WhereBuilder {
    pub fn highlight() -> Expr {
        TypesBuilder::highlight()
    }

    pub fn build<SubMod: SubstituteModel, StC: StateChooser>(
        generator: &mut QueryGenerator<SubMod, StC>, expr: &mut Expr
    ) -> SubgraphType {
        generator.expect_state("WHERE");
        generator.expect_state("call53_types");
        let selection_type = TypesBuilder::build(generator, expr, TypeAssertion::GeneratedBy(SubgraphType::Val3));
        generator.expect_state("EXIT_WHERE");
        selection_type
    }
}
