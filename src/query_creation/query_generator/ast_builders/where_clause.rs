use sqlparser::ast::Expr;

use crate::query_creation::{query_generator::{ast_builders::types_value::TypeAssertion, QueryGenerationResult, QueryGenerator}, state_generator::{state_choosers::StateChooser, subgraph_type::SubgraphType}};

use super::types::TypesBuilder;

/// subgraph def_WHERE
pub struct WhereBuilder { }

impl WhereBuilder {
    pub fn highlight() -> Expr {
        TypesBuilder::highlight()
    }

    pub fn build<StC: StateChooser + Send + Sync>(
        generator: &mut QueryGenerator<StC>, expr: &mut Expr
    ) -> QueryGenerationResult<SubgraphType> {
        generator.expect_state("WHERE")?;
        generator.expect_state("call53_types")?;
        let selection_type = TypesBuilder::build(generator, expr, TypeAssertion::GeneratedBy(SubgraphType::Val3))?;
        generator.expect_state("EXIT_WHERE")?;
        Ok(selection_type)
    }
}
