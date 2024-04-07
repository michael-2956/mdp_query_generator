use sqlparser::ast::{Expr, Ident};

use crate::query_creation::{query_generator::{value_choosers::QueryValueChooser, QueryGenerator, TypeAssertion}, state_generator::{state_choosers::StateChooser, subgraph_type::SubgraphType, substitute_models::SubstituteModel}};

pub struct TypesBuilder { }

impl TypesBuilder {
    pub fn empty() -> Expr {
        Expr::Identifier(Ident::new("[?]"))
    }

    pub fn build<SubMod: SubstituteModel, StC: StateChooser, QVC: QueryValueChooser>(
        generator: &mut QueryGenerator<SubMod, StC, QVC>, expr: &mut Expr, type_assertion: TypeAssertion
    ) -> SubgraphType {
        todo!()
    }
}
