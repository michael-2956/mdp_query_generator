use sqlparser::ast::Expr;

use crate::query_creation::{query_generator::{match_next_state, value_choosers::QueryValueChooser, QueryGenerator}, state_generator::{state_choosers::StateChooser, subgraph_type::SubgraphType, substitute_models::SubstituteModel}};

use super::types::TypesBuilder;

/// subgraph def_formulas
pub struct FormulasBuilder { }

impl FormulasBuilder {
    pub fn empty() -> Expr {
        TypesBuilder::empty()
    }

    pub fn build<SubMod: SubstituteModel, StC: StateChooser, QVC: QueryValueChooser>(
        generator: &mut QueryGenerator<SubMod, StC, QVC>, expr: &mut Expr
    ) -> SubgraphType {
        generator.expect_state("formulas");
        generator.assert_single_type_argument();
        let (selected_type, types_value) = match_next_state!(generator, {
            "call2_number" |
            "call1_number" |
            "call0_number" => generator.handle_number(),
            "call1_VAL_3" => generator.handle_val_3(),
            "call0_text" => generator.handle_text(),
            "call0_date" => generator.handle_date(),
            "call0_timestamp" => generator.handle_timestamp(),
            "call0_interval" => generator.handle_interval(),
        });
        *expr = types_value;
        generator.expect_state("EXIT_formulas");
        selected_type
    }
}
