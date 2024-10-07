use sqlparser::ast::{Distinct, SelectItem};

use crate::query_creation::{query_generator::{match_next_state, QueryGenerator}, state_generator::state_choosers::StateChooser};

use super::select_item::SelectItemBuilder;

/// subgraph def_SELECT
pub struct SelectBuilder { }

impl SelectBuilder {
    pub fn nothing() -> (Option<Distinct>, Vec<SelectItem>) {
        (None, SelectItemBuilder::nothing())
    }

    pub fn highlight() -> (Option<Distinct>, Vec<SelectItem>) {
        (None, SelectItemBuilder::highlight())
    }

    pub fn build<StC: StateChooser>(
        generator: &mut QueryGenerator<StC>, distinct: &mut Option<Distinct>, projection: &mut Vec<SelectItem>
    ) {
        generator.expect_state("SELECT");
        match_next_state!(generator, {
            "SELECT_DISTINCT" => {
                *distinct = Some(Distinct::Distinct);
                generator.clause_context.query_mut().set_distinct();
                generator.expect_state("call0_SELECT_item");
            },
            "call0_SELECT_item" => { },
        });

        generator.clause_context.query_mut().create_select_type();  // before the first item is called
        *projection = SelectItemBuilder::highlight();
        SelectItemBuilder::build(generator, projection);

        generator.expect_state("EXIT_SELECT");
    }
}
