use sqlparser::ast::Expr;

use crate::query_creation::{query_generator::{match_next_state, QueryGenerator}, state_generator::{state_choosers::StateChooser, subgraph_type::SubgraphType}};

use super::{date::DateBuilder, interval::IntervalBuilder, number::NumberBuilder, text::TextBuilder, timestamp::TimestampBuilder, types::TypesBuilder, val_3::Val3Builder};

/// subgraph def_formulas
pub struct FormulasBuilder { }

impl FormulasBuilder {
    pub fn highlight() -> Expr {
        TypesBuilder::highlight()
    }

    pub fn build<StC: StateChooser>(
        generator: &mut QueryGenerator<StC>, expr: &mut Expr
    ) -> SubgraphType {
        generator.expect_state("formulas");
        generator.assert_single_type_argument();
        let selected_type = match_next_state!(generator, {
            "call2_number" |
            "call1_number" |
            "call0_number" => NumberBuilder::build(generator, expr),
            "call1_VAL_3" => Val3Builder::build(generator, expr),
            "call0_text" => TextBuilder::build(generator, expr),
            "call0_date" => DateBuilder::build(generator, expr),
            "call0_timestamp" => TimestampBuilder::build(generator, expr),
            "call0_interval" => IntervalBuilder::build(generator, expr),
        });
        generator.expect_state("EXIT_formulas");
        selected_type
    }
}
