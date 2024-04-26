use sqlparser::ast::{Expr, Ident};

use crate::{query_creation::{query_generator::{match_next_state, QueryGenerator, ast_builders::types_value::TypeAssertion}, state_generator::{state_choosers::StateChooser, subgraph_type::SubgraphType, CallTypes}}, unwrap_variant};

use super::types_value::TypesValueBuilder;

/// subgraph def_types
pub struct TypesBuilder { }

impl TypesBuilder {
    pub fn highlight() -> Expr {
        Expr::Identifier(Ident::new("[?]"))
    }

    pub fn nothing() -> Expr {
        Expr::Identifier(Ident::new(""))
    }

    pub fn build<StC: StateChooser>(
        generator: &mut QueryGenerator<StC>, expr: &mut Expr, type_assertion: TypeAssertion
    ) -> SubgraphType {
        generator.expect_state("types");

        let selected_types = unwrap_variant!(generator.state_generator.get_fn_selected_types_unwrapped(), CallTypes::TypeList);
        let selected_type = match_next_state!(generator, {
            "types_select_type_bigint" => SubgraphType::BigInt,
            "types_select_type_integer" => SubgraphType::Integer,
            "types_select_type_numeric" => SubgraphType::Numeric,
            "types_select_type_3vl" => SubgraphType::Val3,
            "types_select_type_text" => SubgraphType::Text,
            "types_select_type_date" => SubgraphType::Date,
            "types_select_type_interval" => SubgraphType::Interval,
            "types_select_type_timestamp" => SubgraphType::Timestamp,
        });
        let allowed_type_list = SubgraphType::filter_by_selected(&selected_types, selected_type);

        generator.state_generator.set_known_list(allowed_type_list);
        generator.expect_state("call0_types_value");
        *expr = TypesValueBuilder::highlight();
        let selected_type = TypesValueBuilder::build(generator, expr, type_assertion);

        generator.expect_state("EXIT_types");
        selected_type
    }
}
