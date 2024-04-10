use sqlparser::ast::{Expr, Ident};

use crate::{query_creation::{query_generator::{value_choosers::QueryValueChooser, QueryGenerator, TypeAssertion}, state_generator::{state_choosers::StateChooser, subgraph_type::SubgraphType, substitute_models::SubstituteModel, CallTypes}}, unwrap_variant};

pub struct TypesBuilder { }

impl TypesBuilder {
    pub fn empty() -> Expr {
        Expr::Identifier(Ident::new("[?]"))
    }

    pub fn build<SubMod: SubstituteModel, StC: StateChooser, QVC: QueryValueChooser>(
        generator: &mut QueryGenerator<SubMod, StC, QVC>, expr: &mut Expr, type_assertion: TypeAssertion
    ) -> SubgraphType {
        generator.expect_state("types");

        let selected_types = unwrap_variant!(generator.state_generator.get_fn_selected_types_unwrapped(), CallTypes::TypeList);
        let selected_type = match generator.next_state().as_str() {
            "types_select_type_bigint" => SubgraphType::BigInt,
            "types_select_type_integer" => SubgraphType::Integer,
            "types_select_type_numeric" => SubgraphType::Numeric,
            "types_select_type_3vl" => SubgraphType::Val3,
            "types_select_type_text" => SubgraphType::Text,
            "types_select_type_date" => SubgraphType::Date,
            "types_select_type_interval" => SubgraphType::Interval,
            "types_select_type_timestamp" => SubgraphType::Timestamp,
            any => generator.panic_unexpected(any),
        };
        let allowed_type_list = SubgraphType::filter_by_selected(&selected_types, selected_type);

        generator.state_generator.set_known_list(allowed_type_list);
        generator.expect_state("call0_types_value");
        let selected_type;
        (selected_type, *expr) = generator.handle_types_value(type_assertion);

        generator.expect_state("EXIT_types");
        selected_type
    }
}
