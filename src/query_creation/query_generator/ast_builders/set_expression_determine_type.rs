use std::collections::VecDeque;

use crate::{query_creation::{query_generator::{ast_builders::types_type::TypesTypeBuilder, match_next_state, QueryGenerator}, state_generator::{state_choosers::StateChooser, subgraph_type::SubgraphType, CallTypes}}, unwrap_variant};

/// subgraph def_set_expression_determine_type
pub struct SetExpressionDetermineTypeBuilder { }

impl SetExpressionDetermineTypeBuilder {
    pub fn build<StC: StateChooser>(generator: &mut QueryGenerator<StC>) -> VecDeque<SubgraphType> {
        generator.expect_states(&["set_expression_determine_type", "call9_types_type"]);
        
        let (
            first_column_list, remaining_columns
        ) = unwrap_variant!(
            generator.state_generator.get_fn_selected_types_unwrapped(), CallTypes::QueryTypes
        ).split_first();
        
        generator.state_generator.set_known_list(first_column_list);
        let subgraph_type = TypesTypeBuilder::build(generator);
        
        let mut column_types = match_next_state!(generator, {
            "set_expression_determine_type_can_finish" => VecDeque::new(),
            "call0_set_expression_determine_type" => {
                generator.state_generator.set_known_query_type_list(remaining_columns);
                SetExpressionDetermineTypeBuilder::build(generator)
            }
        });

        column_types.push_front(subgraph_type);

        generator.expect_state("EXIT_set_expression_determine_type");

        column_types
    }
}
