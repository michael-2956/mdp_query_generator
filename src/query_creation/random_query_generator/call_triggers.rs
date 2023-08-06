use smol_str::SmolStr;

use core::fmt::Debug;

use crate::{query_creation::state_generators::{SubgraphType, CallTypes, markov_chain_generator::FunctionContext}, unwrap_variant, unwrap_variant_ref};

use super::query_info::ClauseContext;

pub trait CallTriggerTrait: Debug {
    fn get_trigger_name(&self) -> SmolStr;

    fn get_new_trigger_state(&self, clause_context: &ClauseContext, function_context: &FunctionContext) -> Box<dyn std::any::Any>;

    fn run(&self, clause_context: &ClauseContext, function_context: &FunctionContext, trigger_state: &Box<dyn std::any::Any>) -> bool;
}

#[derive(Debug)]
pub struct IsColumnTypeAvailableTrigger {}

#[derive(Debug)]
pub struct IsColumnTypeAvailableTriggerState {
    pub selected_types: Vec<SubgraphType>,
}

impl CallTriggerTrait for IsColumnTypeAvailableTrigger {
    fn get_trigger_name(&self) -> SmolStr {
        SmolStr::new("is_column_type_available")
    }

    fn get_new_trigger_state(&self, _clause_context: &ClauseContext, function_context: &FunctionContext) -> Box<dyn std::any::Any> {
        let selected_type = match function_context.current_node.node_common.name.as_str() {
            "types_select_type_3vl" => SubgraphType::Val3,
            "types_select_type_array" => SubgraphType::Array((Box::new(SubgraphType::Undetermined), None)),
            "types_select_type_list_expr" => SubgraphType::ListExpr(Box::new(SubgraphType::Undetermined)),
            "types_select_type_numeric" => SubgraphType::Numeric,
            "types_select_type_string" => SubgraphType::String,
            any => panic!("{any} unexpectedly triggered the is_column_type_available call trigger affector"),
        };
        let allowed_type_list = match selected_type {
            with_inner @ (SubgraphType::Array(..) | SubgraphType::ListExpr(..)) => {
                let argument_selected_types = unwrap_variant!(function_context.call_params.selected_types.clone(), CallTypes::TypeList);
                argument_selected_types
                    .iter()
                    .map(|x| x.to_owned())
                    .filter(|x| std::mem::discriminant(x) == std::mem::discriminant(&with_inner))
                    .collect::<Vec<_>>()
            }
            any => vec![any]
        };

        Box::new(IsColumnTypeAvailableTriggerState {
            selected_types: allowed_type_list
        })
    }

    fn run(&self, clause_context: &ClauseContext, _function_context: &FunctionContext, trigger_state: &Box<dyn std::any::Any>) -> bool {
        trigger_state
            .downcast_ref::<IsColumnTypeAvailableTriggerState>()
            .unwrap()
            .selected_types
            .iter()
            .any(|x|
                clause_context
                    .from()
                    .is_type_available(&x)
            )
    }
}

#[derive(Debug)]
pub struct CanExtendArrayTrigger {}

#[derive(Debug)]
struct CanExtendArrayTriggerState {
    array_length: usize,
}

impl CallTriggerTrait for CanExtendArrayTrigger {
    fn get_trigger_name(&self) -> SmolStr {
        SmolStr::new("can_extend_array")
    }

    fn get_new_trigger_state(&self, _clause_context: &ClauseContext, function_context: &FunctionContext) -> Box<dyn std::any::Any> {
        Box::new(CanExtendArrayTriggerState {
            array_length: match function_context.current_node.node_common.name.as_str() {
                /// TODO: stateful triggers.
                // NOTES:
                // - Only states that can reach end without any stateful trigger affectors can be
                // cached, stateful triggers do not partake as dead_end_infos keys.
                // - For other states DFS will have to be repeatedly ran every time we need to check
                // for dead ends.
                // - However, once we have checked, but only for this stack frame, we can indeed
                // save this information, but only for this stackframe, into local_dead_end_infos.
                // - In stack_frame it will be nice to have a method that will check and update both
                // of those so that the code is more concise.
                "array_multiple_values" => 1,
                "call50_types" => 2,
                any => panic!("{any} unexpectedly triggered the can_extend_array call trigger affector"),
            }
        })
    }

    fn run(&self, _clause_context: &ClauseContext, function_context: &FunctionContext, trigger_state: &Box<dyn std::any::Any>) -> bool {
        let array_length = trigger_state
            .downcast_ref::<CanExtendArrayTriggerState>()
            .unwrap()
            .array_length;

        /// TODO: this part should be in update_trigger_state() which should be affected by state selection
        let desired_element_num = unwrap_variant_ref!(unwrap_variant_ref!(
            function_context.call_params.selected_types, CallTypes::TypeList
        ).iter().next().unwrap(), SubgraphType::Array).1;

        if let Some(desired_element_num) = desired_element_num {
            match function_context.current_node.node_common.name.as_str() {
                "array_one_more_value_is_allowed" => array_length < desired_element_num,
                "array_exit_allowed" => array_length == desired_element_num,
                any => panic!("{any} unexpectedly triggered the can_extend_array call trigger"),
            }
        } else {
            /// TODO: should be just true, but here limits array length to 1.
            match function_context.current_node.node_common.name.as_str() {
                "array_one_more_value_is_allowed" => array_length < 1,
                "array_exit_allowed" => array_length == 1,
                any => panic!("{any} unexpectedly triggered the can_extend_array call trigger"),
            }
        }
    }
}
