use smol_str::SmolStr;

use core::fmt::Debug;

use crate::{query_creation::state_generators::{SubgraphType, CallTypes, markov_chain_generator::FunctionContext}, unwrap_variant};

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
                "array_multiple_values" => 1,
                /// TODO: stateful triggers (only states before any stateful triggers and trigger affectors can be cached)
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
        /// TODO: Array can accept its length as an argument, but when passing the inner
        /// type value length is ignored, so is not accessible through function_context.call_params
        /// The solution to this would be to pass the arguments wrapped in outer type, so [RI...] would
        /// become just [R...]. Whether the arguments should be wrapped or not can be specified in
        /// function declaration.
        // let argument_selected_types = unwrap_variant!(function_context.call_params.selected_types.clone(), CallTypes::TypeList);
        match function_context.current_node.node_common.name.as_str() {
            "array_one_more_value_is_allowed" => array_length < 1,
            "array_exit_allowed" => array_length == 1,
            any => panic!("{any} unexpectedly triggered the can_extend_array call trigger"),
        }
    }
}
