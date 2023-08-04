use smol_str::SmolStr;

use core::fmt::Debug;

use crate::{query_creation::state_generators::{SubgraphType, CallTypes}, unwrap_variant};

use super::query_info::QueryContextManager;

pub trait CallTriggerTrait: Debug {
    fn get_trigger_name(&self) -> SmolStr;

    fn get_trigger_state(&self, query_context_manager: &QueryContextManager) -> Box<dyn std::any::Any>;

    fn get_default_trigger_value(&self) -> bool;

    fn run(&self, query_context_manager: &QueryContextManager, trigger_state: &Box<dyn std::any::Any>) -> bool;
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

    fn get_trigger_state(&self, query_context_manager: &QueryContextManager) -> Box<dyn std::any::Any> {
        let selected_type = match query_context_manager.get_current_node().as_str() {
            "types_select_type_3vl" => SubgraphType::Val3,
            "types_select_type_array" => SubgraphType::Array(Box::new(SubgraphType::Undetermined)),
            "types_select_type_list_expr" => SubgraphType::ListExpr(Box::new(SubgraphType::Undetermined)),
            "types_select_type_numeric" => SubgraphType::Numeric,
            "types_select_type_string" => SubgraphType::String,
            any => panic!("{any} unexpectedly triggered the is_column_type_available call trigger affector"),
        };
        let allowed_type_list = match selected_type {
            with_inner @ (SubgraphType::Array(..) | SubgraphType::ListExpr(..)) => {
                let argument_selected_types = unwrap_variant!(
                    query_context_manager.get_current_fn_call_params().selected_types, CallTypes::TypeList
                );
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

    fn get_default_trigger_value(&self) -> bool {
        true
    }

    fn run(&self, query_context_manager: &QueryContextManager, trigger_state: &Box<dyn std::any::Any>) -> bool {
        trigger_state
            .downcast_ref::<IsColumnTypeAvailableTriggerState>()
            .unwrap()
            .selected_types
            .iter()
            .any(|x|
                query_context_manager
                    .from()
                    .is_type_available(&x)
            )
    }
}
