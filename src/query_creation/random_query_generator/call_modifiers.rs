use smol_str::SmolStr;

use core::fmt::Debug;

use crate::{query_creation::state_generators::{CallTypes, markov_chain_generator::{FunctionContext, subgraph_type::SubgraphType}}, unwrap_variant};

use super::query_info::ClauseContext;

pub trait NamedValue: Debug {
    fn name() -> SmolStr where Self : Sized;
}

#[derive(Debug, Clone)]
pub struct TypesTypeValue {
    pub selected_types: Vec<SubgraphType>,
}

impl NamedValue for TypesTypeValue {
    fn name() -> SmolStr {
        SmolStr::new("types_type")
    }
}

#[derive(Debug, Clone)]
pub enum ValueSetterValue {
    TypesTypeValue(TypesTypeValue),
}

pub trait ValueSetter: Debug {
    /// returns value name
    fn get_value_name(&self) -> SmolStr;

    /// get the value in given context
    fn get_value(&self, clause_context: &ClauseContext, function_context: &FunctionContext) -> ValueSetterValue;
}

#[derive(Debug, Clone)]
pub struct TypesTypeValueSetter { }

impl ValueSetter for TypesTypeValueSetter {
    fn get_value_name(&self) -> SmolStr {
        TypesTypeValue::name()
    }

    fn get_value(&self, _clause_context: &ClauseContext, function_context: &FunctionContext) -> ValueSetterValue {
        let selected_type = match function_context.current_node.node_common.name.as_str() {
            "types_select_type_integer" => SubgraphType::Integer,
            "types_select_type_numeric" => SubgraphType::Numeric,
            "types_select_type_3vl" => SubgraphType::Val3,
            "types_select_type_text" => SubgraphType::Text,
            "types_select_type_date" => SubgraphType::Date,
            any => panic!("{any} unexpectedly triggered the is_column_type_available call modifier affector"),
        };
        let allowed_type_list = match selected_type {
            with_inner @ SubgraphType::ListExpr(..) => {
                let argument_selected_types = unwrap_variant!(function_context.call_params.selected_types.clone(), CallTypes::TypeList);
                argument_selected_types
                    .iter()
                    .map(|x| x.to_owned())
                    .filter(|x| std::mem::discriminant(x) == std::mem::discriminant(&with_inner))
                    .collect::<Vec<_>>()
            }
            any => vec![any]
        };

        ValueSetterValue::TypesTypeValue(TypesTypeValue {
            selected_types: allowed_type_list
        })
    }
}

/// Call modifier that relies on external (static in function scope) values
pub trait StatelessCallModifier: Debug {
    /// returns call modifier name
    fn get_name(&self) -> SmolStr;

    /// returns associated value, which sets the state of this modifier
    fn get_associated_value_name(&self) -> Option<SmolStr>;

    /// Runs the modifier value based on the current value
    fn run(&self, clause_context: &ClauseContext, function_context: &FunctionContext, associated_value: &ValueSetterValue) -> bool;
}

pub trait StatefulCallModifier: Debug {
    fn new() -> Box<dyn StatefulCallModifier> where Self : Sized;

    fn dyn_box_clone(&self) -> Box<dyn StatefulCallModifier>;

    fn get_name(&self) -> SmolStr;

    fn update_state(&mut self, clause_context: &ClauseContext, function_context: &FunctionContext);

    fn run(&self, clause_context: &ClauseContext, function_context: &FunctionContext) -> bool;
}

#[derive(Debug, Clone)]
pub struct IsColumnTypeAvailableModifier {}

impl StatelessCallModifier for IsColumnTypeAvailableModifier {
    fn get_name(&self) -> SmolStr {
        SmolStr::new("is_column_type_available")
    }

    fn get_associated_value_name(&self) -> Option<SmolStr> {
        Some(TypesTypeValue::name())
    }

    fn run(&self, clause_context: &ClauseContext, function_context: &FunctionContext, associated_value: &ValueSetterValue) -> bool {
        let check_group_by = match function_context.current_node.node_common.name.as_str() {
            "call0_column_spec" => false,
            "call1_column_spec" => true,
            any => panic!("is_column_type_available call trigger unexpectedly called by {any}"),
        };
        let ValueSetterValue::TypesTypeValue(associated_value) = associated_value;
        associated_value.selected_types.iter()
            .any(|x|
                if check_group_by {
                    clause_context.group_by().is_type_available(x)
                } else {
                    clause_context.from().is_type_available(x, None)
                }
            )
    }
}

#[derive(Debug, Clone)]
pub struct HasUniqueColumnNamesForTypeModifier {}

impl StatelessCallModifier for HasUniqueColumnNamesForTypeModifier {
    fn get_name(&self) -> SmolStr {
        SmolStr::new("has_unique_column_names_for_type")
    }

    fn get_associated_value_name(&self) -> Option<SmolStr> {
        None
    }

    fn run(&self, clause_context: &ClauseContext, function_context: &FunctionContext, _associated_value: &ValueSetterValue) -> bool {
        let column_types = unwrap_variant!(
            &function_context.call_params.selected_types, CallTypes::TypeList
        );
        clause_context.from().has_unique_columns_for_types(column_types)
    }
}
