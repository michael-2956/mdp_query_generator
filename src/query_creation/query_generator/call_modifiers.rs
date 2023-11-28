use smol_str::SmolStr;
use sqlparser::ast::Ident;

use core::fmt::Debug;

use crate::{query_creation::state_generator::{CallTypes, markov_chain_generator::{FunctionContext, subgraph_type::SubgraphType}}, unwrap_variant};

use super::query_info::ClauseContext;

pub trait NamedValue: Debug {
    fn name() -> SmolStr where Self : Sized;
}

#[derive(Debug, Clone)]
pub enum ValueSetterValue {
    TypesType(TypesTypeValue),
    WildcardRelations(WildcardRelationsValue),
}

pub trait ValueSetter: Debug {
    /// returns value name
    fn get_value_name(&self) -> SmolStr;

    /// get the value in given context
    fn get_value(&self, clause_context: &ClauseContext, function_context: &FunctionContext) -> ValueSetterValue;
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

        ValueSetterValue::TypesType(TypesTypeValue {
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
    fn run(&self, clause_context: &ClauseContext, function_context: &FunctionContext, associated_value: Option<&ValueSetterValue>) -> bool;
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

    fn run(&self, clause_context: &ClauseContext, function_context: &FunctionContext, associated_value: Option<&ValueSetterValue>) -> bool {
        let check_group_by = function_context.call_params.modifiers.contains(&SmolStr::new("having clause mode"));
        unwrap_variant!(
            associated_value.unwrap(), ValueSetterValue::TypesType
        ).selected_types.iter().any(|x|
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

    fn run(&self, clause_context: &ClauseContext, function_context: &FunctionContext, _associated_value: Option<&ValueSetterValue>) -> bool {
        let column_types = unwrap_variant!(
            &function_context.call_params.selected_types, CallTypes::TypeList
        );
        // in any context the column ambiguity is determined by FROM
        clause_context.from().has_unique_columns_for_types(column_types)
    }
}

#[derive(Debug, Clone)]
pub struct WildcardRelationsValue {
    /// relations eligible to be selected by qualified wildcard
    pub wildcard_selectable_relations: Vec<Ident>,
    /// total number of relations
    pub total_relations_num: usize,
}

impl NamedValue for WildcardRelationsValue {
    fn name() -> SmolStr {
        SmolStr::new("wildcard_relations")
    }
}

#[derive(Debug, Clone)]
pub struct WildcardRelationsValueSetter { }

impl ValueSetter for WildcardRelationsValueSetter {
    fn get_value_name(&self) -> SmolStr {
        WildcardRelationsValue::name()
    }

    fn get_value(&self, clause_context: &ClauseContext, function_context: &FunctionContext) -> ValueSetterValue {
        if function_context.current_node.node_common.name != "SELECT_tables_eligible_for_wildcard" {
            panic!("{} can only be set at SELECT_tables_eligible_for_wildcard", WildcardRelationsValue::name())
        }
        let single_column = function_context.call_params.modifiers.contains(&SmolStr::new("single column"));
        let allowed_types = unwrap_variant!(function_context.call_params.selected_types.clone(), CallTypes::TypeList);
        // 1. All types selected by wildcard from table should be in the allowed types
        // 2. if single_column modifier is present, only a single column should be present in every relation
        ValueSetterValue::WildcardRelations(WildcardRelationsValue {
            wildcard_selectable_relations: clause_context.from().relations_iter().filter_map(|(alias, relation)| {
                let column_types = relation.get_column_types();
                if single_column && column_types.len() != 1 {
                    None
                } else {
                    if column_types.iter().all(|col_type| allowed_types.contains(col_type)) {
                        Some(alias.clone())
                    } else {
                        None
                    }
                }
            }).collect(),
            total_relations_num: clause_context.from().relations_num(),
        })
    }
}

#[derive(Debug, Clone)]
pub struct IsWildcardAvailableModifier {}

impl StatelessCallModifier for IsWildcardAvailableModifier {
    fn get_name(&self) -> SmolStr {
        SmolStr::new("is_wildcard_available")
    }

    fn get_associated_value_name(&self) -> Option<SmolStr> {
        Some(WildcardRelationsValue::name())
    }

    fn run(&self, _clause_context: &ClauseContext, function_context: &FunctionContext, associated_value: Option<&ValueSetterValue>) -> bool {
        let wildcard_relations = unwrap_variant!(associated_value.unwrap(), ValueSetterValue::WildcardRelations);
        let single_column = function_context.call_params.modifiers.contains(&SmolStr::new("single column"));
        // 3. if no relations are available at all, the wildcards should be off.
        // 4. if multiple relations are present but single_column is ON, the SELECT_wildcard is OFF
        match function_context.current_node.node_common.name.as_str() {
            "SELECT_wildcard" => {
                if single_column {
                    // there is a single relation and it is eligible to be selected by wildcard
                    wildcard_relations.total_relations_num == 1 && wildcard_relations.wildcard_selectable_relations.len() == 1
                } else {
                    wildcard_relations.total_relations_num > 0
                }
            },
            "SELECT_qualified_wildcard" => {
                wildcard_relations.wildcard_selectable_relations.len() > 0
            },
            any => panic!("is_wildcard_available cannot be called at {any}")
        }
    }
}
