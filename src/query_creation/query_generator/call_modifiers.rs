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
    IsGroupingSets(IsGroupingSetsValue),
    GroupingEnabled(GroupingEnabledValue),
    WildcardRelations(WildcardRelationsValue),
    DistinctAggregation(DistinctAggregationValue),
    HasAccessibleColumns(HasAccessibleColumnsValue),
    HasUniqueColumnNamesForType(HasUniqueColumnNamesForSelectedTypesValue),
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
    pub type_is_available_in_clause: bool,
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

    fn get_value(&self, clause_context: &ClauseContext, function_context: &FunctionContext) -> ValueSetterValue {
        let selected_type = match function_context.current_node.node_common.name.as_str() {
            "types_select_type_integer" => SubgraphType::Integer,
            "types_select_type_numeric" => SubgraphType::Numeric,
            "types_select_type_3vl" => SubgraphType::Val3,
            "types_select_type_text" => SubgraphType::Text,
            "types_select_type_date" => SubgraphType::Date,
            any => panic!("{any} unexpectedly triggered the types_type value setter"),
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

        let check_group_by = function_context.call_params.modifiers.contains(&SmolStr::new("group by columns"));

        ValueSetterValue::TypesType(TypesTypeValue {
            type_is_available_in_clause: allowed_type_list.iter().any(|x|
                if check_group_by {
                    clause_context.group_by().is_type_available(x)
                } else {
                    clause_context.from().is_type_available(x, None)
                }
            ),
            selected_types: allowed_type_list,
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
    fn run(&self, function_context: &FunctionContext, associated_value: Option<&ValueSetterValue>) -> bool;
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

    fn run(&self, _function_context: &FunctionContext, associated_value: Option<&ValueSetterValue>) -> bool {
        unwrap_variant!(associated_value.unwrap(), ValueSetterValue::TypesType).type_is_available_in_clause
    }
}

#[derive(Debug, Clone)]
pub struct HasUniqueColumnNamesForSelectedTypesValue {
    value: bool,
}

impl NamedValue for HasUniqueColumnNamesForSelectedTypesValue {
    fn name() -> SmolStr {
        SmolStr::new("do_unique_column_names_exist_for_selected_types")
    }
}

#[derive(Debug, Clone)]
pub struct HasUniqueColumnNamesForTypeValueSetter { }

impl ValueSetter for HasUniqueColumnNamesForTypeValueSetter {
    fn get_value_name(&self) -> SmolStr {
        HasUniqueColumnNamesForSelectedTypesValue::name()
    }

    fn get_value(&self, clause_context: &ClauseContext, function_context: &FunctionContext) -> ValueSetterValue {
        let column_types = unwrap_variant!(
            &function_context.call_params.selected_types, CallTypes::TypeList
        );
        // in any clause context the column ambiguity is determined by FROM
        ValueSetterValue::HasUniqueColumnNamesForType(HasUniqueColumnNamesForSelectedTypesValue {
            value: clause_context.from().has_unique_columns_for_types(column_types)
        })
    }
}

#[derive(Debug, Clone)]
pub struct HasUniqueColumnNamesForSelectedTypesModifier {}

impl StatelessCallModifier for HasUniqueColumnNamesForSelectedTypesModifier {
    fn get_name(&self) -> SmolStr {
        SmolStr::new("has_unique_column_names_for_selected_types")
    }

    fn get_associated_value_name(&self) -> Option<SmolStr> {
        Some(HasUniqueColumnNamesForSelectedTypesValue::name())
    }

    fn run(&self, _function_context: &FunctionContext, associated_value: Option<&ValueSetterValue>) -> bool {
        unwrap_variant!(associated_value.unwrap(), ValueSetterValue::HasUniqueColumnNamesForType).value
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

    fn run(&self, function_context: &FunctionContext, associated_value: Option<&ValueSetterValue>) -> bool {
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

#[derive(Debug, Clone)]
pub struct FromHasAccessibleColumnsModifier {}

impl StatelessCallModifier for FromHasAccessibleColumnsModifier {
    fn get_name(&self) -> SmolStr {
        SmolStr::new("from_has_accessible_columns")
    }

    fn get_associated_value_name(&self) -> Option<SmolStr> {
        None
    }

    fn run(&self, _function_context: &FunctionContext, _associated_value: Option<&ValueSetterValue>) -> bool {
        true
    }

    // fn run(&self, clause_context: &ClauseContext, _function_context: &FunctionContext, _associated_value: Option<&ValueSetterValue>) -> bool {
    //     clause_context.from().relations_iter().any(|(_, relation)| relation.has_accessible_columns())
    // }
}

#[derive(Debug, Clone)]
pub struct GroupingEnabledValue {
    /// grouping is enabled in this query
    pub enabled: bool,
}

impl NamedValue for GroupingEnabledValue {
    fn name() -> SmolStr {
        SmolStr::new("grouping_enabled")
    }
}

#[derive(Debug, Clone)]
pub struct GroupingEnabledValueSetter { }

impl ValueSetter for GroupingEnabledValueSetter {
    fn get_value_name(&self) -> SmolStr {
        GroupingEnabledValue::name()
    }

    fn get_value(&self, clause_context: &ClauseContext, _function_context: &FunctionContext) -> ValueSetterValue {
        ValueSetterValue::GroupingEnabled(GroupingEnabledValue {
            enabled: clause_context.group_by().is_grouping_active(),
        })
    }
}

#[derive(Debug, Clone)]
pub struct GroupingModeSwitchModifier {}

impl StatelessCallModifier for GroupingModeSwitchModifier {
    fn get_name(&self) -> SmolStr {
        SmolStr::new("grouping mode switch")
    }

    fn get_associated_value_name(&self) -> Option<SmolStr> {
        Some(GroupingEnabledValue::name())
    }

    fn run(&self, function_context: &FunctionContext, associated_value: Option<&ValueSetterValue>) -> bool {
        let grouping_enabled = unwrap_variant!(associated_value.unwrap(), ValueSetterValue::GroupingEnabled).enabled;
        match function_context.current_node.node_common.name.as_str() {
            "call73_types" => grouping_enabled,
            "call54_types" => !grouping_enabled,
            any => panic!("grouping mode switch cannot be called at {any}")
        }
    }
}

#[derive(Debug, Clone)]
pub struct IsGroupingSetsValue {
    pub value: bool,
}

impl NamedValue for IsGroupingSetsValue {
    fn name() -> SmolStr {
        SmolStr::new("is_grouping_sets")
    }
}

#[derive(Debug, Clone)]
pub struct IsGroupingSetsValueSetter { }

impl ValueSetter for IsGroupingSetsValueSetter {
    fn get_value_name(&self) -> SmolStr {
        IsGroupingSetsValue::name()
    }

    fn get_value(&self, _clause_context: &ClauseContext, function_context: &FunctionContext) -> ValueSetterValue {
        ValueSetterValue::IsGroupingSets(IsGroupingSetsValue {
            value: match function_context.current_node.node_common.name.as_str() {
                "grouping_rollup" => false,
                "grouping_cube" => false,
                "grouping_set" => true,
                any => panic!("is_grouping_sets value cannot be evalueted at {any}")
            },
        })
    }
}

#[derive(Debug, Clone)]
pub struct IsEmptySetAllowedModifier {}

impl StatelessCallModifier for IsEmptySetAllowedModifier {
    fn get_name(&self) -> SmolStr {
        SmolStr::new("empty set allowed")
    }

    fn get_associated_value_name(&self) -> Option<SmolStr> {
        Some(IsGroupingSetsValue::name())
    }

    fn run(&self, function_context: &FunctionContext, associated_value: Option<&ValueSetterValue>) -> bool {
        let is_grouping_sets = unwrap_variant!(associated_value.unwrap(), ValueSetterValue::IsGroupingSets).value;
        match function_context.current_node.node_common.name.as_str() {
            "set_list_empty_allowed" => is_grouping_sets,
            any => panic!("'empty set allowed' cannot be called at {any}")
        }
    }
}

#[derive(Debug, Clone)]
pub struct HasAccessibleColumnsValue {
    /// accessible columns are present in FROM
    pub available: bool,
}

impl NamedValue for HasAccessibleColumnsValue {
    fn name() -> SmolStr {
        SmolStr::new("has_accessible_cols")
    }
}

#[derive(Debug, Clone)]
pub struct HasAccessibleColumnsValueSetter { }

impl ValueSetter for HasAccessibleColumnsValueSetter {
    fn get_value_name(&self) -> SmolStr {
        HasAccessibleColumnsValue::name()
    }

    fn get_value(&self, clause_context: &ClauseContext, _function_context: &FunctionContext) -> ValueSetterValue {
        ValueSetterValue::HasAccessibleColumns(HasAccessibleColumnsValue {
            available: clause_context.from().has_unique_columns_for_types(&vec![SubgraphType::Undetermined]),
        })
    }
}

#[derive(Debug, Clone)]
pub struct HasAccessibleColumnsModifier {}

impl StatelessCallModifier for HasAccessibleColumnsModifier {
    fn get_name(&self) -> SmolStr {
        SmolStr::new("has_accessible_cols_mod")
    }

    fn get_associated_value_name(&self) -> Option<SmolStr> {
        Some(HasAccessibleColumnsValue::name())
    }

    fn run(&self, _function_context: &FunctionContext, associated_value: Option<&ValueSetterValue>) -> bool {
        unwrap_variant!(associated_value.unwrap(), ValueSetterValue::HasAccessibleColumns).available
    }
}

#[derive(Debug, Clone)]
pub struct DistinctAggregationValue {
    /// aggreagtion is DISTINCT
    pub is_distinct: bool,
}

impl NamedValue for DistinctAggregationValue {
    fn name() -> SmolStr {
        SmolStr::new("distinct_aggr")
    }
}

#[derive(Debug, Clone)]
pub struct DistinctAggregationValueSetter { }

impl ValueSetter for DistinctAggregationValueSetter {
    fn get_value_name(&self) -> SmolStr {
        DistinctAggregationValue::name()
    }

    fn get_value(&self, _clause_context: &ClauseContext, function_context: &FunctionContext) -> ValueSetterValue {
        ValueSetterValue::DistinctAggregation(DistinctAggregationValue {
            is_distinct: match function_context.current_node.node_common.name.as_str() {
                "aggregate_not_distinct" => false,
                "aggregate_distinct" => true,
                any => panic!("{any} unexpectedly triggered the distinct_aggr value setter"),
            },
        })
    }
}

#[derive(Debug, Clone)]
pub struct DistinctAggregationModifier {}

impl StatelessCallModifier for DistinctAggregationModifier {
    fn get_name(&self) -> SmolStr {
        SmolStr::new("distinct_aggr_mod")
    }

    fn get_associated_value_name(&self) -> Option<SmolStr> {
        Some(DistinctAggregationValue::name())
    }

    fn run(&self, _function_context: &FunctionContext, associated_value: Option<&ValueSetterValue>) -> bool {
        !unwrap_variant!(associated_value.unwrap(), ValueSetterValue::DistinctAggregation).is_distinct
    }
}
