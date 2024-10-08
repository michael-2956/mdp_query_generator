use smol_str::SmolStr;
use sqlparser::ast::ObjectName;

use core::fmt::Debug;
use std::collections::BTreeMap;

use crate::{query_creation::state_generator::{markov_chain_generator::{markov_chain::QueryTypes, subgraph_type::SubgraphType, FunctionContext}, CallTypes}, unwrap_variant};

use super::query_info::{CheckAccessibility, ClauseContext, ColumnRetrievalOptions, IdentName};

pub trait NamedValue: Debug {
    fn name() -> SmolStr where Self : Sized;
}

#[derive(Debug, Clone)]
pub enum ValueSetterValue {
    CanSkipLimit(CanSkipLimitValue),
    IsGroupingSets(IsGroupingSetsValue),
    GroupingEnabled(GroupingEnabledValue),
    CanAddMoreColumns(CanAddMoreColumnsValue),
    WildcardRelations(WildcardRelationsValue),
    AvailableTableNames(AvailableTableNamesValue),
    DistinctAggregation(DistinctAggregationValue),
    SelectIsNotDistinct(SelectIsNotDistinctValue),
    HasAccessibleColumns(HasAccessibleColumnsValue),
    IsColumnTypeAvailable(IsColumnTypeAvailableValue),
    QueryTypeNotExhausted(QueryTypeNotExhaustedValue),
    SelectAccessibleColumns(SelectAccessibleColumnsValue),
    NameAccessibilityOfSelectedTypes(NameAccessibilityOfSelectedTypesValue),
}

pub trait ValueSetter: Debug {
    /// returns value name
    fn get_value_name(&self) -> SmolStr;

    /// get the value in given context
    fn get_value(&self, clause_context: &ClauseContext, function_context: &FunctionContext) -> ValueSetterValue;
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
pub struct IsColumnTypeAvailableValue {
    /// ccolumn type is available
    pub is_available: bool,
    /// can let through in types
    pub let_through: bool,
}

impl NamedValue for IsColumnTypeAvailableValue {
    fn name() -> SmolStr {
        SmolStr::new("is_column_type_available_val")
    }
}

#[derive(Debug, Clone)]
pub struct IsColumnTypeAvailableValueSetter { }

impl ValueSetter for IsColumnTypeAvailableValueSetter {
    fn get_value_name(&self) -> SmolStr {
        IsColumnTypeAvailableValue::name()
    }

    fn get_value(&self, clause_context: &ClauseContext, function_context: &FunctionContext) -> ValueSetterValue {
        let mut argument_types = unwrap_variant!(function_context.call_params.selected_types.clone(), CallTypes::TypeList);

        let selected_type_opt = match function_context.current_node.node_common.name.as_str() {
            "column_type_available" => None,
            "types_select_type_bigint" => Some(SubgraphType::BigInt),
            "types_select_type_integer" => Some(SubgraphType::Integer),
            "types_select_type_numeric" => Some(SubgraphType::Numeric),
            "types_select_type_3vl" => Some(SubgraphType::Val3),
            "types_select_type_text" => Some(SubgraphType::Text),
            "types_select_type_date" => Some(SubgraphType::Date),
            "types_select_type_interval" => Some(SubgraphType::Interval),
            "types_select_type_timestamp" => Some(SubgraphType::Timestamp),
            any => panic!("{any} unexpectedly triggered the is_column_type_available_val value setter"),
        };

        if let Some(selected_type) = selected_type_opt {
            argument_types = SubgraphType::filter_by_selected(&argument_types, selected_type);
        }

        let type_is_available = clause_context.is_type_available(
            argument_types, ColumnRetrievalOptions::from_call_mods(&function_context.call_params.modifiers)
        );
        let types_value_has_other_options = [
            "no typed nulls", "no subquery", "no literals",
            "no case", "no formulas",
        ].into_iter().any(
            |m| !function_context.call_params.modifiers.contains(&SmolStr::new(m))
        );
        return ValueSetterValue::IsColumnTypeAvailable(IsColumnTypeAvailableValue {
            is_available: type_is_available,
            let_through: type_is_available || types_value_has_other_options
        })
    }
}

#[derive(Debug, Clone)]
pub struct IsColumnTypeAvailableModifier {}

impl StatelessCallModifier for IsColumnTypeAvailableModifier {
    fn get_name(&self) -> SmolStr {
        SmolStr::new("is_column_type_available_gate")
    }

    fn get_associated_value_name(&self) -> Option<SmolStr> {
        Some(IsColumnTypeAvailableValue::name())
    }

    fn run(&self, function_context: &FunctionContext, associated_value: Option<&ValueSetterValue>) -> bool {
        let associated_value = unwrap_variant!(associated_value.unwrap(), ValueSetterValue::IsColumnTypeAvailable);
        match function_context.current_node.node_common.name.as_str() {
            "call0_types_value" => associated_value.let_through,
            "call0_column_spec" => associated_value.is_available,
            any => panic!("{any} unexpectedly triggered the is_column_type_available modifier"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct NameAccessibilityOfSelectedTypesValue {
    accessible_by_column_name: bool,
    accessible_by_qualified_column_name: bool,
}

impl NamedValue for NameAccessibilityOfSelectedTypesValue {
    fn name() -> SmolStr {
        SmolStr::new("name_accessibility_of_selected_types_value")
    }
}

#[derive(Debug, Clone)]
pub struct NameAccessibilityOfSelectedTypesValueSetter { }

impl ValueSetter for NameAccessibilityOfSelectedTypesValueSetter {
    fn get_value_name(&self) -> SmolStr {
        NameAccessibilityOfSelectedTypesValue::name()
    }

    fn get_value(&self, clause_context: &ClauseContext, function_context: &FunctionContext) -> ValueSetterValue {
        let column_types = unwrap_variant!(
            &function_context.call_params.selected_types, CallTypes::TypeList
        );
        let column_retrieval_options = ColumnRetrievalOptions::from_call_mods(&function_context.call_params.modifiers);
        let accessible_by_column_name = clause_context.has_columns_for_types(
            column_types.clone(), CheckAccessibility::ColumnName, column_retrieval_options.clone()
        );
        let accessible_by_qualified_column_name = clause_context.has_columns_for_types(
            column_types.clone(), CheckAccessibility::QualifiedColumnName, column_retrieval_options
        );
        ValueSetterValue::NameAccessibilityOfSelectedTypes(NameAccessibilityOfSelectedTypesValue {
            accessible_by_column_name,
            accessible_by_qualified_column_name,
        })
    }
}

#[derive(Debug, Clone)]
pub struct SelectedTypesAccessibleByNamingMethodModifier {}

impl StatelessCallModifier for SelectedTypesAccessibleByNamingMethodModifier {
    fn get_name(&self) -> SmolStr {
        SmolStr::new("selected_types_accessible_by_naming_method")
    }

    fn get_associated_value_name(&self) -> Option<SmolStr> {
        Some(NameAccessibilityOfSelectedTypesValue::name())
    }

    fn run(&self, function_context: &FunctionContext, associated_value: Option<&ValueSetterValue>) -> bool {
        let associated_value = unwrap_variant!(associated_value.unwrap(), ValueSetterValue::NameAccessibilityOfSelectedTypes);
        match function_context.current_node.node_common.name.as_str() {
            "unqualified_column_name" => associated_value.accessible_by_column_name,
            "qualified_column_name" => associated_value.accessible_by_qualified_column_name,
            any => panic!("selected_types_accessible_by_naming_method cannot be called at {any}")
        }
    }
}

#[derive(Debug, Clone)]
pub struct WildcardRelationsValue {
    /// relations eligible to be selected by qualified wildcard, from the
    /// current FROM to the upmost parent FROM
    pub relation_levels_selectable_by_qualified_wildcard: Vec<Vec<IdentName>>,
    /// wildcard can be used
    pub can_use_wildcard: bool,
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
        let query_types = unwrap_variant!(&function_context.call_params.selected_types, CallTypes::QueryTypes);
        ValueSetterValue::WildcardRelations(WildcardRelationsValue {
            can_use_wildcard: clause_context.can_use_wildcard(query_types),
            relation_levels_selectable_by_qualified_wildcard: clause_context.get_relation_levels_selectable_by_qualified_wildcard(query_types),
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
        match function_context.current_node.node_common.name.as_str() {
            "SELECT_wildcard" => wildcard_relations.can_use_wildcard,
            "SELECT_qualified_wildcard" => {
                wildcard_relations.relation_levels_selectable_by_qualified_wildcard.iter().any(
                    |level| level.len() > 0
                )
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
            enabled: clause_context.top_group_by().is_grouping_active(),
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
            "call84_types" => grouping_enabled,
            "call85_types" => !grouping_enabled,
            any => panic!("grouping mode switch cannot be called at {any}")
        }
    }
}

#[derive(Debug, Clone)]
pub struct CanAddMoreColumnsValue {
    /// should add more columns?
    /// None is not enforced
    pub should_add: Option<bool>,
}

impl NamedValue for CanAddMoreColumnsValue {
    fn name() -> SmolStr {
        SmolStr::new("CanAddMoreColumnsValue")
    }
}

#[derive(Debug, Clone)]
pub struct CanAddMoreColumnsValueSetter { }

impl ValueSetter for CanAddMoreColumnsValueSetter {
    fn get_value_name(&self) -> SmolStr {
        CanAddMoreColumnsValue::name()
    }

    fn get_value(&self, _clause_context: &ClauseContext, function_context: &FunctionContext) -> ValueSetterValue {
        ValueSetterValue::CanAddMoreColumns(CanAddMoreColumnsValue {
            should_add: match unwrap_variant!(&function_context.call_params.selected_types, CallTypes::QueryTypes) {
                QueryTypes::ColumnTypeLists { column_type_lists } => Some(column_type_lists.len() > 1),
                // not enforced
                QueryTypes::TypeList { .. } => None,
            },
        })
    }
}

#[derive(Debug, Clone)]
pub struct CanAddMoreColumnsModifier {}

impl StatelessCallModifier for CanAddMoreColumnsModifier {
    fn get_name(&self) -> SmolStr {
        SmolStr::new("CanAddMoreColumnsModifier")
    }

    fn get_associated_value_name(&self) -> Option<SmolStr> {
        Some(CanAddMoreColumnsValue::name())
    }

    fn run(&self, function_context: &FunctionContext, associated_value: Option<&ValueSetterValue>) -> bool {
        let should_add = unwrap_variant!(associated_value.unwrap(), ValueSetterValue::CanAddMoreColumns).should_add;
        match function_context.current_node.node_common.name.as_str() {
            "call1_SELECT_item" => should_add.unwrap_or(true),
            "SELECT_item_can_finish" => should_add.map(|x| !x).unwrap_or(true),
            any => panic!("CanAddMoreColumnsModifier value cannot be evalueted at {any}"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct QueryTypeNotExhaustedValue {
    pub should_continue: Option<bool>,
}

impl NamedValue for QueryTypeNotExhaustedValue {
    fn name() -> SmolStr {
        SmolStr::new("QueryTypeNotExhaustedValue")
    }
}

#[derive(Debug, Clone)]
pub struct QueryTypeNotExhaustedValueSetter { }

impl ValueSetter for QueryTypeNotExhaustedValueSetter {
    fn get_value_name(&self) -> SmolStr {
        QueryTypeNotExhaustedValue::name()
    }

    fn get_value(&self, _clause_context: &ClauseContext, function_context: &FunctionContext) -> ValueSetterValue {
        ValueSetterValue::QueryTypeNotExhausted(QueryTypeNotExhaustedValue {
            should_continue: match unwrap_variant!(&function_context.call_params.selected_types, CallTypes::QueryTypes) {
                QueryTypes::ColumnTypeLists { column_type_lists } => Some(column_type_lists.len() > 1),
                QueryTypes::TypeList { .. } => None,
            },
        })
    }
}

#[derive(Debug, Clone)]
pub struct QueryTypeNotExhaustedModifier {}

impl StatelessCallModifier for QueryTypeNotExhaustedModifier {
    fn get_name(&self) -> SmolStr {
        SmolStr::new("QueryTypeNotExhaustedModifier")
    }

    fn get_associated_value_name(&self) -> Option<SmolStr> {
        Some(QueryTypeNotExhaustedValue::name())
    }

    fn run(&self, function_context: &FunctionContext, associated_value: Option<&ValueSetterValue>) -> bool {
        let should_add = unwrap_variant!(associated_value.unwrap(), ValueSetterValue::QueryTypeNotExhausted).should_continue;
        match function_context.current_node.node_common.name.as_str() {
            "call0_set_expression_determine_type" => should_add.unwrap_or(true),
            "set_expression_determine_type_can_finish" => should_add.map(|x| !x).unwrap_or(true),
            any => panic!("QueryTypeNotExhaustedModifier value cannot be evalueted at {any}"),
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
            available: clause_context.has_columns(CheckAccessibility::Either, ColumnRetrievalOptions::new(
                false, false, false
            )),
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

#[derive(Debug, Clone)]
pub struct CanSkipLimitValue {
    /// LIMIT can be skipped
    pub can_skip: bool,
}

impl NamedValue for CanSkipLimitValue {
    fn name() -> SmolStr {
        SmolStr::new("can_skip_limit")
    }
}

#[derive(Debug, Clone)]
pub struct CanSkipLimitValueSetter { }

impl ValueSetter for CanSkipLimitValueSetter {
    fn get_value_name(&self) -> SmolStr {
        CanSkipLimitValue::name()
    }

    fn get_value(&self, clause_context: &ClauseContext, function_context: &FunctionContext) -> ValueSetterValue {
        ValueSetterValue::CanSkipLimit(CanSkipLimitValue {
            can_skip: match function_context.current_node.node_common.name.as_str() {
                "query_can_skip_limit_set_val" => {
                    !function_context.call_params.modifiers.contains(&SmolStr::new("single row")) ||
                    (
                        clause_context.top_group_by().is_single_group() &&
                        clause_context.top_group_by().is_single_row()
                    )
                },
                any => panic!("{any} unexpectedly triggered the can_skip_limit value setter"),
            },
        })
    }
}

#[derive(Debug, Clone)]
pub struct CanSkipLimitModifier {}

impl StatelessCallModifier for CanSkipLimitModifier {
    fn get_name(&self) -> SmolStr {
        SmolStr::new("can_skip_limit_mod")
    }

    fn get_associated_value_name(&self) -> Option<SmolStr> {
        Some(CanSkipLimitValue::name())
    }

    fn run(&self, _function_context: &FunctionContext, associated_value: Option<&ValueSetterValue>) -> bool {
        unwrap_variant!(associated_value.unwrap(), ValueSetterValue::CanSkipLimit).can_skip
    }
}

#[derive(Debug, Clone)]
pub struct SelectAccessibleColumnsValue {
    /// accessible columns
    pub accessible_columns: Vec<IdentName>,
}

impl NamedValue for SelectAccessibleColumnsValue {
    fn name() -> SmolStr {
        SmolStr::new("select_has_accessible_columns")
    }
}

#[derive(Debug, Clone)]
pub struct SelectAccessibleColumnsValueSetter { }

impl ValueSetter for SelectAccessibleColumnsValueSetter {
    fn get_value_name(&self) -> SmolStr {
        SelectAccessibleColumnsValue::name()
    }

    fn get_value(&self, clause_context: &ClauseContext, function_context: &FunctionContext) -> ValueSetterValue {
        ValueSetterValue::SelectAccessibleColumns(SelectAccessibleColumnsValue {
            accessible_columns: match function_context.current_node.node_common.name.as_str() {
                "order_by_select_reference" => {
                    clause_context.query().get_all_select_aliases_iter()
                        .fold(BTreeMap::new(),|mut acc, ident| {
                            // count them
                            *acc.entry(ident.clone()).or_insert(0usize) += 1; acc
                            // retrieve ones mentioned once
                        }).into_iter().filter(|(_, count)| *count == 1).map(|x| x.0).collect()
                },
                any => panic!("{any} unexpectedly triggered the select_has_accessible_columns value setter"),
            },
        })
    }
}

#[derive(Debug, Clone)]
pub struct SelectHasAccessibleColumnsModifier {}

impl StatelessCallModifier for SelectHasAccessibleColumnsModifier {
    fn get_name(&self) -> SmolStr {
        SmolStr::new("select_has_accessible_columns_mod")
    }

    fn get_associated_value_name(&self) -> Option<SmolStr> {
        Some(SelectAccessibleColumnsValue::name())
    }

    fn run(&self, _function_context: &FunctionContext, associated_value: Option<&ValueSetterValue>) -> bool {
        let is_empty = unwrap_variant!(
            associated_value.unwrap(), ValueSetterValue::SelectAccessibleColumns
        ).accessible_columns.is_empty();
        !is_empty
    }
}

#[derive(Debug, Clone)]
pub struct SelectIsNotDistinctValue {
    /// is not distinct
    pub is_not_distinct: bool,
}

impl NamedValue for SelectIsNotDistinctValue {
    fn name() -> SmolStr {
        SmolStr::new("select_is_not_distinct")
    }
}

#[derive(Debug, Clone)]
pub struct SelectIsNotDistinctValueSetter { }

impl ValueSetter for SelectIsNotDistinctValueSetter {
    fn get_value_name(&self) -> SmolStr {
        SelectIsNotDistinctValue::name()
    }

    fn get_value(&self, clause_context: &ClauseContext, function_context: &FunctionContext) -> ValueSetterValue {
        ValueSetterValue::SelectIsNotDistinct(SelectIsNotDistinctValue {
            is_not_distinct: match function_context.current_node.node_common.name.as_str() {
                "order_by_list" => !clause_context.query().is_distinct(),
                any => panic!("{any} unexpectedly triggered the select_is_not_distinct value setter"),
            },
        })
    }
}

#[derive(Debug, Clone)]
pub struct SelectIsNotDistinctModifier {}

impl StatelessCallModifier for SelectIsNotDistinctModifier {
    fn get_name(&self) -> SmolStr {
        SmolStr::new("select_is_not_distinct_mod")
    }

    fn get_associated_value_name(&self) -> Option<SmolStr> {
        Some(SelectIsNotDistinctValue::name())
    }

    fn run(&self, _function_context: &FunctionContext, associated_value: Option<&ValueSetterValue>) -> bool {
        unwrap_variant!(associated_value.unwrap(), ValueSetterValue::SelectIsNotDistinct).is_not_distinct
    }
}

#[derive(Debug, Clone)]
pub struct AvailableTableNamesValue {
    pub table_names: Vec<ObjectName>,
}

impl NamedValue for AvailableTableNamesValue {
    fn name() -> SmolStr {
        SmolStr::new("available_table_names")
    }
}

#[derive(Debug, Clone)]
pub struct AvailableTableNamesValueSetter { }

impl ValueSetter for AvailableTableNamesValueSetter {
    fn get_value_name(&self) -> SmolStr {
        AvailableTableNamesValue::name()
    }

    fn get_value(&self, clause_context: &ClauseContext, function_context: &FunctionContext) -> ValueSetterValue {
        ValueSetterValue::AvailableTableNames(AvailableTableNamesValue {
            table_names: match function_context.current_node.node_common.name.as_str() {
                "FROM_item_alias" => {
                    clause_context.schema_ref().get_all_table_names().into_iter().cloned().collect()
                },
                "FROM_item_no_alias" => {
                    clause_context.get_unused_table_names()
                },
                any => panic!("{any} unexpectedly triggered the available_table_names value setter"),
            },
        })
    }
}

#[derive(Debug, Clone)]
pub struct FromTableNamesAvailableModifier {}

impl StatelessCallModifier for FromTableNamesAvailableModifier {
    fn get_name(&self) -> SmolStr {
        SmolStr::new("from_table_names_available")
    }

    fn get_associated_value_name(&self) -> Option<SmolStr> {
        Some(AvailableTableNamesValue::name())
    }

    fn run(&self, _function_context: &FunctionContext, associated_value: Option<&ValueSetterValue>) -> bool {
        !unwrap_variant!(associated_value.unwrap(), ValueSetterValue::AvailableTableNames).table_names.is_empty()
    }
}
