use sqlparser::ast::{ObjectName, TableAlias, TableFactor};

use crate::{query_creation::{query_generator::{call_modifiers::{AvailableTableNamesValue, ValueSetterValue}, highlight_ident, match_next_state, value_chooser, QueryGenerator}, state_generator::state_choosers::StateChooser}, unwrap_pat, unwrap_variant};

use super::query::QueryBuilder;

/// subgraph def_FROM_item
pub struct FromItemBuilder { }

impl FromItemBuilder {
    pub fn highlight() -> TableFactor {
        TableFactor::Table {
            name: ObjectName(vec![highlight_ident()]),
            alias: None,
            args: None,
            with_hints: vec![],
            version: None,
            partitions: vec![],
        }
    }

    pub fn build<StC: StateChooser>(
        generator: &mut QueryGenerator<StC>, from_item: &mut TableFactor
    ) {
        generator.expect_state("FROM_item");

        let alias = unwrap_pat!(from_item, TableFactor::Table { alias, .. }, alias);
        // TODO: is this logical for the LLM?
        *alias = match_next_state!(generator, {
            "FROM_item_alias" => Some(TableAlias {
                name: value_chooser!(generator).choose_from_alias(),
                columns: vec![]
            }),
            "FROM_item_no_alias" => None,
        });

        match_next_state!(generator, {
            "FROM_item_table" => {
                let available_table_names = &unwrap_variant!(
                    generator.state_generator.get_named_value::<AvailableTableNamesValue>().unwrap(),
                    ValueSetterValue::AvailableTableNames
                ).table_names;

                let name = unwrap_pat!(from_item, TableFactor::Table { name, .. }, name);
                *name = value_chooser!(generator).choose_table_name(available_table_names);
                let name = name.clone();

                let alias = unwrap_pat!(from_item, TableFactor::Table { alias, .. }, alias);
                if let Some(table_alias) = alias {
                    let n_columns = generator.clause_context.schema_ref().num_columns_in_table(&name);
                    table_alias.columns = value_chooser!(generator).choose_from_column_renames(n_columns);
                }

                generator.clause_context.add_from_table_by_name(&name, alias.clone()).unwrap();
            },
            "call0_Query" => {
                *from_item = TableFactor::Derived {
                    lateral: false,
                    subquery: Box::new(QueryBuilder::nothing()),
                    alias: alias.clone(),
                };

                let subquery = &mut **unwrap_pat!(from_item, TableFactor::Derived { subquery, .. }, subquery);
                let column_idents_and_graph_types = QueryBuilder::build(generator, subquery);
                
                let alias = &mut *unwrap_pat!(from_item, TableFactor::Derived { alias, .. }, alias);
                if let Some(query_alias) = alias {
                    let n_columns = column_idents_and_graph_types.len();
                    query_alias.columns = value_chooser!(generator).choose_from_column_renames(n_columns);
                }
                generator.clause_context.top_from_mut().append_query(column_idents_and_graph_types, alias.clone().unwrap());
            },
        });

        generator.expect_state("EXIT_FROM_item");
    }
}