use sqlparser::ast::{ObjectName, SelectItem, WildcardAdditionalOptions};

use crate::{query_creation::{query_generator::{ast_builders::{types::TypesBuilder, types_value::TypeAssertion}, call_modifiers::{ValueSetterValue, WildcardRelationsValue}, highlight_ident, match_next_state, query_info::QueryProps, value_chooser, QueryGenerator}, state_generator::{state_choosers::StateChooser, CallTypes}}, unwrap_pat, unwrap_variant};

/// subgraph def_SELECT_item
pub struct SelectItemBuilder { }

impl SelectItemBuilder {
    pub fn nothing() -> Vec<SelectItem> {
        vec![]
    }

    pub fn highlight() -> Vec<SelectItem> {
        vec![SelectItem::UnnamedExpr(TypesBuilder::highlight())]
    }

    pub fn build<StC: StateChooser>(
        generator: &mut QueryGenerator<StC>, projection: &mut Vec<SelectItem>
    ) {
        generator.expect_states(&["SELECT_item", "SELECT_item_grouping_enabled"]);

        let select_item = projection.last_mut().unwrap();
        let (
            first_column_list, remaining_columns
        ) = unwrap_variant!(
            generator.state_generator.get_fn_selected_types_unwrapped(), CallTypes::QueryTypes
        ).split_first();

        match_next_state!(generator, {
            "SELECT_tables_eligible_for_wildcard" => {
                let new_types = match_next_state!(generator, {
                    "SELECT_wildcard" => {
                        *select_item = SelectItem::Wildcard(WildcardAdditionalOptions {
                            opt_exclude: None, opt_except: None, opt_rename: None, opt_replace: None
                        });
                        Vec::from_iter(generator.clause_context.top_active_from().get_wildcard_columns_iter())
                    },
                    "SELECT_qualified_wildcard" => {
                        let wildcard_relations = unwrap_variant!(generator.state_generator.get_named_value::<WildcardRelationsValue>().unwrap(), ValueSetterValue::WildcardRelations);
                        let (alias, relation) = value_chooser!(generator).choose_qualified_wildcard_relation(
                            &generator.clause_context, wildcard_relations
                        );
                        *select_item = SelectItem::QualifiedWildcard(
                            ObjectName(vec![alias]),
                            WildcardAdditionalOptions {
                                opt_exclude: None, opt_except: None, opt_rename: None, opt_replace: None
                            }
                        );
                        Vec::from_iter(relation.get_wildcard_columns_iter())
                    }
                });
                generator.clause_context.query_mut().select_type_mut().extend(new_types.into_iter());
            },
            alias_node @ ("SELECT_unnamed_expr" | "SELECT_expr_with_alias") => {
                let (mut alias, expr) = match alias_node {
                    "SELECT_unnamed_expr" => (
                        None, unwrap_variant!(select_item, SelectItem::UnnamedExpr)
                    ),
                    "SELECT_expr_with_alias" => {
                        *select_item = SelectItem::ExprWithAlias {
                            expr: TypesBuilder::highlight(), alias: highlight_ident(),
                        };
                        let alias = unwrap_pat!(select_item, SelectItem::ExprWithAlias { alias, .. }, alias);
                        *alias = value_chooser!(generator).choose_select_alias();
                        (
                            Some(alias.clone().into()),
                            unwrap_pat!(select_item, SelectItem::ExprWithAlias { expr, .. }, expr)
                        )
                    },
                    any => generator.panic_unexpected(any)
                };
                generator.expect_state("select_expr");
                match_next_state!(generator, {
                    "call73_types" => { },
                    "call54_types" => { },
                });
                generator.state_generator.set_known_list(first_column_list.clone());
                let subgraph_type = TypesBuilder::build(generator, expr, TypeAssertion::GeneratedByOneOf(&first_column_list));
                generator.expect_state("select_expr_done");
                if alias.is_none() {
                    alias = QueryProps::extract_alias(expr);
                }
                generator.clause_context.query_mut().select_type_mut().push((alias, subgraph_type));
            }
        });

        projection.push(SelectItem::UnnamedExpr(TypesBuilder::highlight()));
        generator.expect_state("SELECT_item_can_add_more_columns");
        match_next_state!(generator, {
            "SELECT_item_can_finish" => { projection.pop(); },
            "call1_SELECT_item" => {
                generator.state_generator.set_known_query_type_list(remaining_columns);
                SelectItemBuilder::build(generator, projection);
            }
        });
        generator.expect_state("EXIT_SELECT_item");
    }
}
