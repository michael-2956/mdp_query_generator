use std::collections::HashSet;

use itertools::Itertools;
use sqlparser::ast::{ObjectName, SelectItem, WildcardAdditionalOptions};

use crate::{query_creation::{query_generator::{ast_builders::{types::TypesBuilder, types_value::TypeAssertion}, call_modifiers::{ValueSetterValue, WildcardRelationsValue}, highlight_ident, match_next_state, query_info::QueryProps, value_chooser, QueryGenerator}, state_generator::{state_choosers::StateChooser, subgraph_type::SubgraphType, CallTypes}}, unwrap_pat, unwrap_variant};

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
        generator.expect_state("SELECT_item");

        match_next_state!(generator, {
            "SELECT_item_grouping_enabled" => { },
            "SELECT_item_can_finish" => {
                projection.pop(); // last item is not used
                generator.expect_state("EXIT_SELECT_item");
                return
            }
        });

        let select_item = projection.last_mut().unwrap();
        let arg = unwrap_variant!(
            generator.state_generator.get_fn_selected_types_unwrapped(), CallTypes::QueryTypes
        );

        let remaining_columns = match_next_state!(generator, {
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
                let remaining_columns = arg.after_prefix(new_types.iter().map(|(_, tp)| tp));
                generator.clause_context.query_mut().select_type_mut().extend(new_types.into_iter());
                remaining_columns
            },
            alias_node @ ("SELECT_unnamed_expr" | "SELECT_expr_with_alias") => {
                let (first_column_list, remaining_columns) = arg.split_first();
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
                generator.state_generator.set_compatible_list(first_column_list.iter().flat_map(
                    |tp| tp.get_compat_types().into_iter()  // vec![tp.clone()].into_iter()
                ).collect::<HashSet<SubgraphType>>().into_iter().collect_vec());
                let (column_name, subgraph_type) = TypesBuilder::build_store_name(generator, expr, TypeAssertion::CompatibleWithOneOf(&first_column_list));
                alias = alias.or(column_name);
                generator.expect_state("select_expr_done");
                if alias.is_none() {
                    alias = QueryProps::extract_alias(expr);
                }
                generator.clause_context.query_mut().select_type_mut().push((alias, subgraph_type));
                remaining_columns
            }
        });

        generator.expect_state("call1_SELECT_item");
        generator.state_generator.set_known_query_type_list(remaining_columns);
        projection.push(SelectItem::UnnamedExpr(TypesBuilder::highlight()));
        SelectItemBuilder::build(generator, projection);

        generator.expect_state("EXIT_SELECT_item");
    }
}
