use sqlparser::ast::{ObjectName, SelectItem, WildcardAdditionalOptions};

use crate::{query_creation::{query_generator::{ast_builders::{types::TypesBuilder, types_value::TypeAssertion}, call_modifiers::{ValueSetterValue, WildcardRelationsValue}, highlight_ident, match_next_state, query_info::{IdentName, QueryProps}, value_chooser, QueryGenerator, QueryValueChooser}, state_generator::{state_choosers::StateChooser, subgraph_type::SubgraphType, substitute_models::SubstituteModel}}, unwrap_pat, unwrap_variant};

/// subgraph def_SELECT
pub struct SelectBuilder { }

impl SelectBuilder {
    pub fn nothing() -> (bool, Vec<SelectItem>) {
        (false, vec![])
    }

    pub fn highlight() -> (bool, Vec<SelectItem>) {
        (false, vec![SelectItem::UnnamedExpr(TypesBuilder::highlight())])
    }

    pub fn build<SubMod: SubstituteModel, StC: StateChooser>(
        generator: &mut QueryGenerator<SubMod, StC>, distinct: &mut bool, projection: &mut Vec<SelectItem>
    ) {
        generator.expect_state("SELECT");
        match_next_state!(generator, {
            "SELECT_DISTINCT" => {
                *distinct = true;
                generator.clause_context.query_mut().set_distinct();
                generator.expect_state("SELECT_list");
            },
            "SELECT_list" => { },
        });

        let mut column_idents_and_graph_types: Vec<(Option<IdentName>, SubgraphType)> = vec![];

        loop {
            projection.push(SelectItem::UnnamedExpr(TypesBuilder::highlight()));
            let select_item = projection.last_mut().unwrap();

            match_next_state!(generator, {
                "SELECT_tables_eligible_for_wildcard" => {
                    match_next_state!(generator, {
                        "SELECT_wildcard" => {
                            column_idents_and_graph_types.extend(
                                generator.clause_context.top_active_from().get_wildcard_columns_iter()
                            );
                            *select_item = SelectItem::Wildcard(WildcardAdditionalOptions {
                                opt_exclude: None, opt_except: None, opt_rename: None,
                            });
                        },
                        "SELECT_qualified_wildcard" => {
                            let wildcard_relations = unwrap_variant!(generator.state_generator.get_named_value::<WildcardRelationsValue>().unwrap(), ValueSetterValue::WildcardRelations);
                            let (alias, relation) = value_chooser!(generator).choose_qualified_wildcard_relation(
                                &generator.clause_context, wildcard_relations
                            );
                            column_idents_and_graph_types.extend(relation.get_wildcard_columns());
                            *select_item = SelectItem::QualifiedWildcard(
                                ObjectName(vec![alias]),
                                WildcardAdditionalOptions {
                                    opt_exclude: None, opt_except: None, opt_rename: None,
                                }
                            );
                        }
                    });
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
                    let subgraph_type = TypesBuilder::build(generator, expr, TypeAssertion::None);
                    generator.expect_state("select_expr_done");
                    if alias.is_none() {
                        alias = QueryProps::extract_alias(expr);
                    }
                    column_idents_and_graph_types.push((alias, subgraph_type));
                }
            });
            match_next_state!(generator, {
                "SELECT_list_multiple_values" => generator.expect_state("SELECT_list"),
                "EXIT_SELECT" => break
            });
        }

        generator.clause_context.query_mut().set_select_type(column_idents_and_graph_types);
    }
}
