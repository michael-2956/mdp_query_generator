use sqlparser::ast::{Expr, GroupByExpr};

use crate::{query_creation::{query_generator::{ast_builders::{column_spec::ColumnSpecBuilder, set_item::SetItemBuilder, types::TypesBuilder}, match_next_state, query_info::ColumnRetrievalOptions, QueryGenerator}, state_generator::state_choosers::StateChooser}, unwrap_variant};

pub struct GroupByBuilder { }

impl GroupByBuilder {
    pub fn highlight() -> GroupByExpr {
        GroupByExpr::Expressions(vec![TypesBuilder::highlight()])
    }

    pub fn nothing() -> GroupByExpr {
        GroupByExpr::Expressions(vec![])
    }

    pub fn build<StC: StateChooser>(
        generator: &mut QueryGenerator<StC>, group_by: &mut GroupByExpr
    ) {
        generator.expect_state("GROUP_BY");

        let group_by_vec = unwrap_variant!(group_by, GroupByExpr::Expressions);

        match_next_state!(generator, {
            "group_by_single_group" => {
                // grouping is present but not indicated (is implicit)
                generator.clause_context.top_group_by_mut().set_single_group_grouping();
                generator.clause_context.top_group_by_mut().set_single_row_grouping();
                generator.expect_state("EXIT_GROUP_BY");
                *group_by_vec = vec![];
                return
            },
            "has_accessible_columns" => {
                generator.expect_state("grouping_column_list");
            },
        });

        *group_by_vec = vec![];

        loop {
            group_by_vec.push(TypesBuilder::highlight());
            let group_by_entry = group_by_vec.last_mut().unwrap();
            let mut return_result = false;

            match_next_state!(generator, {
                "call1_column_spec" => {
                    let column_type = ColumnSpecBuilder::build(generator, group_by_entry);
                    let column_name = generator.clause_context.retrieve_column_by_column_expr(
                        group_by_entry, ColumnRetrievalOptions::new(false, false, false)
                    ).unwrap().1;

                    generator.clause_context.top_group_by_mut().append_column(column_name, column_type);

                    match_next_state!(generator, {
                        "grouping_column_list" => { },
                        "EXIT_GROUP_BY" => return_result = true,
                    });
                },
                "special_grouping" => {
                    let set_list = match_next_state!(generator, {
                        "grouping_set" => {
                            *group_by_entry = Expr::GroupingSets(vec![]);
                            unwrap_variant!(group_by_entry, Expr::GroupingSets)
                        },
                        "grouping_rollup" => {
                            *group_by_entry = Expr::Rollup(vec![]);
                            unwrap_variant!(group_by_entry, Expr::Rollup)
                        },
                        "grouping_cube" => {
                            *group_by_entry = Expr::Cube(vec![]);
                            unwrap_variant!(group_by_entry, Expr::Cube)
                        },
                    });
                    generator.expect_state("set_list");
                    loop {
                        set_list.push(SetItemBuilder::highlight());
                        let current_set = set_list.last_mut().unwrap();

                        match_next_state!(generator, {
                            "call1_set_item" => {
                                SetItemBuilder::build(generator, current_set);
                            },
                            "set_list_empty_allowed" => {
                                *current_set = vec![];
                            },
                        });

                        match_next_state!(generator, {
                            "set_list" => { },
                            "grouping_column_list" => { break; },
                            "EXIT_GROUP_BY" => {
                                return_result = true;
                                break;
                            },
                        });
                    }
                },
            });

            if return_result {
                break
            }
        }

        // For cases such as: GROUPING SETS ( (), (), () ), set single group
        if !generator.clause_context.top_group_by().contains_columns() {
            generator.clause_context.top_group_by_mut().set_single_group_grouping();
            // For GROUPING SETS ( () ), set single row
            if let &[Expr::GroupingSets(ref set_list)] = group_by_vec.as_slice() {
                if set_list.len() == 1 {  // set is guaranteed to be empty (since no columns)
                    generator.clause_context.top_group_by_mut().set_single_row_grouping();
                }
            }
        }

        generator.clause_context.query_mut().set_aggregation_indicated();
    }
}
