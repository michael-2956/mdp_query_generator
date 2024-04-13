use sqlparser::ast::Expr;

use crate::{query_creation::{query_generator::{ast_builders::types::TypesBuilder, match_next_state, query_info::ColumnRetrievalOptions, value_choosers::QueryValueChooser, QueryGenerator, TypeAssertion}, state_generator::{state_choosers::StateChooser, substitute_models::SubstituteModel}}, unwrap_variant};

pub struct GroupByBuilder { }

impl GroupByBuilder {
    pub fn highlight() -> Vec<Expr> {
        vec![]
    }

    pub fn build<SubMod: SubstituteModel, StC: StateChooser, QVC: QueryValueChooser>(
        generator: &mut QueryGenerator<SubMod, StC, QVC>, group_by: &mut Vec<Expr>
    ) {
        generator.expect_state("GROUP_BY");

        match_next_state!(generator, {
            "group_by_single_group" => {
                // grouping is present but not indicated (is implicit)
                generator.clause_context.top_group_by_mut().set_single_group_grouping();
                generator.clause_context.top_group_by_mut().set_single_row_grouping();
                generator.expect_state("EXIT_GROUP_BY");
                return
            },
            "has_accessible_columns" => {
                generator.expect_state("grouping_column_list");
            },
        });

        loop {
            group_by.push(TypesBuilder::highlight());
            let group_by_entry = group_by.last_mut().unwrap();
            let mut return_result = false;

            match_next_state!(generator, {
                "call70_types" => {
                    let column_type = TypesBuilder::build(generator, group_by_entry, TypeAssertion::None);
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
                        set_list.push(vec![]);
                        let current_set = set_list.last_mut().unwrap();
                        let mut finish_grouping_sets = false;

                        loop {
                            match_next_state!(generator, {
                                "call69_types" => {
                                    current_set.push(TypesBuilder::highlight());
                                    let column_expr = current_set.last_mut().unwrap();
                                    let column_type = TypesBuilder::build(generator, column_expr, TypeAssertion::None);

                                    let column_name = generator.clause_context.retrieve_column_by_column_expr(
                                        &column_expr, ColumnRetrievalOptions::new(false, false, false)
                                    ).unwrap().1;
                                    generator.clause_context.top_group_by_mut().append_column(column_name, column_type);
                                }, // either set_list or set_multiple in the next iteration
                                "set_list_empty_allowed" => { },  // one of the following breaks in the next iteration
                                "set_list" => break,
                                "set_multiple" => { },  // either call69_types or one of the following breaks in the next iteration
                                "grouping_column_list" => {
                                    finish_grouping_sets = true;
                                    break;
                                },
                                "EXIT_GROUP_BY" => {
                                    finish_grouping_sets = true;
                                    return_result = true;
                                    break;
                                },
                            });
                        }

                        if finish_grouping_sets {
                            break
                        }
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
            if let &[Expr::GroupingSets(ref set_list)] = group_by.as_slice() {
                if set_list.len() == 1 {  // set is guaranteed to be empty (since no columns)
                    generator.clause_context.top_group_by_mut().set_single_row_grouping();
                }
            }
        }

        generator.clause_context.query_mut().set_aggregation_indicated();
    }
}
