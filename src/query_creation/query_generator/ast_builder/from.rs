use sqlparser::ast::{self, Ident, Join, ObjectName, TableWithJoins};

use crate::query_creation::{query_generator::{match_next_state, value_choosers::QueryValueChooser, QueryGenerator, TypeAssertion}, state_generator::{state_choosers::StateChooser, subgraph_type::SubgraphType, substitute_models::SubstituteModel}};

/// subgraph def_FROM
pub struct FromBuilder { }

impl FromBuilder {
    pub fn empty() -> Vec<TableWithJoins> {
        vec![TableWithJoins { relation: sqlparser::ast::TableFactor::Table {
            name: ObjectName(vec![Ident::new("[?]")]),
            alias: None,
            args: None,
            columns_definition: None,
            with_hints: vec![]
        }, joins: vec![] }]
    }

    pub fn build<SubMod: SubstituteModel, StC: StateChooser, QVC: QueryValueChooser>(
        generator: &mut QueryGenerator<SubMod, StC, QVC>, from: &mut Vec<TableWithJoins>
    ) {
        generator.expect_state("FROM");

        loop {
            generator.clause_context.top_from_mut().add_subfrom();
            from.push(TableWithJoins { relation: match_next_state!(generator, {
                "call0_FROM_item" => generator.handle_from_item(),
                "EXIT_FROM" => {
                    generator.clause_context.top_from_mut().delete_subfrom();
                    break
                },
            }), joins: vec![] });

            match_next_state!(generator, {
                "FROM_cartesian_product" => { },
                "FROM_join_by" => {
                    let joins = &mut from.last_mut().unwrap().joins;
                    loop {
                        let join_type = match_next_state!(generator, {
                            s @ ( "FROM_join_join" | "FROM_left_join" | "FROM_right_join" | "FROM_full_join" ) => s.to_string(),
                        });
                        generator.expect_states(&["FROM_join_to", "call1_FROM_item"]);
                        let relation = generator.handle_from_item();
                        // only activate sub-from for JOIN ON
                        generator.clause_context.top_from_mut().activate_subfrom();
                        generator.expect_states(&["FROM_join_on", "call83_types"]);
                        let join_on = ast::JoinConstraint::On(
                            generator.handle_types(TypeAssertion::GeneratedBy(SubgraphType::Val3)).1
                        );
                        generator.clause_context.top_from_mut().deactivate_subfrom();
                        joins.push(Join {
                            relation,
                            join_operator: match join_type.as_str() {
                                "FROM_join_join" => ast::JoinOperator::Inner(join_on),
                                "FROM_left_join" => ast::JoinOperator::LeftOuter(join_on),
                                "FROM_right_join" => ast::JoinOperator::RightOuter(join_on),
                                "FROM_full_join" => ast::JoinOperator::FullOuter(join_on),
                                any => generator.panic_unexpected(any)
                            },
                        });
                        match_next_state!(generator, {
                            "FROM_join_by" => { },
                            "FROM_cartesian_product" => break,
                        });
                    }
                },
            });

            generator.clause_context.top_from_mut().delete_subfrom();
        }
        generator.clause_context.top_from_mut().activate_from();
    }
}
