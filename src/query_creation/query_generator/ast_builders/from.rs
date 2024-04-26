use sqlparser::ast::{Join, JoinConstraint, JoinOperator, TableWithJoins};

use crate::{query_creation::{query_generator::{ast_builders::{from_item::FromItemBuilder, types::TypesBuilder}, match_next_state, QueryGenerator, ast_builders::types_value::TypeAssertion}, state_generator::{state_choosers::StateChooser, subgraph_type::SubgraphType}}, unwrap_pat};

/// subgraph def_FROM
pub struct FromBuilder { }

impl FromBuilder {
    pub fn highlight() -> Vec<TableWithJoins> {
        vec![]  // the decision to include the clause is in the builder itself
    }

    pub fn build<StC: StateChooser>(
        generator: &mut QueryGenerator<StC>, from: &mut Vec<TableWithJoins>
    ) {
        generator.expect_state("FROM");

        loop {
            generator.clause_context.top_from_mut().add_subfrom();
            match_next_state!(generator, {
                "call0_FROM_item" => {
                    from.push(TableWithJoins {
                        relation: FromItemBuilder::highlight(),
                        joins: vec![]
                    });
                    let from_item = &mut from.last_mut().unwrap().relation;
                    FromItemBuilder::build(generator, from_item);
                },
                "EXIT_FROM" => {
                    generator.clause_context.top_from_mut().delete_subfrom();
                    break
                },
            });

            match_next_state!(generator, {
                "FROM_cartesian_product" => { },
                "FROM_join_by" => {
                    let joins = &mut from.last_mut().unwrap().joins;
                    loop {
                        let join_on = JoinConstraint::On(TypesBuilder::highlight());
                        let join_operator = match_next_state!(generator, {
                            "FROM_join_join" => JoinOperator::Inner(join_on),
                            "FROM_left_join" => JoinOperator::LeftOuter(join_on),
                            "FROM_right_join" => JoinOperator::RightOuter(join_on),
                            "FROM_full_join" => JoinOperator::FullOuter(join_on),
                        });
                        joins.push(Join {
                            relation: FromItemBuilder::highlight(),
                            join_operator,
                        });

                        generator.expect_states(&["FROM_join_to", "call1_FROM_item"]);
                        let relation = &mut joins.last_mut().unwrap().relation;
                        FromItemBuilder::build(generator, relation);

                        generator.expect_states(&["FROM_join_on", "call83_types"]);
                        let join_on = unwrap_pat!(&mut joins.last_mut().unwrap().join_operator,
                            JoinOperator::Inner(JoinConstraint::On(join_on)) |
                            JoinOperator::LeftOuter(JoinConstraint::On(join_on)) |
                            JoinOperator::RightOuter(JoinConstraint::On(join_on)) |
                            JoinOperator::FullOuter(JoinConstraint::On(join_on)),
                            join_on
                        );
                        generator.clause_context.top_from_mut().activate_subfrom();
                        TypesBuilder::build(generator, join_on, TypeAssertion::GeneratedBy(SubgraphType::Val3));
                        generator.clause_context.top_from_mut().deactivate_subfrom();

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
