use sqlparser::ast::{Join, JoinConstraint, JoinOperator, TableWithJoins};

use crate::{query_creation::{query_generator::{ast_builder::{from_item::FromItemBuilder, types::TypesBuilder}, match_next_state, value_choosers::QueryValueChooser, QueryGenerator, TypeAssertion}, state_generator::{state_choosers::StateChooser, subgraph_type::SubgraphType, substitute_models::SubstituteModel}}, unwrap_pat};

/// subgraph def_FROM
pub struct FromBuilder { }

impl FromBuilder {
    pub fn empty() -> Vec<TableWithJoins> {
        vec![]  // the decision to include the clause is in the builder itself
    }

    pub fn build<SubMod: SubstituteModel, StC: StateChooser, QVC: QueryValueChooser>(
        generator: &mut QueryGenerator<SubMod, StC, QVC>, from: &mut Vec<TableWithJoins>
    ) {
        generator.expect_state("FROM");

        loop {
            generator.clause_context.top_from_mut().add_subfrom();
            match_next_state!(generator, {
                "call0_FROM_item" => {
                    from.push(TableWithJoins {
                        relation: FromItemBuilder::empty(),
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
                        let join_on = JoinConstraint::On(TypesBuilder::empty());
                        let join_operator = match_next_state!(generator, {
                            "FROM_join_join" => JoinOperator::Inner(join_on),
                            "FROM_left_join" => JoinOperator::LeftOuter(join_on),
                            "FROM_right_join" => JoinOperator::RightOuter(join_on),
                            "FROM_full_join" => JoinOperator::FullOuter(join_on),
                        });
                        joins.push(Join {
                            relation: FromItemBuilder::empty(),
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
