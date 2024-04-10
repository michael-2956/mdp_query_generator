use sqlparser::ast::{Expr, Query, Select, SetExpr, Value};

use crate::{query_creation::{query_generator::{ast_builder::{from::FromBuilder, order_by::OrderByBuilder, select::SelectBuilder, where_clause::WhereBuilder}, match_next_state, query_info::IdentName, value_choosers::QueryValueChooser, QueryGenerator}, state_generator::{state_choosers::StateChooser, subgraph_type::SubgraphType, substitute_models::SubstituteModel}}, unwrap_pat, unwrap_variant};

pub struct QueryBuilder { }

/// subgraph def_Query
impl QueryBuilder {
    pub fn empty() -> Query {
        Query {
            with: None,
            body: Box::new(SetExpr::Select(Box::new(
                Select {
                    distinct: false,
                    top: None,
                    projection: vec![],
                    into: None,
                    from: vec![],
                    lateral_views: vec![],
                    selection: None,
                    group_by: vec![],
                    cluster_by: vec![],
                    distribute_by: vec![],
                    sort_by: vec![],
                    having: None,
                    qualify: None,
                }
            ))),
            order_by: vec![],
            limit: None,
            offset: None,
            fetch: None,
            locks: vec![],
        }
    }

    pub fn build<SubMod: SubstituteModel, StC: StateChooser, QVC: QueryValueChooser>(
        generator: &mut QueryGenerator<SubMod, StC, QVC>, query: &mut Query
    ) -> Vec<(Option<IdentName>, SubgraphType)> {
        generator.substitute_model.notify_subquery_creation_begin();
        generator.clause_context.on_query_begin(generator.state_generator.get_fn_modifiers_opt());
        generator.expect_state("Query");

        let select_body = &mut *unwrap_pat!(*query.body, SetExpr::Select(ref mut b), b);

        generator.expect_state("call0_FROM");
        select_body.from = FromBuilder::empty();
        FromBuilder::build(generator, &mut select_body.from);

        match_next_state!(generator, {
            "call0_WHERE" => {
                select_body.selection = Some(WhereBuilder::empty());
                let where_val3 = unwrap_variant!(&mut select_body.selection, Some);
                WhereBuilder::build(generator, where_val3);

                match_next_state!(generator, {
                    "call0_SELECT" => {},
                    "call0_GROUP_BY" => {
                        // SELECT FROM T1 WHERE C1 = 1 GROUP BY [?]
                        select_body.group_by = generator.handle_group_by(); 
                        // SELECT FROM T1 WHERE C1 = 1 GROUP BY C2
                        match_next_state!(generator, {
                            "call0_SELECT" => {},
                            "call0_HAVING" => {
                                // SELECT FROM T1 WHERE C1 = 1 GROUP BY C2 HAVING [?]
                                select_body.having = Some(generator.handle_having().1);
                                // SELECT FROM T1 WHERE C1 = 1 GROUP BY C2 HAVING C2 > 2
                                generator.expect_state("call0_SELECT");
                            }, 
                        });
                    },
                });
            },
            "call0_SELECT" => {},
            "call0_GROUP_BY" => {
                // SELECT FROM T1 GROUP BY [?]
                select_body.group_by = generator.handle_group_by();
                // SELECT FROM T1 GROUP BY C2
                match_next_state!(generator, {
                    "call0_SELECT" => {},
                    "call0_HAVING" => {
                        // SELECT FROM T1 GROUP BY C2 HAVING [?]
                        select_body.having = Some(generator.handle_having().1);
                        // SELECT FROM T1 GROUP BY C2 HAVING C2 > 2
                        generator.expect_state("call0_SELECT");
                    },
                });

            },
        });

        (select_body.distinct, select_body.projection) = SelectBuilder::empty();
        SelectBuilder::build(generator, &mut select_body.distinct, &mut select_body.projection);

        if generator.clause_context.top_group_by().is_grouping_active() && !generator.clause_context.query().is_aggregation_indicated() {
            if select_body.group_by.is_empty() {
                // Grouping is active but wasn't indicated in any way. Add GROUP BY true
                // instead of GROUP BY (), because unsupported by parser
                select_body.group_by = vec![Expr::Value(Value::Boolean(true))]
            }
        }

        generator.expect_state("call0_ORDER_BY");
        query.order_by = OrderByBuilder::empty();
        OrderByBuilder::build(generator, &mut query.order_by);

        generator.expect_state("call0_LIMIT");
        query.limit = generator.handle_limit().1;

        generator.expect_state("EXIT_Query");
        let output_type = generator.clause_context.query_mut().pop_output_type();
        // if output_type.iter().filter_map(|(o, _)| o.as_ref()).any(|o| o.value == "case") {
        //     eprintln!("output: {:?}", output_type);
        // }
        generator.substitute_model.notify_subquery_creation_end();
        generator.clause_context.on_query_end();

        output_type
    }
}
