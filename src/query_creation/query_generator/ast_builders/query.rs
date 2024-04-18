use sqlparser::ast::{Expr, Query, Select, SetExpr, Value};

use crate::{query_creation::{query_generator::{ast_builders::{from::FromBuilder, group_by::GroupByBuilder, having::HavingBuilder, limit::LimitBuilder, order_by::OrderByBuilder, select::SelectBuilder, where_clause::WhereBuilder}, match_next_state, query_info::IdentName, QueryGenerator}, state_generator::{state_choosers::StateChooser, subgraph_type::SubgraphType, substitute_models::SubstituteModel}}, unwrap_pat, unwrap_variant};

pub struct QueryBuilder { }

fn query_with_select(nothing: bool) -> Query {
    let (distinct, projection) = if nothing {
        SelectBuilder::nothing()
    } else { SelectBuilder::highlight() };

    Query {
        with: None,
        body: Box::new(SetExpr::Select(Box::new(
            Select {
                distinct,
                top: None,
                projection,
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

/// subgraph def_Query
impl QueryBuilder {
    pub fn nothing() -> Query {
        query_with_select(true)
    }

    /// highlights the output type. Query does not\
    /// have the usual "highlight" function. Instead,\
    /// use "nothing" before generation.
    pub fn highlight_type() -> Query {
        query_with_select(false)
    }

    pub fn build<SubMod: SubstituteModel, StC: StateChooser>(
        generator: &mut QueryGenerator<SubMod, StC>, query: &mut Query
    ) -> Vec<(Option<IdentName>, SubgraphType)> {
        generator.substitute_model.notify_subquery_creation_begin();
        generator.clause_context.on_query_begin(generator.state_generator.get_fn_modifiers_opt());
        generator.expect_state("Query");

        let select_body = &mut *unwrap_pat!(*query.body, SetExpr::Select(ref mut b), b);

        generator.expect_state("call0_FROM");
        select_body.from = FromBuilder::highlight();
        FromBuilder::build(generator, &mut select_body.from);

        match_next_state!(generator, {
            "call0_WHERE" => {
                select_body.selection = Some(WhereBuilder::highlight());
                let where_val3 = unwrap_variant!(&mut select_body.selection, Some);
                WhereBuilder::build(generator, where_val3);

                match_next_state!(generator, {
                    "call0_SELECT" => {},
                    "call0_GROUP_BY" => {
                        select_body.group_by = GroupByBuilder::highlight();
                        GroupByBuilder::build(generator, &mut select_body.group_by);
                        match_next_state!(generator, {
                            "call0_SELECT" => {},
                            "call0_HAVING" => {
                                select_body.having = Some(HavingBuilder::highlight());
                                HavingBuilder::build(generator, select_body.having.as_mut().unwrap());
                                generator.expect_state("call0_SELECT");
                            }, 
                        });
                    },
                });
            },
            "call0_SELECT" => {},
            "call0_GROUP_BY" => {
                select_body.group_by = GroupByBuilder::highlight();
                GroupByBuilder::build(generator, &mut select_body.group_by);
                match_next_state!(generator, {
                    "call0_SELECT" => {},
                    "call0_HAVING" => {
                        select_body.having = Some(HavingBuilder::highlight());
                        HavingBuilder::build(generator, select_body.having.as_mut().unwrap());
                        generator.expect_state("call0_SELECT");
                    },
                });
            },
        });

        (select_body.distinct, select_body.projection) = SelectBuilder::nothing();
        SelectBuilder::build(generator, &mut select_body.distinct, &mut select_body.projection);

        if generator.clause_context.top_group_by().is_grouping_active() && !generator.clause_context.query().is_aggregation_indicated() {
            if select_body.group_by.is_empty() {
                // Grouping is active but wasn't indicated in any way. Add GROUP BY true
                // instead of GROUP BY (), because unsupported by parser
                select_body.group_by = vec![Expr::Value(Value::Boolean(true))]
            }
        }

        generator.expect_state("call0_ORDER_BY");
        query.order_by = OrderByBuilder::highlight();
        OrderByBuilder::build(generator, &mut query.order_by);

        generator.expect_state("call0_LIMIT");
        query.limit = LimitBuilder::highlight();
        LimitBuilder::build(generator, &mut query.limit);

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