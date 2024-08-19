use sqlparser::ast::{Expr, GroupByExpr, Query, Select, SetExpr, Value};

use crate::{query_creation::{query_generator::{ast_builders::{from::FromBuilder, group_by::GroupByBuilder, having::HavingBuilder, limit::LimitBuilder, order_by::OrderByBuilder, select::SelectBuilder, where_clause::WhereBuilder}, match_next_state, query_info::IdentName, QueryGenerator}, state_generator::{state_choosers::StateChooser, subgraph_type::SubgraphType}}, unwrap_pat};

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
                group_by: GroupByExpr::Expressions(vec![]),
                cluster_by: vec![],
                distribute_by: vec![],
                sort_by: vec![],
                having: None,
                qualify: None,
                named_window: vec![],
            }
        ))),
        order_by: vec![],
        limit: None,
        offset: None,
        fetch: None,
        locks: vec![],
        limit_by: vec![],
        for_clause: None,
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
    /// 
    /// This functiton s only needed to highlight that \
    /// some decision will be affecting the single \
    /// output column of the query.
    pub fn highlight_type() -> Query {
        query_with_select(false)
    }

    pub fn build<StC: StateChooser>(
        generator: &mut QueryGenerator<StC>, query: &mut Query
    ) -> Vec<(Option<IdentName>, SubgraphType)> {
        generator.substitute_model.notify_subquery_creation_begin();
        generator.clause_context.on_query_begin(generator.state_generator.get_fn_modifiers_opt());
        generator.expect_state("Query");

        let select_body = &mut *unwrap_pat!(*query.body, SetExpr::Select(ref mut b), b);

        generator.expect_state("call0_FROM");
        select_body.from = FromBuilder::highlight();
        FromBuilder::build(generator, &mut select_body.from);

        select_body.selection = Some(WhereBuilder::highlight());
        match_next_state!(generator, {
            "call0_WHERE" => {
                let where_val3 = select_body.selection.as_mut().unwrap();
                WhereBuilder::build(generator, where_val3);
                generator.expect_state("WHERE_done");
            },
            "WHERE_done" => { select_body.selection = None; },
        });

        select_body.group_by = GroupByBuilder::highlight();
        match_next_state!(generator, {
            "call0_GROUP_BY" => {
                GroupByBuilder::build(generator, &mut select_body.group_by);

                select_body.having = Some(HavingBuilder::highlight());
                match_next_state!(generator, {
                    "call0_HAVING" => {
                        HavingBuilder::build(generator, select_body.having.as_mut().unwrap());
                        generator.expect_state("call0_SELECT");
                    },
                    "call0_SELECT" => {
                        select_body.having = None;
                    },
                });
            },
            "call0_SELECT" => {
                select_body.group_by = GroupByBuilder::nothing();
            },
        });

        (select_body.distinct, select_body.projection) = SelectBuilder::nothing();
        SelectBuilder::build(generator, &mut select_body.distinct, &mut select_body.projection);

        if generator.clause_context.top_group_by().is_grouping_active() && !generator.clause_context.query().is_aggregation_indicated() {
            if select_body.group_by == GroupByExpr::Expressions(vec![]) {
                // Grouping is active but wasn't indicated in any way. Add GROUP BY true
                // instead of GROUP BY (), because unsupported by parser
                select_body.group_by = GroupByExpr::Expressions(vec![Expr::Value(Value::Boolean(true))])
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
