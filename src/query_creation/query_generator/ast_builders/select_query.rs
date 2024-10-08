use sqlparser::ast::{Expr, GroupByExpr, Select, Value};

use crate::query_creation::{query_generator::{ast_builders::{from::FromBuilder, group_by::GroupByBuilder, having::HavingBuilder, select::SelectBuilder, where_clause::WhereBuilder}, match_next_state, QueryGenerator}, state_generator::state_choosers::StateChooser};

pub struct SelectQueryBuilder { }

fn select_set_expr(nothing: bool) -> Select {
    let (distinct, projection) = if nothing {
        SelectBuilder::nothing()
    } else { SelectBuilder::highlight() };

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
}

/// subgraph def_Query
impl SelectQueryBuilder {
    pub fn nothing() -> Select {
        select_set_expr(true)
    }

    /// highlights the output type. Select query does not\
    /// have the usual "highlight" function. Instead,\
    /// use "nothing" before generation.
    /// 
    /// This function is only needed to highlight that \
    /// some decision will be affecting the single \
    /// output column of the query.
    pub fn highlight_type() -> Select {
        select_set_expr(false)
    }

    pub fn build<StC: StateChooser>(
        generator: &mut QueryGenerator<StC>, select_body: &mut Select
    ) {
        generator.expect_state("SELECT_query");

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

        generator.expect_state("EXIT_SELECT_query");
    }
}
