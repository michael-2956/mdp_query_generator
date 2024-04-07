use sqlparser::ast::{Expr, Query, Select, SetExpr, Value};

use crate::{query_creation::{query_generator::{ast_builder::from::FromBuilder, match_next_state, query_info::IdentName, value_choosers::QueryValueChooser, QueryGenerator}, state_generator::{state_choosers::StateChooser, subgraph_type::SubgraphType, substitute_models::SubstituteModel}}, unwrap_pat};

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
                // SELECT FROM T1 WHERE [?]
                select_body.selection = Some(generator.handle_where().1);
                // SELECT FROM T1 WHERE C1 = 1
                // no "[?]"
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

        // SELECT [?] FROM T1 WHERE C1 = 1

        // handle_select should accept a reference for a select_builder from the
        // current query_builder...

        // once it is needed to print the query, we update the contents with these two lines
        // however, I believe that we would have to create a way to only run these lines...
        // however, a whole branch of the AST will have to be ran to update everything
        // so ideally, in to_string(), we should call query_builder.update_select_component()
        // update_select_component() would call select_builder.update_inprogress_expr_component() and so on.

        // the only unanswered question is -- how do we know that update_select_component should be called
        // specifically? maybe update_inprogress_components method should be available for all component builders
        // in addition, maybe a method called is_component_inprogress. It would be set to true at the start of 
        // the generation, and set to false once the component is extracted...
        
        // Maybe we should have a builders only for the unfinished components... For the finished ones, its just them
        // in the ASTs. then any builder has only up to one sub-builder at a time. So QueryBuilder would have
        // Option<SelectBuilder>, and once update_builders() is called, for every option with a Some, we first
        // call update_builders on them (SelectBuilder), then we perform the actions to put their components to
        // our components.

        // I do believe that we first need all the classes moved to their respective files. That
        // way, we can do SelectBuilder::empty() for every cmoponent initially, but then when
        // we need it, we do self.select_builder = Some(SelectBuilder::builder()) and then
        // self.select_builder.as_ref().map(|bd| bd.build(generator))
        // ... .build() calls generator.query_builder.update() a bunch of times...
        // then we do self.set_select_ast(self.select_builder.take().unwrap().into_ast())
        // NOTE: our .update() does this:
        // if let Some(ref select_builder) = self.select_builder {
        //     self.set_select_ast(select_builder.ast_ref().clone())
        // }

        // Steps to do everything:
        // 1) transfer the handle_... methods to their respective files in
        // the ast_builder directory
        // 2) rewrite the handle_... function calls to do:
        // SelectBuilder::build(generator, |select_ast| {
        //    select_body.projection = select_ast;
        //    select_body.distinct = self.clause_context.query().is_distinct();
        // }) instead
        // This way, instead of returning the ast, update(projection.clone()) lambda will need to be called
        // every time the projection has changed.
        // However, the query ifself maybe a subquery... so in here:
        // SelectBuilder::build(generator, |select_ast| {
        //    select_body.projection = select_ast;
        //    select_body.distinct = self.clause_context.query().is_distinct();
        //    Self::update_select_body(&mut query, select_body.clone())
        //    update(query.clone())
        // })
        // in Self::update_select_body we do Box::new(SetExpr::Select(Box::new(...

        // but update(query.clone()) would take much time to complete since we would
        // need to clone something in many places
        // what is QueryBuilder::build(...) is passed not only the generator,
        // but also the &mut query itself? we still have to know a way of how we
        // integrate this query into its outer code... so a lambda is still needed...
        // but also if the select_body.projection = vec![] and select_body.distinct = false
        // are done, then we may just pass &mut select_body.projection and 
        // &mut select_body.distinct to SelectBuilder::build instead of the lambdas!
        // then we will kinda not need to go an extra mile to do anything at all
        // so the calls will look like:
        // SelectBuilder::build(generator, &mut select_body.projection, &mut select_body.distinct)
        
        // now, the final thing left is... how do we get those "[?]" to appear?
        // I guess we can figure out later? because even if we just give some incomplete Query
        // to chat, it would still be ok, right? And then we think what we do next to actually
        // place that token somewhere... But woudln't it be too late? I hope not

        // but what if:
        // (select_body.projection, select_body.distinct) = SelectBuilder::empty()
        // -> for starters would give vec![] and false, but then can actually give "[?]"
        // SelectBuilder::build(generator, &mut select_body.projection, &mut select_body.distinct)

        select_body.projection = generator.handle_select();
        select_body.distinct = generator.clause_context.query().is_distinct();
        // SELECT C2 FROM T1 WHERE C1 = 1

        if generator.clause_context.top_group_by().is_grouping_active() && !generator.clause_context.query().is_aggregation_indicated() {
            if select_body.group_by.is_empty() {
                // Grouping is active but wasn't indicated in any way. Add GROUP BY true
                // instead of GROUP BY (), because unsupported by parser
                select_body.group_by = vec![Expr::Value(Value::Boolean(true))]
            }
        }

        generator.expect_state("call0_ORDER_BY");
        query.order_by = generator.handle_order_by();

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
