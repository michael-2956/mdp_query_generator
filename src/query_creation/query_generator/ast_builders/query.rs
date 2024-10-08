use itertools::Itertools;
use sqlparser::ast::{Query, SetExpr};

use crate::query_creation::{query_generator::{ast_builders::{limit::LimitBuilder, order_by::OrderByBuilder}, query_info::IdentName, QueryGenerator}, state_generator::{markov_chain_generator::markov_chain::QueryTypes, state_choosers::StateChooser, subgraph_type::SubgraphType}};

use super::{set_expression::SetExpressionBuilder, set_expression_determine_type::SetExpressionDetermineTypeBuilder};

pub struct QueryBuilder { }

fn query_with_setexpr(setexpr: SetExpr) -> Query {
    Query {
        with: None,
        body: Box::new(setexpr),
        order_by: vec![],
        limit: None,
        offset: None,
        fetch: None,
        locks: vec![],
        limit_by: vec![],
        for_clause: None,
    }
}

// left can be a set operation
// right is ALWAYS select
// number of columns and column types must match

// basically we create a set operation query
// if agent selects intersect/union/except,
// we make it into a set operation
// we place existing query on the left and new one on the right
// if even more additions are required we place the set operation on the left
// and place new query on the right.
// in case of intersect, the same happens but in reverse.


// So problems: single row is possible with intersect of multiple queries,
// but actually we would have to know what's in the database to do that.
// So if we have a single row any operations are forbidden.



// + task 1: separate into query and select query
// task 2: add union operations but order by is OFF when they are used. Update ast2path
// task 3: study order by behavior
// task 4: add order by
// task 5: test & fix bugs
// task 6: update ast 2 path
// task 7: test syntactic coverage on spider
// task 8: study & add with
// task 9: update ast 2 path
// task 10: test & fix bugs
// task 11: test syntaxtic coverage on TPC-DS (adapt queries beforehand) and spider
// task 12: find if anything is uncovered in tpc ds, cover it
// task 13: modify all the queries as required for spider
// task 14: describe experiment setup fully for syntax in paper
// task 15: describe experiment setup fully for semantics in paper
// task 16: polish other paper sections
// + ADDITIONAL TASKS:
// [
//    task 17: discover how learnedsqlgen agent works and see how it can be implemented
//    task 18: implement it and try to conduct experiment
//    task 19: describe experiments in the paper and compare results
// ]
// task 20: final polish
// task 21: submit

/// subgraph def_Query
impl QueryBuilder {
    pub fn nothing() -> Query {
        query_with_setexpr(SetExpressionBuilder::nothing())
    }

    /// highlights the output type. Query does not\
    /// have the usual "highlight" function. Instead,\
    /// use "nothing" before generation.
    /// 
    /// This function is only needed to highlight that \
    /// some decision will be affecting the single \
    /// output column of the query.
    pub fn highlight_type() -> Query {
        query_with_setexpr(SetExpressionBuilder::highlight_type())
    }

    pub fn build<StC: StateChooser>(
        generator: &mut QueryGenerator<StC>, query: &mut Query
    ) -> Vec<(Option<IdentName>, SubgraphType)> {
        generator.substitute_model.notify_subquery_creation_begin();
        generator.clause_context.on_query_begin(generator.state_generator.get_fn_modifiers_opt());
        generator.expect_state("Query");

        generator.expect_state("call1_set_expression_determine_type");
        let column_type_list = QueryTypes::ColumnTypeLists {
            column_type_lists: SetExpressionDetermineTypeBuilder::build(
                generator
            ).into_iter().map(|x| vec![x]).collect_vec()
        };

        generator.expect_state("call0_set_expression");
        generator.state_generator.set_known_query_type_list(column_type_list);
        query.body = Box::new(SetExpressionBuilder::nothing());
        SetExpressionBuilder::build(generator, &mut query.body);

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
