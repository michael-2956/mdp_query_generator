use sqlparser::ast::Expr;

use crate::query_creation::{query_generator::{ast_builders::column_spec::ColumnSpecBuilder, match_next_state, query_info::ColumnRetrievalOptions, QueryGenerator}, state_generator::state_choosers::StateChooser};

use super::types::TypesBuilder;

/// subgraph def_set_item
pub struct SetItemBuilder { }

impl SetItemBuilder {
    /// highlights the last expression
    pub fn highlight() -> Vec<Expr> {
        vec![TypesBuilder::highlight()]
    }

    /// builds the last expression in current_set
    /// adds an another expression to current_set if desired
    pub fn build<StC: StateChooser>(
        generator: &mut QueryGenerator<StC>, current_set: &mut Vec<Expr>
    ) {
        generator.expect_state("set_item");
        generator.expect_state("call2_column_spec");

        let column_expr = current_set.last_mut().unwrap();
        let column_type = ColumnSpecBuilder::build(generator, column_expr);
        let column_name = generator.clause_context.retrieve_column_by_column_expr(
            &column_expr, ColumnRetrievalOptions::new(false, false, false)
        ).unwrap().1;
        generator.clause_context.top_group_by_mut().append_column(column_name, column_type);

        match_next_state!(generator, {
            "call0_set_item" => {
                current_set.push(TypesBuilder::highlight());
                SetItemBuilder::build(generator, current_set);
                generator.expect_state("EXIT_set_item");
            },
            "EXIT_set_item" => { },
        });
    }
}
