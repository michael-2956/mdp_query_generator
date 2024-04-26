use sqlparser::ast::{Expr, OrderByExpr};

use crate::{query_creation::{query_generator::{ast_builders::types_value::TypeAssertion, call_modifiers::{SelectAccessibleColumnsValue, ValueSetterValue}, match_next_state, value_chooser, QueryGenerator}, state_generator::state_choosers::StateChooser}, unwrap_variant};

use super::types::TypesBuilder;

/// subgraph def_ORDER_BY
pub struct OrderByBuilder { }

impl OrderByBuilder {
    pub fn highlight() -> Vec<OrderByExpr> {
        vec![OrderByExpr {
            expr: TypesBuilder::highlight(),
            asc: None,
            nulls_first: None,
        }]  // the decision to include the clause is in the builder itself
    }

    pub fn build<StC: StateChooser>(
        generator: &mut QueryGenerator<StC>, order_by: &mut Vec<OrderByExpr>
    ) {
        generator.expect_state("ORDER_BY");
        match_next_state!(generator, {
            "EXIT_ORDER_BY" => {
                *order_by = vec![];
                return
            },
            "order_by_list" => { },
        });

        loop {
            let order_by_expr = order_by.last_mut().unwrap();

            match_next_state!(generator, {
                "order_by_select_reference" => {
                    generator.expect_state("order_by_select_reference_by_alias");
                    let aliases = &unwrap_variant!(generator.state_generator
                        .get_named_value::<SelectAccessibleColumnsValue>().unwrap(),
                        ValueSetterValue::SelectAccessibleColumns
                    ).accessible_columns.iter().collect::<Vec<_>>();
                    let alias = value_chooser!(generator).choose_select_alias_order_by(aliases);
                    order_by_expr.expr = Expr::Identifier(alias);
                },
                "order_by_expr" => {
                    match_next_state!(generator, {
                        "call84_types" | "call85_types" => { }
                    });
                    TypesBuilder::build(generator, &mut order_by_expr.expr, TypeAssertion::None);
                },
            });
            generator.expect_state("order_by_expr_done");

            match_next_state!(generator, {
                "order_by_order_selected" => { },
                "order_by_asc" => {
                    order_by_expr.asc = Some(true);
                    generator.expect_state("order_by_order_selected");
                },
                "order_by_desc" => {
                    order_by_expr.asc = Some(false);
                    generator.expect_state("order_by_order_selected");
                },
            });

            match_next_state!(generator, {
                "order_by_nulls_order_selected" => { },
                "order_by_nulls_first" => {
                    order_by_expr.nulls_first = Some(true);
                    generator.expect_state("order_by_nulls_order_selected");
                },
                "order_by_nulls_last" => {
                    order_by_expr.nulls_first = Some(false);
                    generator.expect_state("order_by_nulls_order_selected");
                },
            });

            match_next_state!(generator, {
                "order_by_list" => {
                    order_by.push(OrderByExpr {
                        expr: TypesBuilder::highlight(),
                        asc: None,
                        nulls_first: None,
                    });
                },
                "EXIT_ORDER_BY" => break,
            });
        }
    }
}
