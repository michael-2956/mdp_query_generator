use sqlparser::{ast::{SelectItem, Expr, BinaryOperator}};

mod query_creation_helpers;
use crate::query_creation_helpers::{create_select, create_compound_identifier, create_table};

fn main() {
    let q1 = create_select(
        vec![SelectItem::UnnamedExpr(create_compound_identifier("R.A"))],
        vec![create_table("R")],
        Some(Expr::InSubquery {
            expr: Box::new(create_compound_identifier("R.A")),
            subquery: Box::new(create_select(
                vec![SelectItem::UnnamedExpr(create_compound_identifier("S.A"))],
                vec![create_table("S")],
                None
            )),
            negated: true,
        })
    );

    println!("Query 1: {:#?}", q1.to_string());

    let q2 = create_select(
        vec![SelectItem::UnnamedExpr(create_compound_identifier("R.A"))],
        vec![create_table("R")],
        Some(Expr::Exists {
            subquery: Box::new(create_select(
                vec![SelectItem::UnnamedExpr(create_compound_identifier("S.A"))],
                vec![create_table("S")],
                Some(Expr::BinaryOp {
                    left: Box::new(create_compound_identifier("S.A")),
                    op: BinaryOperator::Eq,
                    right: Box::new(create_compound_identifier("R.A"))
                })
            )),
            negated: true
        })
    );

    println!("Query 2: {:#?}", q2.to_string());
}