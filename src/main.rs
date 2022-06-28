use sqlparser::ast::{Expr, SelectItem};

use equivalence_testing::query_creation::helpers::{
    create_compound_identifier, create_select, create_table,
};

fn main() {
    // SELECT R.A FROM R WHERE R.A NOT IN (SELECT S.A FROM S)
    let q1 = create_select(
        vec![SelectItem::UnnamedExpr(create_compound_identifier("R.A"))],
        vec![create_table("R", None)],
        Some(Expr::InSubquery {
            expr: Box::new(create_compound_identifier("R.A")),
            subquery: Box::new(create_select(
                vec![SelectItem::UnnamedExpr(create_compound_identifier("S.A"))],
                vec![create_table("S", None)],
                None,
                false,
            )),
            negated: true,
        }),
        false,
    );
    println!("Query: {:#?}", q1.to_string());
}
