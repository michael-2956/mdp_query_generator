
use sqlparser::parser::{Parser, ParserError};

use sqlparser::ast::{
    Expr, Query, SetExpr,
};
use sqlparser::dialect::PostgreSqlDialect;

use std::io::Write;
use std::fs::OpenOptions;

pub fn string_to_query(input: &str) -> Option<Box<Query>> {
    let dialect = PostgreSqlDialect {};
    match Parser::parse_sql(&dialect, input) {
        Ok(ast) => match ast.into_iter().next().expect("No single query in sql file") {
            sqlparser::ast::Statement::Query(query) => Some(query),
            _ => panic!("Query present in file is not a SELECT query.")
        },
        Err(err) => {
            if err != ParserError::RecursionLimitExceeded {
                // compensating for the fact that the parser does not
                // always produce RecursionLimitExceeded when it is what
                // actually happens
                if input.len() < 1000 {
                    eprintln!("Query: {}", input);
                    eprintln!("Parsing error! {}", err);
                    eprintln!("Query length: {}", input.len());
                } else {
                    let mut parser_bugs = OpenOptions::new()
                        .create(true).write(true).append(true).open("parser_bugs.sql").unwrap();
                    write!(parser_bugs, "{};\n", input).unwrap();
                    eprintln!("Query parsing: Skipped a query of length {} (assumed parser bug)", input.len());
                }
            } else {
                eprintln!("Query parsing: Skipped a query of length {} (parser recursion limit)", input.len());
            }
            None
        },
    }
}

pub fn check_query(query: Box<Query>) -> bool {
    let body = query.body;
    let select = match *body {
        SetExpr::Select(select) => select,
        _ => panic!("query is not a SELECT query.")
    };
    let where_ = select.selection;
    return check_where(where_);
}

fn check_where(where_opt: Option<Expr>) -> bool {
    if let Some(where_) = where_opt {
        check_expr(where_)
    } else { true }
}

fn check_expr(expr: Expr) -> bool {
    match expr {
        // Identifier e.g. table name or column name
        Expr::Identifier(_ident) => true,
        // Multi-part identifier, e.g. `table_alias.column` or `schema.table.col`
        Expr::CompoundIdentifier(_vec) => true,
        // CompositeAccess (postgres) eg: SELECT (information_schema._pg_expandarray(array['i','i'])).n    
        Expr::CompositeAccess{expr, key: _} => check_expr(*expr),
        // "IS FALSE", "IS TRUE", "IS NULL", "IS NOT NULL" return boolean value and are not nullable
        // `IS FALSE` operator
        Expr::IsFalse(_expr) => true,
        Expr::IsNotFalse(_) => true,
        // `IS UNKNOWN` operator
        Expr::IsUnknown(_) => true,
        Expr::IsNotUnknown(_) => true,
        // `IS TRUE` operator
        Expr::IsTrue(_expr) => true,
        Expr::IsNotTrue(_) => true,
        // `IS NULL` operator
        Expr::IsNull(_expr) => true,
        // `IS NOT NULL` operator
        Expr::IsNotNull(_expr) => true,
        // "IS DISTINCT FROM" return boolean value and is not nullable
        // `IS DISTINCT FROM` operator
        Expr::IsDistinctFrom(_expr_first, _expr_second) => true,
        // "IS NOT DISTINCT FROM" return boolean value and is not nullable
        // `IS NOT DISTINCT FROM` operator
        Expr::IsNotDistinctFrom(_expr, _negated) => true,
        // `[ NOT ] IN (val1, val2, ...)`
        Expr::InList{expr, list, negated} => {
            if negated {
                check_expr(*expr) && list.into_iter().all(check_expr)
            } else { true }
        },
        // `[ NOT ] IN (SELECT ...)`
        Expr::InSubquery {expr: _, subquery: _, negated} => !negated,  // !!!!! FALSE IS HERE
        // `[ NOT ] IN UNNEST(array_expression)`
        Expr::InUnnest {  
            expr, array_expr, negated,
        } => {
            if negated {
                check_expr(*expr) && check_expr(*array_expr)
            } else { true }
        },
        // A literal value, such as string, number, date or NULL
        Expr::Value(_val) => true,
        // BETWEEN is treated as comparison operation
        // `<expr> [ NOT ] BETWEEN <low> AND <high>`
        Expr::Between {
            expr, negated, low, high,
        } => {
            if negated {
                check_expr(*low) && check_expr(*high) && check_expr(*expr)
            } else { true }
        },
        // Binary operation e.g. `1 + 1` or `foo > bar`
        Expr::BinaryOp {
            left, op: _, right,
        } => check_expr(*left) && check_expr(*right),
        // Any operation e.g. `1 ANY (1)` or `foo > ANY(bar)`, It will be wrapped in the right side of BinaryExpr
        Expr::AnyOp { left, compare_op: _, right } => check_expr(*left) && check_expr(*right),
        // ALL operation e.g. `1 ALL (1)` or `foo > ALL(bar)`, It will be wrapped in the right side of BinaryExpr
        Expr::AllOp { left, compare_op: _, right } => check_expr(*left) && check_expr(*right),
        // Unary operation e.g. `NOT foo`
        Expr::UnaryOp { op, expr} => {
            if op == sqlparser::ast::UnaryOperator::Not {
                check_expr(*expr)
            } else { true }
        },
        // SUBSTRING(<expr> [FROM <expr>] [FOR <expr>])
        Expr::Substring {
            expr, substring_from, substring_for, special: _
        } => {
            let condition_1 = check_expr(*expr);

            let condition_2 = match substring_from {
                Some(x) => check_expr(*x),
                None => true,
            };

            let condition_3 = match substring_for {
                Some(x) => check_expr(*x),
                None => true,
            };

            return condition_1 && condition_2 && condition_3;
        },
        // Nested expression e.g. `(foo > bar)` or `(1)`
        Expr::Nested(expr) => check_expr(*expr),
        // A literal value, such as string, number, date or NULL /// TODO
        // Expr::Value(value) => {
        //     if value == sqlparser::ast::Value::Null {
        //         false
        //     } else { true }
        // },
        // An exists expression `[ NOT ] EXISTS(SELECT ...)`, used in expressions like
        // `WHERE [ NOT ] EXISTS (SELECT ...)`.
        Expr::Exists {subquery, negated} => {
            if negated {
                return check_query(subquery);
            } else { true }
        },
        // A parenthesized subquery `(SELECT ...)`, used in expression like
        // `SELECT (subquery) AS x` or `WHERE (subquery) = x`
        Expr::Subquery(subquery) => check_query(subquery),
        // An array expression e.g. `ARRAY[1, 2]`
        Expr::Array(array) => array.elem.into_iter().all(check_expr), 
        // The `LISTAGG` function `SELECT LISTAGG(...) WITHIN GROUP (ORDER BY ...)`
        Expr::ListAgg(list_agg) => {
            let condition_1 = check_expr(*list_agg.expr);

            let condition_2 = match list_agg.separator {
                Some(x) => check_expr(*x),
                None => true,
            };

            condition_1 && condition_2
        },
        // The `grouping SETS` expr.
        Expr::GroupingSets(vec_vec) => {
            vec_vec.into_iter().all(|vec| vec.into_iter().all(check_expr))
        },
        // The `CUBE` expr.
        Expr::Cube(vec_vec) => {
            vec_vec.into_iter().all(|vec| vec.into_iter().all(check_expr))
        },
        // The `ROLLUP` expr.
        Expr::Rollup(vec_vec) => {
            vec_vec.into_iter().all(|vec| vec.into_iter().all(check_expr))
        },
        // ROW / TUPLE a single value, such as `SELECT (1, 2)`
        Expr::Tuple(vec) => {
            vec.into_iter().all(check_expr)
        },
        // An array index expression e.g. `(ARRAY[1, 2])[1]` or `(current_schemas(FALSE))[1]`
        Expr::ArrayIndex { 
            obj,
            indexes,
        } => {
            check_expr(*obj) && indexes.into_iter().all(check_expr)
        },
        Expr::JsonAccess { left, operator: _, right } => {
            check_expr(*left) && check_expr(*right)
        },
        _ => true,
    }
}