use sqlparser::ast::{BinaryOperator, Expr, UnaryOperator};

use crate::{query_creation::{query_generator::{ast_builders::{list_expr::ListExprBuilder, query::QueryBuilder, types::TypesBuilder, types_type::TypesTypeBuilder, types_value::TypeAssertion}, match_next_state, QueryGenerator}, state_generator::{markov_chain_generator::markov_chain::QueryTypes, state_choosers::StateChooser, subgraph_type::SubgraphType}}, unwrap_pat, unwrap_variant};

/// subgraph def_VAL_3
pub struct Val3Builder { }

impl Val3Builder {
    pub fn highlight() -> Expr {
        TypesBuilder::highlight()
    }

    pub fn build<StC: StateChooser>(
        generator: &mut QueryGenerator<StC>, val3: &mut Expr
    ) -> SubgraphType {
        generator.expect_state("VAL_3");
        match_next_state!(generator, {
            "IsNull" => {
                let is_null_not_flag = match_next_state!(generator, {
                    "IsNull_not" => {
                        generator.expect_state("call55_types");
                        true
                    },
                    "call55_types" => false,
                });
                *val3 = if is_null_not_flag {
                    Expr::IsNotNull(Box::new(TypesBuilder::highlight()))
                } else {
                    Expr::IsNull(Box::new(TypesBuilder::highlight()))
                };

                let expr = &mut **unwrap_pat!(val3, Expr::IsNotNull(expr) | Expr::IsNull(expr), expr);
                TypesBuilder::build(generator, expr, TypeAssertion::None);
            },
            "IsDistinctFrom" => {
                let is_distinct_not_flag = match_next_state!(generator, {
                    "IsDistinctNOT" => {
                        generator.expect_state("call0_types_type");
                        true
                    },
                    "call0_types_type" => false,
                });
                *val3 = if is_distinct_not_flag {
                    Expr::IsNotDistinctFrom(Box::new(TypesBuilder::highlight()), Box::new(TypesBuilder::highlight()))
                } else {
                    Expr::IsDistinctFrom(Box::new(TypesBuilder::highlight()), Box::new(TypesBuilder::highlight()))
                };

                let tp = TypesTypeBuilder::build(generator);
                generator.state_generator.set_compatible_list(tp.get_compat_types());

                // indicate which expression we will be working on
                **unwrap_pat!(val3, Expr::IsNotDistinctFrom(_, expr) | Expr::IsDistinctFrom(_, expr), expr) = TypesBuilder::nothing();

                generator.expect_state("call56_types");
                let expr = &mut **unwrap_pat!(val3, Expr::IsNotDistinctFrom(expr, _) | Expr::IsDistinctFrom(expr, _), expr);
                TypesBuilder::build(generator, expr, TypeAssertion::CompatibleWith(tp.clone()));

                generator.expect_state("call21_types");
                let expr = &mut **unwrap_pat!(val3, Expr::IsNotDistinctFrom(_, expr) | Expr::IsDistinctFrom(_, expr), expr);
                *expr = TypesBuilder::highlight();
                TypesBuilder::build(generator, expr, TypeAssertion::CompatibleWith(tp.clone()));
            },
            "Exists" => {
                let exists_not_flag = match_next_state!(generator, {
                    "Exists_not" => {
                        generator.expect_state("call2_Query");
                        true
                    },
                    "call2_Query" => false,
                });
                *val3 = Expr::Exists {
                    subquery: Box::new(QueryBuilder::nothing()),
                    negated: exists_not_flag
                };
                let subquery = &mut **unwrap_pat!(val3, Expr::Exists { subquery, .. }, subquery);
                QueryBuilder::build(generator, subquery);
            },
            "InList" => {
                let in_list_not_flag = match_next_state!(generator, {
                    "InListNot" => {
                        generator.expect_state("call3_types_type");
                        true
                    },
                    "call3_types_type" => false,
                });
                *val3 = Expr::InList {
                    expr: Box::new(TypesBuilder::highlight()),
                    list: ListExprBuilder::highlight(),
                    negated: in_list_not_flag
                };

                let tp = TypesTypeBuilder::build(generator);
                generator.state_generator.set_compatible_list(tp.get_compat_types());

                *unwrap_pat!(val3, Expr::InList { list, .. }, list) = ListExprBuilder::nothing();

                generator.expect_state("call57_types");
                let expr = &mut **unwrap_pat!(val3, Expr::InList { expr, .. }, expr);
                TypesBuilder::build(generator, expr, TypeAssertion::CompatibleWith(tp.clone()));

                generator.expect_state("call1_list_expr");
                let list = &mut *unwrap_pat!(val3, Expr::InList { list, .. }, list);
                *list = ListExprBuilder::highlight();
                ListExprBuilder::build(generator, list);
            },
            "InSubquery" => {
                let in_subquery_not_flag = match_next_state!(generator, {
                    "InSubqueryNot" => {
                        generator.expect_state("call4_types_type");
                        true
                    },
                    "call4_types_type" => false,
                });
                *val3 = Expr::InSubquery {
                    expr: Box::new(TypesBuilder::highlight()),
                    subquery: Box::new(QueryBuilder::highlight_type()),
                    negated: in_subquery_not_flag
                };

                let tp = TypesTypeBuilder::build(generator);

                **unwrap_pat!(val3, Expr::InSubquery { subquery, .. }, subquery) = QueryBuilder::nothing();

                generator.state_generator.set_compatible_list(tp.get_compat_types());
                generator.expect_state("call58_types");
                let expr = &mut **unwrap_pat!(val3, Expr::InSubquery { expr, .. }, expr);
                TypesBuilder::build(generator, expr, TypeAssertion::CompatibleWith(tp.clone()));

                generator.state_generator.set_compatible_query_type_list(QueryTypes::ColumnTypeLists {
                    column_type_lists: vec![tp.get_compat_types()]  // single column
                });
                generator.expect_state("call3_Query");
                let subquery = &mut **unwrap_pat!(val3, Expr::InSubquery { subquery, .. }, subquery);
                QueryBuilder::build(generator, subquery);
            },
            "Between" => {
                *val3 = Expr::Between {
                    expr: Box::new(TypesBuilder::highlight()),
                    negated: false,
                    low: Box::new(TypesBuilder::highlight()),
                    high: Box::new(TypesBuilder::highlight())
                };
                generator.expect_state("call5_types_type");
                let tp = TypesTypeBuilder::build(generator);
                generator.state_generator.set_compatible_list(tp.get_compat_types());

                **unwrap_pat!(val3, Expr::Between { low, .. }, low) = TypesBuilder::nothing();
                **unwrap_pat!(val3, Expr::Between { high, .. }, high) = TypesBuilder::nothing();

                generator.expect_state("call59_types");
                let expr = &mut **unwrap_pat!(val3, Expr::Between { expr, .. }, expr);
                TypesBuilder::build(generator, expr, TypeAssertion::CompatibleWith(tp.clone()));

                generator.expect_state("BetweenBetween");
                generator.expect_state("call22_types");
                let low = &mut **unwrap_pat!(val3, Expr::Between { low, .. }, low);
                *low = TypesBuilder::highlight();
                TypesBuilder::build(generator, low, TypeAssertion::CompatibleWith(tp.clone()));

                generator.expect_state("BetweenBetweenAnd");
                generator.expect_state("call23_types");
                let high = &mut **unwrap_pat!(val3, Expr::Between { high, .. }, high);
                *high = TypesBuilder::highlight();
                TypesBuilder::build(generator, high, TypeAssertion::CompatibleWith(tp.clone()));
            },
            "BinaryComp" => {
                let binary_comp_op = match_next_state!(generator, {
                    "BinaryCompEqual" => BinaryOperator::Eq,
                    "BinaryCompUnEqual" => BinaryOperator::NotEq,
                    "BinaryCompLess" => BinaryOperator::Lt,
                    "BinaryCompLessEqual" => BinaryOperator::LtEq,
                    "BinaryCompGreater" => BinaryOperator::Gt,
                    "BinaryCompGreaterEqual" => BinaryOperator::GtEq,
                });
                *val3 = Expr::BinaryOp {
                    left: Box::new(TypesBuilder::highlight()),
                    op: binary_comp_op,
                    right: Box::new(TypesBuilder::highlight())
                };

                generator.expect_state("call1_types_type");
                let tp = TypesTypeBuilder::build(generator);
                generator.state_generator.set_compatible_list(tp.get_compat_types());

                **unwrap_pat!(val3, Expr::BinaryOp { right, .. }, right) = TypesBuilder::nothing();

                generator.expect_state("call60_types");
                let left = &mut **unwrap_pat!(val3, Expr::BinaryOp { left, .. }, left);
                TypesBuilder::build(generator, left, TypeAssertion::CompatibleWith(tp.clone()));

                generator.expect_state("call24_types");
                let right = &mut **unwrap_pat!(val3, Expr::BinaryOp { right, .. }, right);
                *right = TypesBuilder::highlight();
                TypesBuilder::build(generator, right, TypeAssertion::CompatibleWith(tp.clone()));
            },
            "AnyAll" => {
                generator.expect_state("AnyAllSelectOp");
                let any_all_op = match_next_state!(generator, {
                    "AnyAllEqual" => BinaryOperator::Eq,
                    "AnyAllUnEqual" => BinaryOperator::NotEq,
                    "AnyAllLess" => BinaryOperator::Lt,
                    "AnyAllLessEqual" => BinaryOperator::LtEq,
                    "AnyAllGreater" => BinaryOperator::Gt,
                    "AnyAllGreaterEqual" => BinaryOperator::GtEq,
                });
                *val3 = Expr::BinaryOp {
                    left: Box::new(TypesBuilder::highlight()),
                    op: any_all_op,
                    right: Box::new(TypesBuilder::highlight()),
                };

                generator.expect_state("call2_types_type");
                let tp = TypesTypeBuilder::build(generator);
                generator.state_generator.set_compatible_list(tp.get_compat_types());

                **unwrap_pat!(val3, Expr::BinaryOp { right, .. }, right) = TypesBuilder::nothing();

                generator.expect_state("call61_types");
                let left = &mut **unwrap_pat!(val3, Expr::BinaryOp { left, .. }, left);
                TypesBuilder::build(generator, left, TypeAssertion::CompatibleWith(tp.clone()));

                let (left, op) = unwrap_pat!(val3, Expr::BinaryOp { left, op, .. }, (left, op));

                generator.expect_state("AnyAllAnyAll");
                let right_inner = match_next_state!(generator, {
                    "AnyAllAnyAllAll" => {
                        *val3 = Expr::AllOp {
                            left: left.clone(), compare_op: op.clone(),
                            right: Box::new(TypesBuilder::highlight())
                        };
                        &mut **unwrap_pat!(val3, Expr::AllOp { right, .. }, right)
                    },
                    "AnyAllAnyAllAny" => {
                        *val3 = Expr::AnyOp {
                            left: left.clone(), compare_op: op.clone(),
                            right: Box::new(TypesBuilder::highlight())
                        };
                        &mut **unwrap_pat!(val3, Expr::AnyOp { right, .. }, right)
                    },
                });

                generator.expect_state("AnyAllSelectIter");
                match_next_state!(generator, {
                    "call4_Query" => {
                        generator.state_generator.set_compatible_query_type_list(QueryTypes::ColumnTypeLists {
                            column_type_lists: vec![tp.get_compat_types()]  // single column
                        });
                        *right_inner = Expr::Subquery(Box::new(QueryBuilder::nothing()));
                        let subquery = &mut **unwrap_variant!(right_inner, Expr::Subquery);
                        QueryBuilder::build(generator, subquery);
                    },
                });
            },
            "BinaryStringLike" => {
                let text_like_not_flag = match_next_state!(generator, {
                    "BinaryStringLikeNot" => {
                        generator.expect_state("call25_types");
                        true
                    },
                    "call25_types" => false,
                });
                *val3 = Expr::Like {
                    expr: Box::new(TypesBuilder::highlight()),
                    negated: text_like_not_flag,
                    pattern: Box::new(TypesBuilder::nothing()),
                    escape_char: None
                };

                let expr = &mut **unwrap_pat!(val3, Expr::Like { expr, .. }, expr);
                TypesBuilder::build(generator, expr, TypeAssertion::GeneratedBy(SubgraphType::Text));

                generator.expect_state("call26_types");
                let pattern = &mut **unwrap_pat!(val3, Expr::Like { pattern, .. }, pattern);
                *pattern = TypesBuilder::highlight();
                TypesBuilder::build(generator, pattern, TypeAssertion::GeneratedBy(SubgraphType::Text));
            },
            "BinaryBooleanOpV3" => {
                let binary_bool_op = match_next_state!(generator, {
                    "BinaryBooleanOpV3AND" => BinaryOperator::And,
                    "BinaryBooleanOpV3OR" => BinaryOperator::Or,
                });
                *val3 = Expr::BinaryOp {
                    left: Box::new(TypesBuilder::highlight()),
                    op: binary_bool_op,
                    right: Box::new(TypesBuilder::nothing())
                };

                generator.expect_state("call27_types");
                let left = &mut **unwrap_pat!(val3, Expr::BinaryOp { left, .. }, left);
                TypesBuilder::build(generator, left, TypeAssertion::GeneratedBy(SubgraphType::Val3));

                generator.expect_state("call28_types");
                let right = &mut **unwrap_pat!(val3, Expr::BinaryOp { right, .. }, right);
                *right = TypesBuilder::highlight();
                TypesBuilder::build(generator, right, TypeAssertion::GeneratedBy(SubgraphType::Val3));
            },
            "UnaryNot_VAL_3" => {
                generator.expect_state("call30_types");
                *val3 = Expr::UnaryOp {
                    op: UnaryOperator::Not,
                    expr: Box::new(TypesBuilder::highlight())
                };
                let expr = &mut **unwrap_pat!(val3, Expr::UnaryOp { expr, .. }, expr);
                TypesBuilder::build(generator, expr, TypeAssertion::GeneratedBy(SubgraphType::Val3));
            },
        });
        generator.expect_state("EXIT_VAL_3");
        SubgraphType::Val3
    }
}
