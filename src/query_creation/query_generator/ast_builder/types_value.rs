use sqlparser::ast::{Expr, Value};

use crate::{query_creation::{query_generator::{expr_precedence::ExpressionPriority, match_next_state, value_choosers::QueryValueChooser, QueryGenerator, TypeAssertion}, state_generator::{state_choosers::StateChooser, subgraph_type::SubgraphType, substitute_models::SubstituteModel, CallTypes}}, unwrap_variant};

use super::{aggregate_function::AggregateFunctionBuilder, query::QueryBuilder, types::TypesBuilder};

/// subgraph def_types_value
pub struct TypesValueBuilder { }

impl TypesValueBuilder {
    pub fn empty() -> Expr {
        TypesBuilder::empty()
    }

    pub fn build<SubMod: SubstituteModel, StC: StateChooser, QVC: QueryValueChooser>(
        generator: &mut QueryGenerator<SubMod, StC, QVC>, types_value: &mut Expr, type_assertion: TypeAssertion
    ) -> SubgraphType {
        generator.expect_state("types_value");
        let selected_types = unwrap_variant!(generator.state_generator.get_fn_selected_types_unwrapped(), CallTypes::TypeList);
        generator.state_generator.set_known_list(selected_types.clone());
        let selected_type = match_next_state!(generator, {
            "types_value_nested" => {
                generator.expect_state("call1_types_value");
                *types_value = Expr::Nested(Box::new(TypesValueBuilder::empty()));
                let expr = &mut **unwrap_variant!(types_value, Expr::Nested);
                TypesValueBuilder::build(generator, expr, TypeAssertion::None)
            },
            "types_value_null" => {
                *types_value = Expr::Value(Value::Null);
                SubgraphType::Undetermined
            },
            "types_value_typed_null" => {
                let null_type = {
                    let types_without_inner = selected_types.into_iter()
                        .filter(|x| !x.has_inner()).collect::<Vec<_>>();
                    match types_without_inner.as_slice() {
                        [tp] => tp.clone(),
                        any => panic!("allowed_type_list must have single element here (got {:?})", any)
                    }
                };
                *types_value = Expr::Cast {
                    expr: Box::new(Expr::Value(Value::Null)),
                    data_type: null_type.clone().to_data_type(),
                };
                null_type
            },
            "call0_case" => {
                let (tp, expr) = generator.handle_case();
                *types_value = expr;
                tp
            },
            "call0_formulas" => {
                let (tp, expr) = generator.handle_formulas();
                *types_value = expr;
                tp
            },
            "call0_literals" => {
                let (tp, expr) = generator.handle_literals();
                *types_value = expr;
                tp
            },
            "call0_aggregate_function" => AggregateFunctionBuilder::build(generator, types_value),
            "column_type_available" => {
                generator.expect_state("call0_column_spec");
                let (tp, expr) = generator.handle_column_spec();
                *types_value = expr;
                tp
            },
            "call1_Query" => {
                *types_value = Expr::Subquery(Box::new(QueryBuilder::empty()));
                let subquery = &mut **unwrap_variant!(types_value, Expr::Subquery);
                let column_types = QueryBuilder::build(generator, subquery);
                let selected_type = match column_types.len() {
                    1 => column_types.into_iter().next().unwrap().1,
                    any => panic!(
                        "Subquery should have selected a single column, \
                        but selected {any} columns. Subquery: {subquery}"
                    ),
                };
                selected_type
            },
        });
        type_assertion.check(&selected_type, &types_value);
        generator.expect_state("EXIT_types_value");

        // avoid cloning
        let mut p = Expr::Value(Value::Null); std::mem::swap(types_value, &mut p);
        *types_value = p.nest_children_if_needed();

        selected_type
    }
}
