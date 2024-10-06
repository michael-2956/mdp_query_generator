use sqlparser::ast::{Expr, Value};

use crate::{query_creation::{query_generator::{expr_precedence::ExpressionPriority, match_next_state, QueryGenerator}, state_generator::{markov_chain_generator::markov_chain::QueryTypes, state_choosers::StateChooser, subgraph_type::{ContainsSubgraphType, SubgraphType}, CallTypes}}, unwrap_variant};

use super::{aggregate_function::AggregateFunctionBuilder, case::CaseBuilder, column_spec::ColumnSpecBuilder, formulas::FormulasBuilder, literals::LiteralsBuilder, query::QueryBuilder, types::TypesBuilder};

#[derive(Debug, Clone)]
pub enum TypeAssertion<'a> {
    None,
    GeneratedBy(SubgraphType),
    CompatibleWith(SubgraphType),
    GeneratedByOneOf(&'a [SubgraphType]),
    CompatibleWithOneOf(&'a [SubgraphType]),
}

impl<'a> TypeAssertion<'a> {
    pub fn check(&'a self, selected_type: &SubgraphType, types_value: &Expr) {
        if !self.assertion_passed(selected_type) {
            panic!("types got an unexpected type: expected {:?}, got {:?}.\nExpr: {:?}", self, selected_type, types_value);
        }
    }

    pub fn check_type(&'a self, selected_type: &SubgraphType) {
        if !self.assertion_passed(selected_type) {
            panic!("types got an unexpected type: expected {:?}, got {:?}", self, selected_type);
        }
    }

    fn assertion_passed(&'a self, selected_type: &SubgraphType) -> bool {
        match self {
            TypeAssertion::None => true,
            TypeAssertion::GeneratedBy(by) => selected_type.is_same_or_more_determined_or_undetermined(by),
            TypeAssertion::CompatibleWith(with) => selected_type.is_compat_with(with),
            TypeAssertion::GeneratedByOneOf(by_one_of) => {
                by_one_of.contains_generator_of(selected_type)
            },
            TypeAssertion::CompatibleWithOneOf(with_one_of) => {
                with_one_of.iter().any(|with| selected_type.is_compat_with(with))
            },
        }
    }
}

/// subgraph def_types_value
pub struct TypesValueBuilder { }

impl TypesValueBuilder {
    pub fn highlight() -> Expr {
        TypesBuilder::highlight()
    }

    pub fn build<StC: StateChooser>(
        generator: &mut QueryGenerator<StC>, types_value: &mut Expr, type_assertion: TypeAssertion
    ) -> SubgraphType {
        generator.expect_state("types_value");
        let selected_types = unwrap_variant!(generator.state_generator.get_fn_selected_types_unwrapped(), CallTypes::TypeList);
        generator.state_generator.set_known_list(selected_types.clone());
        let selected_type = match_next_state!(generator, {
            "types_value_nested" => {
                generator.expect_state("call1_types_value");
                *types_value = Expr::Nested(Box::new(TypesValueBuilder::highlight()));
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
                    format: None
                };
                null_type
            },
            "call0_case" => CaseBuilder::build(generator, types_value),
            "call0_formulas" => FormulasBuilder::build(generator, types_value),
            "call0_literals" => LiteralsBuilder::build(generator, types_value),
            "call0_aggregate_function" => AggregateFunctionBuilder::build(generator, types_value),
            "column_type_available" => {
                generator.expect_state("call0_column_spec");
                ColumnSpecBuilder::build(generator, types_value)
            },
            "call1_Query" => {
                // known list is also set at this point but we ignore that
                generator.state_generator.set_known_query_type_list(QueryTypes::ColumnTypeLists {
                    column_type_lists: vec![selected_types.clone()]  // single column
                });
                *types_value = Expr::Subquery(Box::new(QueryBuilder::nothing()));
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
