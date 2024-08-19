use sqlparser::ast::{self, Expr, FunctionArg, FunctionArgExpr, ObjectName};

use crate::{query_creation::{query_generator::{aggregate_function_settings::AggregateFunctionAgruments, highlight_ident, match_next_state, value_chooser, QueryGenerator}, state_generator::{state_choosers::StateChooser, subgraph_type::SubgraphType}}, unwrap_pat};

use super::{types::TypesBuilder, types_value::TypeAssertion};

fn unnamed_arg(expr: Expr) -> FunctionArg {
    FunctionArg::Unnamed(FunctionArgExpr::Expr(expr))
}

fn get_last_expr_mut(arg_v: &mut Vec<FunctionArg>) -> &mut Expr {
    unwrap_pat!(arg_v.last_mut().unwrap(), FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)), expr)
}

/// subgraph def_aggregate_function
pub struct AggregateFunctionBuilder { }

impl AggregateFunctionBuilder {
    pub fn highlight() -> Expr {
        TypesBuilder::highlight()
    }

    pub fn build<StC: StateChooser>(
        generator: &mut QueryGenerator<StC>, expr: &mut Expr
    ) -> SubgraphType {
        generator.expect_state("aggregate_function");

        *expr = Expr::Function(ast::Function {
            name: ObjectName(vec![]),
            args: vec![],
            over: None,
            distinct: false,
            special: false,
            filter: None,
            null_treatment: None,
            order_by: vec![],
        });

        // if any aggregate function is present in query, aggregation was succesfully indicated.
        generator.clause_context.query_mut().set_aggregation_indicated();

        let distinct = unwrap_pat!(expr, Expr::Function(ast::Function{ distinct, .. }), distinct);
        *distinct = match_next_state!(generator, {
            "aggregate_not_distinct" => false,
            "aggregate_distinct" => true,
        });

        generator.expect_state("aggregate_select_return_type");

        let args = unwrap_pat!(expr, Expr::Function(ast::Function{ args, .. }), args);
        *args = vec![unnamed_arg(TypesBuilder::highlight())];

        let (
            args_type, return_type
        ): (AggregateFunctionAgruments, SubgraphType) = match_next_state!(generator, {
            "aggregate_select_type_bigint" => {
                let return_type = SubgraphType::BigInt;
                let args_type = match_next_state!(generator, {
                    "arg_star" => {
                        *args = vec![FunctionArg::Unnamed(FunctionArgExpr::Wildcard)];
                        AggregateFunctionAgruments::Wildcard
                    },
                    "arg_bigint_any" => {
                        generator.expect_state("call65_types");
                        TypesBuilder::build(generator, get_last_expr_mut(args), TypeAssertion::None);
                        AggregateFunctionAgruments::AnyType
                    },
                    "arg_bigint" => {
                        generator.expect_state("call75_types");
                        generator.state_generator.set_compatible_list(SubgraphType::BigInt.get_compat_types());
                        TypesBuilder::build(generator, get_last_expr_mut(args), TypeAssertion::CompatibleWith(SubgraphType::BigInt));
                        AggregateFunctionAgruments::TypeList(vec![SubgraphType::BigInt])
                    },
                });
                (args_type, return_type)
            },
            arm @ ("aggregate_select_type_numeric" | "aggregate_select_type_text") => {
                let (return_type, states) = match arm {
                    "aggregate_select_type_text" => (SubgraphType::Text, &["arg_double_text", "call74_types", "arg_single_text", "call63_types"]),
                    "aggregate_select_type_numeric" => (SubgraphType::Numeric, &["arg_double_numeric", "call68_types", "arg_single_numeric", "call66_types"]),
                    any => generator.panic_unexpected(any),
                };

                let mut args_type_v = vec![];
                generator.state_generator.set_compatible_list(return_type.get_compat_types());

                match_next_state!(generator, {
                    arm if arm == states[0] => {
                        generator.expect_state(states[1]);
                        TypesBuilder::build(generator, get_last_expr_mut(args), TypeAssertion::CompatibleWith(return_type.clone()));
                        args.push(unnamed_arg(TypesBuilder::highlight())); // add the placeholder for the next argument
                        args_type_v.push(return_type.clone());
                    },
                    arm if arm == states[2] => { },
                });

                generator.expect_state(states[3]);
                TypesBuilder::build(generator, get_last_expr_mut(args), TypeAssertion::CompatibleWith(return_type.clone()));
                args_type_v.push(return_type.clone());

                let args_type = AggregateFunctionAgruments::TypeList(args_type_v);
                (args_type, return_type)
            },
            arm @ ("aggregate_select_type_bool" | "aggregate_select_type_date" | "aggregate_select_type_timestamp" | "aggregate_select_type_interval" | "aggregate_select_type_integer") => {
                let (return_type, states) = match arm {
                    "aggregate_select_type_bool" => (SubgraphType::Val3, &["arg_single_3vl", "call64_types"]),
                    "aggregate_select_type_date" => (SubgraphType::Date, &["arg_date", "call72_types"]),
                    "aggregate_select_type_timestamp" => (SubgraphType::Timestamp, &["arg_timestamp", "call96_types"]),
                    "aggregate_select_type_interval" => (SubgraphType::Interval, &["arg_interval", "call90_types"]),
                    "aggregate_select_type_integer" => (SubgraphType::Integer, &["arg_integer", "call71_types"]),
                    any => generator.panic_unexpected(any),
                };

                generator.expect_states(states);
                generator.state_generator.set_compatible_list(return_type.get_compat_types());
                TypesBuilder::build(generator, get_last_expr_mut(args), TypeAssertion::CompatibleWith(return_type.clone()));

                (AggregateFunctionAgruments::TypeList(vec![return_type.clone()]), return_type)
            },
        });

        let (func_names, dist) = generator.config.aggregate_functions_distribution.get_functions_and_dist(&args_type, &return_type);
        let name = unwrap_pat!(expr, Expr::Function(ast::Function{ name, .. }), name);
        *name = ObjectName(vec![highlight_ident()]);
        *name = value_chooser!(generator).choose_aggregate_function_name(func_names, dist);

        generator.expect_state("EXIT_aggregate_function");
        return_type
    }
}
