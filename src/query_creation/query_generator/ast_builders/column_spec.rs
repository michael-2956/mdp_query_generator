use sqlparser::ast::Expr;

use crate::{query_creation::{query_generator::{match_next_state, query_info::{CheckAccessibility, ColumnRetrievalOptions, IdentName}, value_chooser, QueryGenerator}, state_generator::{state_choosers::StateChooser, subgraph_type::SubgraphType, CallTypes}}, unwrap_variant_or_else};

use super::types::TypesBuilder;

/// subgraph def_column_spec
pub struct ColumnSpecBuilder { }

impl ColumnSpecBuilder {
    pub fn highlight() -> Expr {
        TypesBuilder::highlight()
    }

    pub fn build<StC: StateChooser>(
        generator: &mut QueryGenerator<StC>, ident_expr: &mut Expr
    ) -> SubgraphType {
        generator.expect_state("column_spec");
        let column_types = unwrap_variant_or_else!(
            generator.state_generator.get_fn_selected_types_unwrapped(), CallTypes::TypeList, || generator.state_generator.print_stack()
        );
        generator.expect_state("column_spec_mentioned_in_group_by");
        let only_group_by_columns = match_next_state!(generator, {
            "column_spec_mentioned_in_group_by_yes" => true,
            "column_spec_mentioned_in_group_by_no" => false,
        });
        generator.expect_state("column_spec_shaded_by_select");
        let shade_by_select_aliases = match_next_state!(generator, {
            "column_spec_shaded_by_select_yes" => true,
            "column_spec_shaded_by_select_no" => false,
        });
        generator.expect_state("column_spec_aggregatable_columns");
        let only_columns_that_can_be_aggregated = match_next_state!(generator, {
            "column_spec_aggregatable_columns_yes" => true,
            "column_spec_aggregatable_columns_no" => false,
        });
        let column_retrieval_options = ColumnRetrievalOptions::new(
            only_group_by_columns, shade_by_select_aliases, only_columns_that_can_be_aggregated
        );
        generator.expect_state("column_spec_choose_qualified");
        let check_accessibility = match_next_state!(generator, {
            "qualified_column_name" => CheckAccessibility::QualifiedColumnName,
            "unqualified_column_name" => CheckAccessibility::ColumnName,
        });
        let (selected_type, qualified_column_name) = value_chooser!(generator).choose_column(
            &generator.clause_context, column_types, check_accessibility.clone(), column_retrieval_options
        );
        *ident_expr = if check_accessibility == CheckAccessibility::QualifiedColumnName {
            Expr::CompoundIdentifier(qualified_column_name.into_iter().map(IdentName::into).collect())
        } else {
            Expr::Identifier(qualified_column_name.last().unwrap().clone().into())
        };
        generator.expect_state("EXIT_column_spec");
        selected_type
    }
}
