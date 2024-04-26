use crate::query_creation::{query_generator::{match_next_state, QueryGenerator}, state_generator::{state_choosers::StateChooser, subgraph_type::SubgraphType}};

/// subgraph def_types_type
pub struct TypesTypeBuilder { }

impl TypesTypeBuilder {
    pub fn build<StC: StateChooser>(
        generator: &mut QueryGenerator<StC>
    ) -> SubgraphType {
        generator.expect_state("types_type");

        let tp = match_next_state!(generator, {
            "types_type_bigint" => SubgraphType::BigInt,
            "types_type_integer" => SubgraphType::Integer,
            "types_type_numeric" => SubgraphType::Numeric,
            "types_type_3vl" => SubgraphType::Val3,
            "types_type_text" => SubgraphType::Text,
            "types_type_date" => SubgraphType::Date,
            "types_type_interval" => SubgraphType::Interval,
            "types_type_timestamp" => SubgraphType::Timestamp,
        });

        generator.expect_state("EXIT_types_type");

        tp
    }
}
