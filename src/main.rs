use equivalence_testing::query_creation::random_query_generator::{
    QueryGenerator, QueryGeneratorParams,
};

fn main() {
    let mut generator = QueryGenerator::new(QueryGeneratorParams::default());
    for _ in 0..20 {
        let query = generator.generate();
        println!("Generated query: {:#?}", query.to_string());
    }
}
