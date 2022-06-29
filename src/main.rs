use equivalence_testing::query_creation::random_query_generator::{QueryGenerator, QueryGeneratorParams};

fn main() {
    let generator = QueryGenerator::new(QueryGeneratorParams::default());
    let query = generator.generate();
    println!("Generated query: {:#?}", query.to_string());
}
