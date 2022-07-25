use std::path::PathBuf;

use equivalence_testing::query_creation::{
    random_query_generator::{QueryGenerator, QueryGeneratorParams},
    state_generators::MarkovChainGenerator,
};
use structopt::StructOpt;

#[derive(StructOpt, Debug)]
#[structopt(name = "basic")]
struct ProgramArgs {
    /// Path to graph source for a markov chain
    #[structopt(parse(from_os_str))]
    input: PathBuf,
}

fn main() {
    let program_args = ProgramArgs::from_args();
    let markov_generator = match MarkovChainGenerator::parse_graph_from_file(program_args.input) {
        Ok(generator) => generator,
        Err(err) => {
            println!("{}", err);
            return;
        }
    };
    let mut generator = QueryGenerator::new(QueryGeneratorParams::from_generator(markov_generator));

    for _ in 0..2 {
        let query = generator.generate();
        println!("Generated query: {:#?}", query.to_string());
    }
}
