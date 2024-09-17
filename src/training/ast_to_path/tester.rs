use std::io::Write;

use crate::{config::{Config, MainConfig}, query_creation::{query_generator::{query_info::DatabaseSchema, value_choosers::DeterministicValueChooser, QueryGenerator}, state_generator::{markov_chain_generator::error::SyntaxError, state_choosers::{MaxProbStateChooser, ProbabilisticStateChooser}, substitute_models::{AntiCallModel, PathModel}, MarkovChainGenerator}}};

use super::{AST2PathTestingConfig, ConvertionError, PathGenerator};

pub struct TestAST2Path {
    config: AST2PathTestingConfig,
    main_config: MainConfig,
    path_generator: PathGenerator,
    random_query_generator: QueryGenerator<ProbabilisticStateChooser>,
    path_query_generator: QueryGenerator<MaxProbStateChooser>,
}

impl TestAST2Path {
    pub fn with_config(config: Config) -> Result<Self, SyntaxError> {
        Ok(Self {
            path_generator: PathGenerator::new(
                DatabaseSchema::parse_schema(&config.ast2path_testing_config.schema),
                &config.chain_config,
                config.generator_config.aggregate_functions_distribution.clone(),
            )?,
            config: config.ast2path_testing_config,
            main_config: config.main_config,
            random_query_generator: QueryGenerator::from_state_generator_and_config(
                MarkovChainGenerator::with_config(&config.chain_config).unwrap(),
                config.generator_config.clone(),
                Box::new(AntiCallModel::new(config.anticall_model_config)),
            ),
            path_query_generator: QueryGenerator::from_state_generator_and_config(
                MarkovChainGenerator::with_config(&config.chain_config).unwrap(),
                config.generator_config,
                Box::new(PathModel::empty())
            ),
        })
    }

    pub fn test(&mut self) -> Result<(), ConvertionError> {
        for i in 0..self.config.n_tests {
            let query = Box::new(self.random_query_generator.generate());
            if i % 10 == 0 {
                if self.main_config.print_progress {
                    print!("{}/{}      \r", i, self.config.n_tests);
                }
                std::io::stdout().flush().unwrap();
            }
            // if i < 490 { continue; }
            // eprintln!("Tested query: {query}");
            let path = self.path_generator.get_query_path(&query)?;
            let generated_query = self.path_query_generator.generate_with_substitute_model_and_value_chooser(
                Box::new(PathModel::from_path_nodes(&path)),
                Box::new(DeterministicValueChooser::from_path_nodes(&path))
            );
            if *query != generated_query {
                eprintln!("\nAST -> path -> AST mismatch!\nOriginal  query: {}\nGenerated query: {}", query, generated_query);
                eprintln!("Path: {:?}", path);
            }
        }
        Ok(())
    }
}