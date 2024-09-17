use std::{io::Write, path::PathBuf, str::FromStr};

use sqlparser::{ast::{Query, Statement}, dialect::PostgreSqlDialect, parser::Parser};

use crate::{config::{Config, MainConfig, TomlReadable}, query_creation::{query_generator::{query_info::DatabaseSchema, value_choosers::DeterministicValueChooser, QueryGenerator}, state_generator::{markov_chain_generator::{error::SyntaxError, markov_chain::MarkovChain}, state_choosers::MaxProbStateChooser, substitute_models::PathModel, MarkovChainGenerator}}};

use super::{ast_to_path::{ConvertionError, PathGenerator}, models::PathwayGraphModel};

pub struct SQLTrainer {
    pub config: TrainingConfig,
    main_config: MainConfig,
    dataset_queries: Vec<Box<Query>>,
    path_generator: PathGenerator,
    path_query_generator: QueryGenerator<MaxProbStateChooser>,
}

#[derive(Debug, Clone)]
pub struct TrainingConfig {
    pub training_db_path: PathBuf,
    pub training_schema: PathBuf,
    pub save_weights_to: PathBuf,
}

impl TomlReadable for TrainingConfig {
    fn from_toml(toml_config: &toml::Value) -> Self {
        let section = &toml_config["training"];
        Self {
            training_db_path: PathBuf::from_str(section["training_db_path"].as_str().unwrap()).unwrap(),
            training_schema: PathBuf::from_str(section["training_schema"].as_str().unwrap()).unwrap(),
            save_weights_to: PathBuf::from_str(section["save_weights_to"].as_str().unwrap()).unwrap(),
        }
    }
}

impl SQLTrainer {
    pub fn with_config(config: &Config) -> Result<Self, SyntaxError> {
        let dataset = std::fs::read_to_string(config.training_config.training_db_path.clone()).unwrap();
        Ok(SQLTrainer {
            dataset_queries: Parser::parse_sql(&PostgreSqlDialect {}, &dataset).unwrap().into_iter()
                .filter_map(|statement| if let Statement::Query(query) = statement {
                    Some(query)
                } else { None })
                .collect(),
            path_generator: PathGenerator::new(
                DatabaseSchema::parse_schema(&config.training_config.training_schema),
                &config.chain_config,
                config.generator_config.aggregate_functions_distribution.clone(),
            )?,
            path_query_generator: QueryGenerator::from_state_generator_and_config(
                MarkovChainGenerator::with_config(&config.chain_config).unwrap(),
                config.generator_config.clone(),
                Box::new(PathModel::empty()),
            ),
            config: config.training_config.clone(),
            main_config: config.main_config.clone(),
        })
    }

    pub fn markov_chain_ref(&self) -> &MarkovChain {
        self.path_generator.markov_chain_ref()
    }

    /// Currently performs full-batch training (the whole dataset is a single batch)
    /// TODO: add setting for mini-batch training (not needed as of now)
    pub fn train(&mut self, mut model: Box<dyn PathwayGraphModel>) -> Result<Box<dyn PathwayGraphModel>, ConvertionError> {
        eprintln!("Training model... ");
        model.start_epoch();
        model.start_batch();
        for (i, query) in self.dataset_queries.iter().enumerate() {
            let path = self.path_generator.get_query_path(query)?;
            model.start_episode(&path);
            model = self.path_query_generator.generate_and_feed_to_model(
                Box::new(PathModel::from_path_nodes(&path)),
                Box::new(DeterministicValueChooser::from_path_nodes(&path)),
                model
            );
            model.end_episode();
            if self.main_config.print_progress {
                if i % 1 == 0 {
                    eprint!("{}/{}      \r", i, self.dataset_queries.len());
                    std::io::stderr().flush().unwrap();
                }
            }
        }
        model.end_batch();
        model.update_weights();
        model.end_epoch();
        eprintln!();
        Ok(model)
    }
}
