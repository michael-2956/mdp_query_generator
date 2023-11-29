use std::{path::PathBuf, str::FromStr, io::Write};

use sqlparser::{parser::Parser, dialect::PostgreSqlDialect, ast::{Statement, Query}};

use crate::{config::{TomlReadable, Config, MainConfig}, query_creation::{state_generator::markov_chain_generator::{error::SyntaxError, markov_chain::MarkovChain, StackFrame}, query_generator::query_info::DatabaseSchema}};

use super::{ast_to_path::{PathNode, ConvertionError, PathGenerator}, markov_weights::MarkovWeights};

pub struct SQLTrainer {
    _config: TrainingConfig,
    main_config: MainConfig,
    dataset_queries: Vec<Box<Query>>,
    path_generator: PathGenerator,
}

pub struct TrainingConfig {
    pub training_db_path: PathBuf,
    pub training_schema: PathBuf,
}

impl TomlReadable for TrainingConfig {
    fn from_toml(toml_config: &toml::Value) -> Self {
        let section = &toml_config["training"];
        Self {
            training_db_path: PathBuf::from_str(section["training_db_path"].as_str().unwrap()).unwrap(),
            training_schema: PathBuf::from_str(section["training_schema"].as_str().unwrap()).unwrap(),
        }
    }
}

impl SQLTrainer {
    pub fn with_config(config: Config) -> Result<Self, SyntaxError> {
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
            )?,
            _config: config.training_config,
            main_config: config.main_config,
        })
    }

    pub fn markov_chain_ref(&self) -> &MarkovChain {
        self.path_generator.markov_chain_ref()
    }

    /// Currently performs full-batch training (the whole dataset is a single batch)
    /// TODO: add setting for mini-batch training (not needed as of now)
    pub fn train(&mut self, mut model: Box<dyn PathwayGraphModel>) -> Result<Box<dyn PathwayGraphModel>, ConvertionError> {
        println!("Training model... ");
        model.start_epoch();
        model.start_batch();
        for (i, query) in self.dataset_queries.iter().enumerate() {
            model = self.path_generator.feed_query_to_model(query, model)?;
            if self.main_config.print_progress {
                if i % 50 == 0 {
                    print!("{}/{}      \r", i, self.dataset_queries.len());
                    std::io::stdout().flush().unwrap();
                }
            }
        }
        model.end_batch();
        model.update_weights();
        model.end_epoch();
        println!();
        Ok(model)
    }
}

pub trait PathwayGraphModel {
    /// initialize the model weights
    fn init_weights(&mut self) { }

    /// prepare model for the new epoch
    fn start_epoch(&mut self) { }

    /// prepare model for the start of an epoch
    fn start_batch(&mut self) { }

    /// prepare model for a training episode
    fn start_episode(&mut self) { }

    /// feed the new state and context (in the form of current call stack and current path) to the model
    fn process_state(&mut self, current_path: &Vec<PathNode>, call_stack: &Vec<StackFrame>);

    /// end the training episode and accumulate the update/gradient
    fn end_episode(&mut self) { }

    /// end the current batch
    fn end_batch(&mut self) { }

    /// update the weights with the accumulated update/gradient
    fn update_weights(&mut self) { }

    /// end the current epoch
    fn end_epoch(&mut self) { }
}

pub struct SubgraphMarkovModel {
    weights: MarkovWeights,
    weights_ready: bool,
}

impl SubgraphMarkovModel {
    /// create model from with the given graph structure
    pub fn new(markov_chain: &MarkovChain) -> Self {
        Self {
            weights: MarkovWeights::new(markov_chain),
            weights_ready: false,
        }
    }
}

impl PathwayGraphModel for SubgraphMarkovModel {
    fn start_epoch(&mut self) {
        if self.weights_ready == true {
            panic!("SubgraphMarkovModel does not allow multiple epochs.")
        }
        self.weights.fill_probs_zero();
        self.weights_ready = false;
    }

    fn process_state(&mut self, _current_path: &Vec<PathNode>, _call_stack: &Vec<StackFrame>) {
        todo!()
    }

    fn update_weights(&mut self) {
        self.weights_ready = true;
        self.weights.normalize();
    }
}
