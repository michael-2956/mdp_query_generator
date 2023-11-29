use std::{path::PathBuf, str::FromStr, io::{Write, self}};

use smol_str::SmolStr;
use sqlparser::{parser::Parser, dialect::PostgreSqlDialect, ast::{Statement, Query}};

use crate::{config::{TomlReadable, Config, MainConfig}, query_creation::{state_generator::{markov_chain_generator::{error::SyntaxError, markov_chain::MarkovChain, StackFrame}, dynamic_models::PathModel, state_choosers::MaxProbStateChooser, MarkovChainGenerator}, query_generator::{query_info::DatabaseSchema, QueryGenerator, value_choosers::DeterministicValueChooser}}};

use super::{ast_to_path::{PathNode, ConvertionError, PathGenerator}, markov_weights::MarkovWeights};

pub struct SQLTrainer {
    _config: TrainingConfig,
    main_config: MainConfig,
    dataset_queries: Vec<Box<Query>>,
    path_generator: PathGenerator,
    path_query_generator: QueryGenerator<PathModel, MaxProbStateChooser, DeterministicValueChooser>,
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
            path_query_generator: QueryGenerator::from_state_generator_and_schema(
                MarkovChainGenerator::with_config(&config.chain_config).unwrap(),
                config.generator_config,
            ),
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
            let path = self.path_generator.get_query_path(query)?;
            model.start_episode(&path);
            model = self.path_query_generator.generate_and_feed_to_model(
                Box::new(PathModel::from_path_nodes(&path)),
                Box::new(DeterministicValueChooser::from_path_nodes(&path)),
                model
            );
            model.end_episode();
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

    /// Prepare model for a training episode.
    /// - Provide full episode path beforehand, so that literal values\
    ///   can be obtained if and when that's needed.
    /// - It's not recommended to use that path for training,\
    ///   instead the process_state method should be used for that\
    ///   purpose, since it provides wider context.
    /// - The provided path is intended to only be used for obtaining the\
    ///   inserted literals.
    fn start_episode(&mut self, _path: &Vec<PathNode>) { }

    /// feed the new state and context (in the form of current call stack and current path) to the model
    fn process_state(&mut self, call_stack: &Vec<StackFrame>, popped_stack_frame: Option<&StackFrame>);

    /// end the training episode and accumulate the update/gradient
    fn end_episode(&mut self) { }

    /// end the current batch
    fn end_batch(&mut self) { }

    /// update the weights with the accumulated update/gradient
    fn update_weights(&mut self) { }

    /// end the current epoch
    fn end_epoch(&mut self) { }

    /// write weights to file
    fn write_to_file(&self, file_path: &str) -> io::Result<()>;

    /// read weights from file
    fn read_from_file(&mut self, file_path: &str) -> io::Result<()>;
}

#[derive(Debug)]
pub struct SubgraphMarkovModel {
    weights: MarkovWeights,
    weights_ready: bool,
    last_state_stack: Vec<SmolStr>,
}

impl SubgraphMarkovModel {
    /// create model from with the given graph structure
    pub fn new(markov_chain: &MarkovChain) -> Self {
        Self {
            weights: MarkovWeights::new(markov_chain),
            weights_ready: false,
            last_state_stack: vec![],
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

    fn process_state(&mut self, call_stack: &Vec<StackFrame>, popped_stack_frame: Option<&StackFrame>) {
        if self.last_state_stack.len() < call_stack.len() {
            let func_name = call_stack.last().unwrap().function_context.call_params.func_name.clone();
            self.last_state_stack.push(func_name);
            // println!("FUNCTION {func_name}");
        } else {
            let is_exit_node = self.last_state_stack.len() > call_stack.len();
            if let Some(last_state) = self.last_state_stack.last_mut() {
                let stack_frame = if is_exit_node {
                    popped_stack_frame.unwrap()
                } else {
                    call_stack.last().unwrap()
                };
                let current_state = stack_frame.function_context.current_node.node_common.name.clone();
                // println!("NODE {current_state}");
                let func_name = &stack_frame.function_context.call_params.func_name;
                self.weights.add_edge(func_name, last_state, &current_state);
                // println!("EDGE {last_state} -> {current_state}");
                *last_state = current_state;
            } else {
                panic!("No last state available, but received call stack: {:?}", call_stack)
            }
            if is_exit_node {
                // println!("POPPING...");
                self.last_state_stack.pop();
            }
        }
        // println!("SELF.last_state_stack: {:#?}", self.last_state_stack);
    }

    fn update_weights(&mut self) {
        self.weights_ready = true;
        self.weights.normalize();
        self.weights.print_outgoing_weights(&SmolStr::new("Query"), &SmolStr::new("call0_FROM"));
        println!("{:?}", 3);
    }

    fn write_to_file(&self, file_path: &str) -> io::Result<()> {
        self.weights.write_to_file(file_path)
    }

    fn read_from_file(&mut self, file_path: &str) -> io::Result<()> {
        self.weights = MarkovWeights::read_from_file(file_path)?;
        Ok(())
    }
}
