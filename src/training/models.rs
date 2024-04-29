use std::{io, path::PathBuf, str::FromStr};

use sqlparser::ast::Query;

use crate::{config::TomlReadable, query_creation::{query_generator::value_choosers::QueryValueChooser, state_generator::markov_chain_generator::{markov_chain::NodeParams, ChainStateCheckpoint, StackFrame}}};

use self::{llm_prompting_models::{chatgpt_model::ChatGPTPromptingModel, prompt_testing_model::PromptTestingModel}, markov_models::{DepthwiseFullFunctionContext, DepthwiseFunctionNameContext, FullFunctionContext, FunctionNameContext, ModelWithMarkovWeights, PathwiseContext, StackedFullFunctionContext, StackedFunctionNamesContext}};

use super::ast_to_path::PathNode;

pub mod markov_models;
pub mod llm_prompting_models;

pub struct ModelConfig {
    /// Which model to use. Can be: "subgraph"
    pub model_name: String,
    /// whether to use pretrained model weights
    pub load_weights: bool,
    /// where to load the weights from
    pub load_weights_from: PathBuf,
    /// where to save a dot file with graph representation.
    /// type save_dot_file=false in config to turn off
    pub save_dot_file: Option<PathBuf>,
    /// whether to use the stacked version of the model
    pub stacked_version: bool,
    /// whether to print weights after training is done
    pub print_weights_after_training: bool,
}

impl TomlReadable for ModelConfig {
    fn from_toml(toml_config: &toml::Value) -> Self {
        let section = &toml_config["model"];
        Self {
            model_name: section["model_name"].as_str().unwrap().to_string(),
            load_weights: section["load_weights"].as_bool().unwrap(),
            load_weights_from: PathBuf::from_str(section["load_weights_from"].as_str().unwrap()).unwrap(),
            save_dot_file: section["save_dot_file"].as_bool().map_or_else(
                || Some(PathBuf::from_str(section["save_dot_file"].as_str().unwrap()).unwrap()),
                |x| if !x { None } else { panic!("save_dot_file can't be 'true'") },
            ),
            stacked_version: section["stacked_version"].as_bool().unwrap(),
            print_weights_after_training: section["print_weights_after_training"].as_bool().unwrap(),
        }
    }
}

impl ModelConfig {
    pub fn create_model(&self) -> io::Result<Box<dyn PathwayGraphModel>> {
        let mut model: Box<dyn PathwayGraphModel> = match (self.model_name.as_str(), self.stacked_version) {
            ("PromptTestingModel", _) => Box::new(PromptTestingModel::new()),
            ("ChatGPTPromptingModel", _) => Box::new(ChatGPTPromptingModel::new()),
            ("subgraph", false) => Box::new(ModelWithMarkovWeights::<FunctionNameContext>::new()),
            ("subgraph", true) => Box::new(ModelWithMarkovWeights::<StackedFunctionNamesContext>::new()),
            ("DepthwiseFunctionNameContext", false) => Box::new(ModelWithMarkovWeights::<DepthwiseFunctionNameContext>::new()),
            ("full_function_context", false) => Box::new(ModelWithMarkovWeights::<FullFunctionContext>::new()),
            ("full_function_context", true) => Box::new(ModelWithMarkovWeights::<StackedFullFunctionContext>::new()),
            ("depthwize_full_function_context", false) => Box::new(ModelWithMarkovWeights::<DepthwiseFullFunctionContext>::new()),
            ("pathwise_context", _) => Box::new(ModelWithMarkovWeights::<PathwiseContext>::new()),
            (any, st) => panic!("\nModel is not supported:\nModel name: {any}\nStacked: {st}\n"),
        };
        if self.load_weights {
            eprintln!("Loading weights from {}...", self.load_weights_from.display());
            model.load_weights(&self.load_weights_from)?;
        }
        if let Some(ref dot_file_path) = self.save_dot_file {
            eprintln!("Saving .dot file to {}...", dot_file_path.display());
            model.write_weights_to_dot(dot_file_path)?;
        }
        Ok(model)
    }
}

/// Returns None with a value if the prediction could not be made
pub enum ModelPredictionResult {
    Some(Vec<(f64, NodeParams)>),
    None(Vec<NodeParams>)
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

    /// Feed the new state and context (in the form of current call stack and current path) to the model.\
    /// popped_stack_frame is the last stack frame popped. It can be used after an exit node
    /// is emitted, as the call_stack will miss it.
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
    fn write_weights(&self, file_path: &PathBuf) -> io::Result<()>;

    /// read weights from file
    fn load_weights(&mut self, file_path: &PathBuf) -> io::Result<()>;

    /// print weights to stdout
    fn print_weights(&self) { unimplemented!() }

    /// initiate the inference process
    fn start_inference(&mut self, _schema_string: String) { }

    /// predict the probability distribution over the outgoing nodes that are available
    fn predict(&mut self, call_stack: &Vec<StackFrame>, node_outgoing: Vec<NodeParams>, current_query_ast_opt: Option<&Query>) -> ModelPredictionResult;

    /// end the inference process
    fn end_inference(&mut self) { }

    fn write_weights_to_dot(&self, _dot_file_path: &PathBuf) -> io::Result<()> { unimplemented!() }

    fn as_value_chooser(&mut self) -> Option<&mut dyn QueryValueChooser> { None }

    fn add_checkpoint(&mut self, _chain_state_checkpoint: ChainStateCheckpoint) { }

    fn try_get_backtracking_checkpoint(&mut self) -> Option<ChainStateCheckpoint> { None }
}
