use std::{io, path::PathBuf, str::FromStr, collections::HashMap, fmt::Display};

use serde::{Serialize, Deserialize};
use smol_str::SmolStr;

use crate::{query_creation::state_generator::markov_chain_generator::{StackFrame, markov_chain::NodeParams}, config::TomlReadable};

use super::{ast_to_path::PathNode, markov_weights::MarkovWeights};

pub struct ModelConfig {
    // Which model to use. Can be: "subgraph"
    pub model_name: String,
    // whether to use pretrained model weights
    pub load_weights: bool,
    // where to load the weights from
    pub load_weights_from: PathBuf,
    // where to save a dot file with graph representation.
    // type save_dot_file=false in config to turn off
    pub save_dot_file: Option<PathBuf>,
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
            )
        }
    }
}

impl ModelConfig {
    pub fn create_model(&self) -> io::Result<Box<dyn PathwayGraphModel>> {
        if self.model_name == "subgraph" {
            let mut model = Box::new(SubgraphMarkovModel::new());
            if self.load_weights {
                println!("Loading weights from {}...", self.load_weights_from.display());
                model.load_weights(&self.load_weights_from)?;
            }
            if let Some(ref dot_file_path) = self.save_dot_file {
                println!("Saving .dot file to {}...", dot_file_path.display());
                model.write_weights_to_dot(dot_file_path)?;
            }
            Ok(model)
        } else {
            panic!("No such model name: {}", self.model_name);
        }
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
    fn print_weights(&self) { todo!() }

    /// initiate the inference process
    fn start_inference(&mut self) { }

    /// predict the probability distribution over the outgoing nodes that are available
    fn predict(&mut self, call_stack: &Vec<StackFrame>, node_outgoing: Vec<NodeParams>) -> Vec<(f64, NodeParams)>;

    /// end the inference process
    fn end_inference(&mut self) { }

    fn write_weights_to_dot(&self, _dot_file_path: &PathBuf) -> io::Result<()> { todo!() }
}

pub trait ModelWithMarkovWeights {
    type FuncType: std::fmt::Debug + Eq + std::hash::Hash + Clone + Serialize + for<'a> Deserialize<'a> + Display;

    fn get_weights_ref(&self) -> &MarkovWeights<HashMap<Self::FuncType, HashMap<SmolStr, HashMap<SmolStr, f64>>>>;

    fn get_weights_mut_ref(&mut self) -> &mut MarkovWeights<HashMap<Self::FuncType, HashMap<SmolStr, HashMap<SmolStr, f64>>>>;

    fn get_weights_ready_mut_ref(&mut self) -> &mut bool;

    fn get_last_state_stack_mut_ref(&mut self) -> &mut Vec<SmolStr>;

    /// the exit_stack_frame_opt is Some only after an exit node was emitted,
    fn get_function_name<'a>(&self, call_stack: &'a Vec<StackFrame>, exit_stack_frame_opt: Option<&StackFrame>) -> &'a Self::FuncType;
}

impl<T> PathwayGraphModel for T
where
    T: ModelWithMarkovWeights
{
    fn start_epoch(&mut self) {
        if *self.get_weights_ready_mut_ref() {
            panic!("SubgraphMarkovModel does not allow multiple epochs.")
        }
    }

    fn process_state(&mut self, call_stack: &Vec<StackFrame>, popped_stack_frame: Option<&StackFrame>) {
        if self.get_last_state_stack_mut_ref().len() < call_stack.len() {
            let func_name = call_stack.last().unwrap().function_context.call_params.func_name.clone();
            self.get_last_state_stack_mut_ref().push(func_name);
        } else {
            let is_exit_node = self.get_last_state_stack_mut_ref().len() > call_stack.len();
            let stack_frame = if is_exit_node {
                popped_stack_frame.unwrap()
            } else {
                call_stack.last().unwrap()
            };
            let (last_state, current_state) = if let Some(last_state) = self.get_last_state_stack_mut_ref().last_mut() {
                let current_state = stack_frame.function_context.current_node.node_common.name.clone();
                let c = last_state.clone();
                *last_state = current_state;
                (c, last_state.clone())
            } else {
                panic!("No last state available, but received call stack: {:?}", call_stack)
            };
            // let (a, b) = (call_stack, if is_exit_node { Some(stack_frame) } else { None });
            let func_name = self.get_function_name(call_stack, if is_exit_node { Some(stack_frame) } else { None });
            self.get_weights_mut_ref().insert_edge(func_name, &last_state, &current_state);
            if is_exit_node {
                self.get_last_state_stack_mut_ref().pop();
            }
        }
        // println!("SELF.last_state_stack: {:#?}", self.last_state_stack);
    }

    fn update_weights(&mut self) {
        *self.get_weights_ready_mut_ref() = true;
        self.get_weights_mut_ref().normalize();
    }

    fn write_weights(&self, file_path: &PathBuf) -> io::Result<()> {
        self.get_weights_ref().write_to_file(file_path)
    }

    fn load_weights(&mut self, file_path: &PathBuf) -> io::Result<()> {
        *self.get_weights_mut_ref() = MarkovWeights::load(file_path)?;
        *self.get_weights_ready_mut_ref() = true;
        Ok(())
    }

    fn print_weights(&self) {
        self.get_weights_ref().print();
    }

    fn predict(&mut self, call_stack: &Vec<StackFrame>, node_outgoing: Vec<NodeParams>) -> Vec<(f64, NodeParams)> {
        let context = &call_stack.last().unwrap().function_context;
        let func_name = self.get_function_name(call_stack, None);
        let current_node = &context.current_node.node_common.name;
        let outgoing_weights = self.get_weights_mut_ref().get_outgoing_weights_opt(func_name, current_node);
        let output = if let Some(outgoing_weights) = outgoing_weights {
            // obtain weights
            let mut output: Vec<_> = node_outgoing.iter().map(|node| (
                *outgoing_weights.get(&node.node_common.name).unwrap(), node
            )).collect();
            // normalize them
            let weight_sum: f64 = output.iter().map(|(w, _)| *w).sum();
            // if the weight sum is 0, the model was not trained in this context
            if weight_sum != 0f64 {
                for (w, _) in output.iter_mut() { *w /= weight_sum; }
                Some(output.into_iter().map(|(w, node)| (
                    w, node.clone()
                )).collect())
            } else { None }
        } else { None };
        output.unwrap_or_else(|| {
            // The model is undertrained for this context and cannot decide.
            // Basically, we're in a place we've never been to during training.
            // We then set weights uniformly.
            eprintln!("The model was not trained in this context:\ncurrent_node = {current_node}\nfunc_name = {func_name}");
            let fill_with = 1f64 / (node_outgoing.len() as f64);
            node_outgoing.into_iter().map(|node| (fill_with, node)).collect()
        })
    }

    fn write_weights_to_dot(&self, dot_file_path: &PathBuf) -> io::Result<()> {
        self.get_weights_ref().write_to_dot(dot_file_path)
    }
}

/// This model is the minimal one. It differentiates only between different edges and subgraphs
#[derive(Debug)]
pub struct SubgraphMarkovModel {
    weights: MarkovWeights<HashMap<SmolStr, HashMap<SmolStr, HashMap<SmolStr, f64>>>>,
    weights_ready: bool,
    last_state_stack: Vec<SmolStr>,
}

impl ModelWithMarkovWeights for SubgraphMarkovModel {
    type FuncType=SmolStr;

    fn get_weights_ref(&self) -> &MarkovWeights<HashMap<Self::FuncType, HashMap<SmolStr, HashMap<SmolStr, f64>>>> {
        &self.weights
    }

    fn get_weights_mut_ref(&mut self) -> &mut MarkovWeights<HashMap<Self::FuncType, HashMap<SmolStr, HashMap<SmolStr, f64>>>> {
        &mut self.weights
    }

    fn get_weights_ready_mut_ref(&mut self) -> &mut bool {
        &mut self.weights_ready
    }

    fn get_last_state_stack_mut_ref(&mut self) -> &mut Vec<SmolStr> {
        &mut self.last_state_stack
    }

    fn get_function_name<'a>(&self, call_stack: &'a Vec<StackFrame>, _exit_stack_frame_opt: Option<&StackFrame>) -> &'a Self::FuncType {
        /// TODO: other models:
        // - Call node + func name
        // - Func name + func params
        // - Call node + func name + func params
        // func params can be further divided into:
        // - func params with call modifier states and relations on entry
        // - func params without call modifier states and relations on entry
        // This is how you call call modifier context:
        // call_stack.last().unwrap().call_modifier_info.get_context()
        &call_stack.last().unwrap().function_context.call_params.func_name
    }
}

impl SubgraphMarkovModel {
    /// create model from with the given graph structure
    pub fn new() -> Self {
        Self {
            weights: MarkovWeights::new(),
            weights_ready: false,
            last_state_stack: vec![],
        }
    }
}
