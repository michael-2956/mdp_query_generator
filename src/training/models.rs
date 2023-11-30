use std::{io, collections::HashMap};

use smol_str::SmolStr;

use crate::query_creation::state_generator::markov_chain_generator::{StackFrame, markov_chain::Function};

use super::{ast_to_path::PathNode, markov_weights::MarkovWeights};


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
    fn write_weights(&self, file_path: &str) -> io::Result<()>;

    /// read weights from file
    fn load_weights(&mut self, file_path: &str) -> io::Result<()>;

    /// print weights
    fn print_weights(&self) { todo!() }
}

#[derive(Debug)]
pub struct SubgraphMarkovModel {
    weights: MarkovWeights,
    weights_ready: bool,
    last_state_stack: Vec<SmolStr>,
}

impl SubgraphMarkovModel {
    /// create model from with the given graph structure
    pub fn new(chain_functions: &HashMap<SmolStr, Function>) -> Self {
        Self {
            weights: MarkovWeights::new(chain_functions),
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
    }

    fn write_weights(&self, file_path: &str) -> io::Result<()> {
        self.weights.write_to_file(file_path)
    }

    fn load_weights(&mut self, file_path: &str) -> io::Result<()> {
        self.weights = MarkovWeights::read_from_file(file_path)?;
        self.weights_ready = true;
        Ok(())
    }

    fn print_weights(&self) {
        self.weights.print();
    }
}
