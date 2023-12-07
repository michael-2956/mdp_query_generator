use std::{io, path::PathBuf, str::FromStr, collections::{HashMap, BTreeMap}, fmt::Display};

use serde::{Serialize, Deserialize};
use smol_str::SmolStr;

use crate::{query_creation::state_generator::markov_chain_generator::{StackFrame, markov_chain::{NodeParams, CallParams}}, config::TomlReadable};

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
    // whether to use the stacked version of the model
    pub stacked_version: bool,
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
            stacked_version: section["stacked_version"].as_bool().unwrap()
        }
    }
}

impl ModelConfig {
    pub fn create_model(&self) -> io::Result<Box<dyn PathwayGraphModel>> {
        let mut model: Box<dyn PathwayGraphModel> = match (self.model_name.as_str(), self.stacked_version) {
            ("subgraph", false) => Box::new(ModelWithMarkovWeights::<FunctionNameContext>::new()),
            ("subgraph", true) => {
                let mut st = Box::new(ModelWithMarkovWeights::<StackedFunctionNamesContext>::new());
                st.track_transitions_in(StackedFunctionNamesContext {
                    func_names: vec![
                        "Query", "WHERE", "types", "Query", "FROM", "Query", "WHERE", "types",
                        "VAL_3", "types", "Query", "WHERE", "types", "VAL_3", "types", "Query",
                    ].into_iter().map(SmolStr::new).collect()
                });
                st
            },
            ("full_function_context", false) => Box::new(ModelWithMarkovWeights::<FullFunctionContext>::new()),
            ("full_function_context", true) => Box::new(ModelWithMarkovWeights::<StackedFullFunctionContext>::new()),
            (any, st) => panic!("No such model: name: {any} stacked: {st}"),
        };
        if self.load_weights {
            println!("Loading weights from {}...", self.load_weights_from.display());
            model.load_weights(&self.load_weights_from)?;
        }
        if let Some(ref dot_file_path) = self.save_dot_file {
            println!("Saving .dot file to {}...", dot_file_path.display());
            model.write_weights_to_dot(dot_file_path)?;
        }
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

#[derive(Debug)]
pub struct ModelWithMarkovWeights<FnCntxt>
where
    FnCntxt: ModelFunctionContext
{
    weights: MarkovWeights<HashMap<FnCntxt, HashMap<SmolStr, HashMap<SmolStr, f64>>>>,
    weights_ready: bool,
    last_state_stack: Vec<SmolStr>,
    track_transitions_in: Option<FnCntxt>,
}

impl<FnCntxt> ModelWithMarkovWeights<FnCntxt>
where
    FnCntxt: ModelFunctionContext
{
    pub fn new() -> Self {
        Self {
            weights: MarkovWeights::new(),
            weights_ready: false,
            last_state_stack: vec![],
            track_transitions_in: None,
        }
    }

    pub fn track_transitions_in(&mut self, fn_context: FnCntxt) {
        self.track_transitions_in = Some(fn_context);
    }
}

impl<FnCntxt> PathwayGraphModel for ModelWithMarkovWeights<FnCntxt>
where
    FnCntxt: ModelFunctionContext
{
    fn start_epoch(&mut self) {
        if self.weights_ready {
            panic!("SubgraphMarkovModel does not allow multiple epochs.")
        }
    }

    fn process_state(&mut self, call_stack: &Vec<StackFrame>, popped_stack_frame: Option<&StackFrame>) {
        if self.last_state_stack.len() < call_stack.len() {
            let func_name = call_stack.last().unwrap().function_context.call_params.func_name.clone();
            self.last_state_stack.push(func_name);
        } else {
            let is_exit_node = self.last_state_stack.len() > call_stack.len();
            let stack_frame = if is_exit_node {
                popped_stack_frame.unwrap()
            } else {
                call_stack.last().unwrap()
            };
            let (last_state, current_state) = if let Some(last_state) = self.last_state_stack.last_mut() {
                let current_state = stack_frame.function_context.current_node.node_common.name.clone();
                let c = last_state.clone();
                *last_state = current_state;
                (c, last_state.clone())
            } else {
                panic!("No last state available, but received call stack: {:?}", call_stack)
            };
            let func_name = FnCntxt::from_call_stack_and_exit_frame(
                call_stack,
                if is_exit_node { Some(stack_frame) } else { None }
            );
            self.weights.insert_edge(func_name, &last_state, &current_state);
            if is_exit_node {
                self.last_state_stack.pop();
            }
        }
    }

    fn update_weights(&mut self) {
        self.weights_ready = true;
        self.weights.normalize();
    }

    fn write_weights(&self, file_path: &PathBuf) -> io::Result<()> {
        self.weights.write_to_file(file_path)
    }

    fn load_weights(&mut self, file_path: &PathBuf) -> io::Result<()> {
        self.weights = MarkovWeights::load(file_path)?;
        self.weights_ready = true;
        Ok(())
    }

    fn print_weights(&self) {
        self.weights.print();
    }

    fn predict(&mut self, call_stack: &Vec<StackFrame>, node_outgoing: Vec<NodeParams>) -> Vec<(f64, NodeParams)> {
        let context = &call_stack.last().unwrap().function_context;
        let func_name = FnCntxt::from_call_stack_and_exit_frame(call_stack, None);
        let current_node = &context.current_node.node_common.name;
        let outgoing_weights = self.weights.get_outgoing_weights_opt(&func_name, current_node);
        if let Some(ref tracked_function) = self.track_transitions_in {
            if func_name == *tracked_function {
                println!("\nFunction: {func_name}\ncurrent_node = {current_node}\noutgoing_weights = {:?}", outgoing_weights);
            }
        }
        let output = if let Some(outgoing_weights) = outgoing_weights {
            // obtain weights
            let mut output: Vec<_> = node_outgoing.iter().map(|node| (
                outgoing_weights.get(&node.node_common.name)
                    .map(|w| (*w, node))
                    .unwrap_or((0f64, node))
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
        if let Some(ref tracked_function) = self.track_transitions_in {
            if func_name == *tracked_function {
                println!("output = {:#?}\n", output);
            }
        }
        output.unwrap_or_else(|| {
            // The model is undertrained for this context and cannot decide.
            // Basically, we're in a place we've never been to during training.
            // We then set weights uniformly.
            // eprintln!("The model was not trained in this context:\ncurrent_node = {current_node}\nfunc_name = {func_name}");
            let fill_with = 1f64 / (node_outgoing.len() as f64);
            node_outgoing.into_iter().map(|node| (fill_with, node)).collect()
        })
    }

    fn write_weights_to_dot(&self, dot_file_path: &PathBuf) -> io::Result<()> {
        self.weights.write_to_dot(dot_file_path)
    }
}

/// Trait for implementing diffrent Function contexts for Models with Markov Weights\
/// TODO: other models:\
/// - DONE (UNSTACKED): (1) func name\
/// - (2) Call node + func name\
/// - DONE (UNSTACKED): (3) Func name + func params\
/// - (4) Call node + func name + func params\
/// Func params should include call modifier context\
/// This is how you get call modifier context:\
/// call_stack.last().unwrap().call_modifier_info.get_context()
pub trait ModelFunctionContext: std::fmt::Debug + Eq + std::hash::Hash + Clone + Serialize + for<'a> Deserialize<'a> + Display {
    fn from_call_stack_and_exit_frame(
        call_stack: &Vec<StackFrame>,
        exit_stack_frame_opt: Option<&StackFrame>
    ) -> Self;
}

#[derive(Debug, PartialEq, Eq, std::hash::Hash, Clone, Serialize, Deserialize)]
pub struct FunctionNameContext {
    func_name: SmolStr,
}

impl ModelFunctionContext for FunctionNameContext {
    fn from_call_stack_and_exit_frame(
        call_stack: &Vec<StackFrame>,
        exit_stack_frame_opt: Option<&StackFrame>
    ) -> Self {
        let stack_frame = if let Some(stack_frame) = exit_stack_frame_opt {
            stack_frame
        } else {
            call_stack.last().unwrap()
        };
        FunctionNameContext {
            func_name: stack_frame.function_context.call_params.func_name.clone()
        }
    }
}

impl std::fmt::Display for FunctionNameContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.func_name)
    }
}

#[derive(Debug, PartialEq, Eq, std::hash::Hash, Clone, Serialize, Deserialize)]
pub struct StackedFunctionNamesContext {
    func_names: Vec<SmolStr>,
}

impl ModelFunctionContext for StackedFunctionNamesContext {
    fn from_call_stack_and_exit_frame(
        call_stack: &Vec<StackFrame>,
        exit_stack_frame_opt: Option<&StackFrame>
    ) -> Self {
        let mut func_names: Vec<_> = call_stack.iter().map(
            |frame| frame.function_context.call_params.func_name.clone()
        ).collect();
        if let Some(frame) = exit_stack_frame_opt {
            func_names.push(frame.function_context.call_params.func_name.clone());
        }
        Self {
            func_names,
        }
    }
}

impl std::fmt::Display for StackedFunctionNamesContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.func_names.first().unwrap())?;
        for func_name in self.func_names.iter().skip(1) {
            write!(f, "_{func_name}")?;
        }
        Ok(())
    }
}

#[derive(Debug, PartialEq, Eq, std::hash::Hash, Clone, Serialize, Deserialize)]
pub struct FullFunctionContext {
    call_params: CallParams,
    call_modifier_context: (BTreeMap<SmolStr, BTreeMap<SmolStr, bool>>, BTreeMap<SmolStr, bool>),
}

impl FullFunctionContext {
    fn from_stack_frame(frame: &StackFrame) -> Self {
        Self {
            call_params: frame.function_context.call_params.clone(),
            call_modifier_context: frame.call_modifier_info.get_context(),
        }
    }
}

impl ModelFunctionContext for FullFunctionContext {
    fn from_call_stack_and_exit_frame(
        call_stack: &Vec<StackFrame>,
        exit_stack_frame_opt: Option<&StackFrame>
    ) -> Self {
        let stack_frame = if let Some(stack_frame) = exit_stack_frame_opt {
            stack_frame
        } else {
            call_stack.last().unwrap()
        };
        Self::from_stack_frame(stack_frame)
    }
}

impl std::fmt::Display for FullFunctionContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.call_params.func_name)
    }
}

#[derive(Debug, PartialEq, Eq, std::hash::Hash, Clone, Serialize, Deserialize)]
pub struct StackedFullFunctionContext {
    contexts: Vec<FullFunctionContext>,
}

impl ModelFunctionContext for StackedFullFunctionContext {
    fn from_call_stack_and_exit_frame(
        call_stack: &Vec<StackFrame>,
        exit_stack_frame_opt: Option<&StackFrame>
    ) -> Self {
        let mut contexts: Vec<_> = call_stack.iter().map(
            |frame| FullFunctionContext::from_stack_frame(frame)
        ).collect();
        if let Some(frame) = exit_stack_frame_opt {
            contexts.push(FullFunctionContext::from_stack_frame(frame));
        }
        Self {
            contexts
        }
    }
}

impl std::fmt::Display for StackedFullFunctionContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.contexts.first().unwrap())?;
        for context in self.contexts.iter().skip(1) {
            write!(f, "_{context}")?;
        }
        Ok(())
    }
}
