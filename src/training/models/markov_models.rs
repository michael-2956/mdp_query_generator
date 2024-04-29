use std::{collections::{BTreeMap, HashMap}, fmt::Display, io, path::PathBuf};

use itertools::Itertools;
use serde::{Deserialize, Serialize};
use smol_str::SmolStr;
use sqlparser::ast::Query;

use crate::query_creation::state_generator::markov_chain_generator::{markov_chain::{CallParams, NodeParams}, StackFrame};

use self::markov_weights::MarkovWeights;

use super::{ModelPredictionResult, PathwayGraphModel};

pub mod markov_weights;

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
    FnCntxt: ModelFunctionContext + 'static
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

    fn predict(&mut self, call_stack: &Vec<StackFrame>, node_outgoing: Vec<NodeParams>, _current_query_ast_opt: Option<&Query>) -> ModelPredictionResult {
        let context = &call_stack.last().unwrap().function_context;
        let func_name = FnCntxt::from_call_stack_and_exit_frame(call_stack, None);
        let current_node = &context.current_node.node_common.name;
        let outgoing_weights = self.weights.get_outgoing_weights_opt(&func_name, current_node);
        if let Some(ref tracked_function) = self.track_transitions_in {
            if func_name == *tracked_function {
                eprintln!(
                    "\nFunction: {func_name}\ncurrent_node = {current_node}\noutgoing_weights = {:?}\nnode_outgoing = {:?}",
                    outgoing_weights,
                    node_outgoing.iter().map(|node| node.node_common.name.clone()).collect::<Vec<_>>()
                );
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
                eprintln!("output = {:#?}\n", output);
            }
        }
        output.map_or(
            ModelPredictionResult::None(node_outgoing),
            |output| ModelPredictionResult::Some(output)
        )
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
pub trait ModelFunctionContext: std::fmt::Debug + Eq + std::hash::Hash + Clone + Serialize + for<'a> Deserialize<'a> + Display + Ord + PartialOrd {
    fn from_call_stack_and_exit_frame(
        call_stack: &Vec<StackFrame>,
        exit_stack_frame_opt: Option<&StackFrame>
    ) -> Self;
}

#[derive(Debug, PartialEq, Eq, std::hash::Hash, Clone, Serialize, Deserialize, PartialOrd, Ord)]
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

#[derive(Debug, PartialEq, Eq, std::hash::Hash, Clone, Serialize, Deserialize, PartialOrd, Ord)]
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

#[derive(Debug, PartialEq, Eq, std::hash::Hash, Clone, Serialize, Deserialize, PartialOrd, Ord)]
pub struct DepthwiseFunctionNameContext {
    func_name: SmolStr,
    depth: usize,
}

impl ModelFunctionContext for DepthwiseFunctionNameContext {
    fn from_call_stack_and_exit_frame(
        call_stack: &Vec<StackFrame>,
        exit_stack_frame_opt: Option<&StackFrame>
    ) -> Self {
        let stack_frame = if let Some(stack_frame) = exit_stack_frame_opt {
            stack_frame
        } else {
            call_stack.last().unwrap()
        };
        DepthwiseFunctionNameContext {
            func_name: stack_frame.function_context.call_params.func_name.clone(),
            depth: call_stack.len() + exit_stack_frame_opt.is_some() as usize,
        }
    }
}

impl std::fmt::Display for DepthwiseFunctionNameContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}_{}", self.func_name, self.depth)
    }
}

#[derive(Debug, PartialEq, Eq, std::hash::Hash, Clone, Serialize, Deserialize, PartialOrd, Ord)]
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

#[derive(Debug, PartialEq, Eq, std::hash::Hash, Clone, Serialize, Deserialize, PartialOrd, Ord)]
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

#[derive(Debug, PartialEq, Eq, std::hash::Hash, Clone, Serialize, Deserialize, PartialOrd, Ord)]
pub struct DepthwiseFullFunctionContext {
    depth: usize,
    context: FullFunctionContext,
}

impl ModelFunctionContext for DepthwiseFullFunctionContext {
    fn from_call_stack_and_exit_frame(
        call_stack: &Vec<StackFrame>,
        exit_stack_frame_opt: Option<&StackFrame>
    ) -> Self {
        Self {
            depth: call_stack.len() + exit_stack_frame_opt.is_some() as usize,
            context: if let Some(frame) = exit_stack_frame_opt {
                FullFunctionContext::from_stack_frame(frame)
            } else {
                FullFunctionContext::from_stack_frame(call_stack.last().unwrap())
            }
        }
    }
}

impl std::fmt::Display for DepthwiseFullFunctionContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "D_{}_{}", self.depth, self.context)
    }
}

#[derive(Debug, PartialEq, Eq, std::hash::Hash, Clone, Serialize, Deserialize, PartialOrd, Ord)]
pub struct PathwiseContext {
    path: Vec<SmolStr>
}

impl ModelFunctionContext for PathwiseContext {
    fn from_call_stack_and_exit_frame(
        _call_stack: &Vec<StackFrame>,
        _exit_stack_frame_opt: Option<&StackFrame>
    ) -> Self {
        todo!()
    }
}

impl std::fmt::Display for PathwiseContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.path.iter().cloned().collect_vec().join("_"))
    }
}
