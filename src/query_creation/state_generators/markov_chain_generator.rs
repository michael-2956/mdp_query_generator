pub mod state_choosers;
pub mod dynamic_models;
pub mod markov_chain;
mod dot_parser;
mod error;

use std::{path::PathBuf, collections::{HashMap, HashSet, VecDeque}};

use core::fmt::Debug;
use smol_str::SmolStr;

use crate::{unwrap_variant, query_creation::{state_generators::markov_chain_generator::markov_chain::FunctionTypes, random_query_generator::{query_info::QueryContextManager, call_triggers::CallTriggerTrait}}, config::TomlReadable};

use self::{
    markov_chain::{
        MarkovChain, NodeParams, CallParams, CallModifiers, Function
    }, error::SyntaxError, dot_parser::NodeCommon
};

use state_choosers::StateChooser;
use dynamic_models::{MarkovModel, DynamicModel};

pub use self::markov_chain::CallTypes;
pub use dot_parser::SubgraphType;

#[derive(Clone)]
struct CallTrigger(fn(&QueryContextManager) -> bool);

impl Debug for CallTrigger {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ptr to {{fn(&QueryContentsManager) -> bool}}")
    }
}

#[derive(Clone)]
struct CallTriggerAffector(fn(&mut QueryContextManager) -> ());

impl Debug for CallTriggerAffector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ptr to {{fn(&QueryContentsManager) -> ()}}")
    }
}

/// The markov chain generator. Runs the functional
/// subgraphs parsed from the .dot file. Manages the
/// probabilities, outputs states, disables nodes if
/// needed.
#[derive(Debug)]
pub struct MarkovChainGenerator<StC: StateChooser> {
    /// this stack contains information about the known type name lists
    /// inferred when [known] is used in TYPES
    known_type_list_stack: Vec<Vec<SubgraphType>>,
    /// this stack contains information about the compatible type names
    /// inferred when [compatible] is used in [TYPES]
    compatible_type_list_stack: Vec<Vec<SubgraphType>>,
    markov_chain: MarkovChain,
    call_stack: Vec<StackItem>,
    pending_call: Option<CallParams>,
    state_chooser: Box<StC>,
    call_triggers: HashMap<SmolStr, Box<dyn CallTriggerTrait>>,
    /// if a dead_end_info was collected for the specific function once,
    /// with set arguments (types & modifiers), and all call modifier
    /// states, we can re-use that information.
    dead_end_infos: HashMap<(CallParams, Vec<bool>), HashMap<SmolStr, bool>>,
}

#[derive(Clone, Debug)]
struct StackItem {
    pub function_params: CallParams,
    pub last_node: NodeParams,
}

impl StackItem {
    fn from_call_params(call_params: CallParams) -> Self {
        let func_name = call_params.func_name.clone();
        Self {
            function_params: call_params,
            last_node: NodeParams {
                node_common: NodeCommon::with_name(func_name),
                call_params: None,
                literal: false,
                min_calls_until_function_exit: 0,
            },
        }
    }
}

#[derive(Clone)]
pub struct ChainConfig {
    pub graph_file_path: PathBuf,
}

impl TomlReadable for ChainConfig {
    fn from_toml(toml_config: &toml::Value) -> Self {
        let section = &toml_config["chain"];
        Self {
            graph_file_path: PathBuf::from(section["graph_file_path"].as_str().unwrap()),
        }
    }
}

impl<StC: StateChooser> MarkovChainGenerator<StC> {
    pub fn with_config(config: ChainConfig) -> Result<Self, SyntaxError> {
        let chain = MarkovChain::parse_dot(config.graph_file_path)?;
        let mut _self = MarkovChainGenerator::<StC> {
            markov_chain: chain,
            call_stack: vec![],
            pending_call: None,
            known_type_list_stack: vec![],
            compatible_type_list_stack: vec![],
            state_chooser: Box::new(StC::new()),
            call_triggers: HashMap::new(),
            dead_end_infos: HashMap::new(),
        };
        _self.reset();
        Ok(_self)
    }

    pub fn register_call_trigger<T: CallTriggerTrait + 'static>(&mut self, trigger: T) {
        self.call_triggers.insert(trigger.get_trigger_name(), Box::new(trigger));
    }

    /// used to print the call stack of the markov chain functions
    pub fn print_stack(&self) {
        println!("Call stack:");
        for stack_item in &self.call_stack {
            println!("{} ({:?} | {:?}) [{}]:", stack_item.function_params.func_name, stack_item.function_params.selected_types, stack_item.function_params.modifiers, stack_item.last_node.node_common.name)
        }
    }

    /// used when the markov chain reaches end, for the object to be iterable multiple times
    pub fn reset(&mut self) {
        let accepted_types = unwrap_variant!(self.markov_chain.functions.get("Query").expect(
            "Graph should have an entry function named Query, with TYPES=[...]"
        ).accepted_types.clone(), FunctionTypes::TypeList);

        self.pending_call = Some(CallParams {
            func_name: SmolStr::new("Query"),
            selected_types: CallTypes::TypeList(accepted_types),  // CallTypes::TypeList(vec![SubgraphType::Numeric]),  // 
            modifiers: CallModifiers::None  // CallModifiers::StaticList(vec![SmolStr::new("single value")])  //
        });

        self.known_type_list_stack = vec![];
        self.compatible_type_list_stack = vec![];
    }

    /// get current function inputs list
    pub fn get_fn_selected_types(&self) -> &CallTypes {
        &self.call_stack.last().unwrap().function_params.selected_types
    }

    /// get crrent function modifiers list
    pub fn get_fn_modifiers(&self) -> &CallModifiers {
       &self.call_stack.last().unwrap().function_params.modifiers
    }

    pub fn get_pending_call_accepted_types(&self) -> FunctionTypes {
        self.markov_chain.functions.get(&self.pending_call.as_ref().unwrap().func_name).unwrap().accepted_types.clone()
    }

    pub fn get_pending_call_accepted_modifiers(&self) -> Option<Vec<SmolStr>> {
        self.markov_chain.functions.get(&self.pending_call.as_ref().unwrap().func_name).unwrap().accepted_modifiers.clone()
    }

    /// push the known type list for the next node that will use the types=[known]
    pub fn push_known_list(&mut self, type_list: Vec<SubgraphType>) {
        self.known_type_list_stack.push(type_list);
    }

    /// push the compatible type list for the next node that will use the type=[compatible]
    pub fn push_compatible_list(&mut self, type_list: Vec<SubgraphType>) {
        self.compatible_type_list_stack.push(type_list);
    }

    /// pop the known type for the current node which will uses type=[known]
    fn pop_known_list(&mut self) -> Vec<SubgraphType> {
        self.known_type_list_stack.pop().unwrap_or_else(|| {
            self.print_stack();
            panic!("No known type list found!")
        })
    }

    /// pop the compatible type for the current node which will uses type=[compatible]
    fn pop_compatible_list(&mut self) -> Vec<SubgraphType> {
        match self.compatible_type_list_stack.pop() {
            Some(item) => item,
            None => {
                self.print_stack();
                panic!("No known type name found!")
            },
        }
    }
}

impl<StC: StateChooser> MarkovChainGenerator<StC> {
    pub fn next(&mut self, query_context_manager: &mut QueryContextManager, dynamic_model: &mut (impl DynamicModel + ?Sized)) -> Option<<Self as Iterator>::Item> {
        if let Some(call_params) = self.pending_call.take() {
            // if the last node requested a call, update stack and return function's first state
            let mut inputs = match call_params.selected_types {
                CallTypes::KnownList => CallTypes::TypeList(self.pop_known_list()),
                CallTypes::Compatible => CallTypes::TypeList(self.pop_compatible_list()),
                CallTypes::PassThrough => self.get_fn_selected_types().clone(),
                CallTypes::PassThroughRelated => {
                    let parent_fn_args = unwrap_variant!(self.get_fn_selected_types(), CallTypes::TypeList);
                    CallTypes::TypeList(parent_fn_args
                        .iter()
                        .map(|x| x.to_owned())
                        .filter(|x| x.get_subgraph_func_name() == call_params.func_name)
                        .collect::<Vec<_>>())
                },
                CallTypes::PassThroughRelatedInner => {
                    let parent_fn_args = unwrap_variant!(self.get_fn_selected_types(), CallTypes::TypeList);
                    CallTypes::TypeList(parent_fn_args
                        .iter()
                        .map(|x| x.to_owned())
                        .filter(|x| x.get_subgraph_func_name() == call_params.func_name)
                        .map(|x| x.inner())
                        .collect::<Vec<_>>())
                },
                any => any,
            };
            // if Undetermined is in the parameter list, replace the list with all acceptable arguments.
            if match &inputs {
                CallTypes::TypeList(type_list) => type_list.contains(&SubgraphType::Undetermined),
                _ => false,
            } {
                inputs = match self.markov_chain.functions.get(&call_params.func_name).unwrap().accepted_types.clone() {
                    FunctionTypes::TypeList(type_list) => CallTypes::TypeList(type_list),
                    _ => panic!("Rust stopped working"),
                };
            }
            let new_call_params = CallParams {
                func_name: call_params.func_name.clone(),
                selected_types: inputs,
                modifiers: call_params.modifiers,
            };
            query_context_manager.on_function_begin(new_call_params.clone());
            self.call_stack.push(StackItem::from_call_params(new_call_params));
            return Some(call_params.func_name);
        }

        // search for the last node which isn't an exit node
        let (function, last_node) = loop {
            {
                let stack_item = match self.call_stack.last() {
                    Some(stack_item) => stack_item,
                    None => {
                        self.reset();
                        return None;
                    },
                };

                let last_node = stack_item.last_node.clone();
                let function = self.markov_chain.functions.get(&stack_item.function_params.func_name).unwrap();

                if last_node.node_common.name != function.exit_node_name {
                    break (function, last_node);
                }
            }
            query_context_manager.on_function_end();
            self.call_stack.pop();
        };

        dynamic_model.notify_call_stack_length(self.call_stack.len());
        dynamic_model.update_current_state(&last_node.node_common.name);

        let stack_item = self.call_stack.last_mut().unwrap();

        let call_trigger_states: Vec<bool> = function.call_trigger_names.iter().map(|trigger_name|
            run_call_trigger(&self.call_triggers, trigger_name, &query_context_manager)
        ).collect();
        let dead_end_info = self.dead_end_infos.entry(
            (stack_item.function_params.to_owned(), call_trigger_states.clone())
        ).or_insert(HashMap::new());

        let last_node_outgoing = function.chain.get(&last_node.node_common.name).unwrap();

        let last_node_outgoing = last_node_outgoing.iter().map(|el| {
            (check_node_off_dfs(function, &stack_item.function_params, &self.call_triggers, dead_end_info, query_context_manager, &el.1.node_common), el.0, el.1.clone())
        }).collect::<Vec<_>>();

        let last_node_outgoing = dynamic_model.assign_log_probabilities(last_node_outgoing);

        let destination = self.state_chooser.choose_destination(last_node_outgoing);

        if let Some(destination) = destination {
            if check_node_off_dfs(function, &stack_item.function_params, &self.call_triggers, dead_end_info, query_context_manager, &destination.node_common) {
                panic!("Chosen node is off: {} (after {})", destination.node_common.name, last_node.node_common.name);
            }
            stack_item.last_node = destination.clone();
        } else {
            self.print_stack();
            panic!("No destination found for {}.", last_node.node_common.name);
        }

        // stack_item.last_node is now the current node
        query_context_manager.set_current_node(stack_item.last_node.node_common.name.clone());

        if let Some(ref call_trigger_affector_name) = stack_item.last_node.node_common.affects_call_trigger_name {
            run_call_trigger_affector(&self.call_triggers, call_trigger_affector_name, query_context_manager);
        }

        if let Some(call_params) = &stack_item.last_node.call_params {
            // if it is a call node, we have a new pending call
            let mut prepared_call_params = call_params.clone();
            prepared_call_params.modifiers = match prepared_call_params.modifiers {
                CallModifiers::PassThrough => stack_item.function_params.modifiers.clone(),
                any => any,
            };
            self.pending_call = Some(prepared_call_params);
        }

        Some(stack_item.last_node.node_common.name.clone())
    }
}

fn run_call_trigger_affector(call_triggers: &HashMap<SmolStr, Box<dyn CallTriggerTrait>>, trigger_name: &SmolStr, query_context_manager: &mut QueryContextManager) {
    let new_state = call_triggers
        .get(trigger_name)
        .unwrap()
        .get_trigger_state(query_context_manager)
    ;
    query_context_manager.assign_call_trigger_state(trigger_name.clone(), new_state);
}

fn run_call_trigger(call_triggers: &HashMap<SmolStr, Box<dyn CallTriggerTrait>>, trigger_name: &SmolStr, query_context_manager: &QueryContextManager) -> bool {
    let trigger = call_triggers
        .get(trigger_name)
        .unwrap();
    if let Some(trigger_state) = query_context_manager.get_call_trigger_state(trigger_name) {
        trigger.run(query_context_manager, trigger_state)
    } else {
        trigger.get_default_trigger_value()
    }
}

fn check_node_off_dfs(
    function: &Function,
    function_call_params: &CallParams,
    call_triggers: &HashMap<SmolStr, Box<dyn CallTriggerTrait>>,
    dead_end_info: &mut HashMap<SmolStr, bool>,
    query_context_manager: &mut QueryContextManager,
    node_common: &NodeCommon
) -> bool {
    if check_node_off(function_call_params, call_triggers, query_context_manager, node_common) {
        return true
    }
    if let Some(is_dead_end) = dead_end_info.get(&node_common.name) {
        return *is_dead_end;
    }

    let mut visited = HashSet::new();
    let mut stack = VecDeque::new();
    stack.push_back(vec![node_common.name.clone()]);

    while let Some(path) = stack.pop_back() {
        let current_node_name = path.last().unwrap().clone();
        if let Some(is_dead_end) = dead_end_info.get(&current_node_name) {
            if !*is_dead_end {
                dead_end_info.extend(path.iter().map(|x| (x.clone(), false)));
                return false;
            }
        }
        if current_node_name == function.exit_node_name {
            dead_end_info.extend(path.iter().map(|x| (x.clone(), false)));
            return false;
        }
        if visited.insert(current_node_name.clone()) {
            if let Some(node_outgoing) = function.chain.get(&current_node_name) {
                stack.extend(node_outgoing
                    .iter()
                    .filter(|x| !visited.contains(&x.1.node_common.name) && !check_node_off(
                        function_call_params, call_triggers, query_context_manager, &x.1.node_common
                    ))
                    .map(|x| [&path[..], &[x.1.node_common.name.clone()]].concat())
                );
            }
        }
    }
    dead_end_info.insert(node_common.name.clone(), true);
    true
}

fn check_node_off(
        function_call_params: &CallParams,
        call_triggers: &HashMap<SmolStr, Box<dyn CallTriggerTrait>>,
        query_context_manager: &mut QueryContextManager,
        node_common: &NodeCommon
    ) -> bool {
    let mut off = false;
    if let Some(ref option_name) = node_common.type_name {
        off = match function_call_params.selected_types {
            CallTypes::None => true,
            CallTypes::TypeList(ref t_name_list) => if !t_name_list
                .iter()
                .any(|x| option_name.is_same_or_more_determined_or_undetermined(x)) {
                    true
                } else {
                    false
                },
            _ => panic!("Expected None or TypeNameList for function selected types")
        };
    }
    if let Some((ref trigger_name, ref trigger_on)) = node_common.trigger {
        off = off || match function_call_params.modifiers {
            CallModifiers::None => *trigger_on,
            CallModifiers::PassThrough => panic!("CallModifiers::PassThrough was not substituted for CallModifiers::StaticList!"),
            CallModifiers::StaticList(ref modifiers) => {
                if modifiers.contains(&trigger_name) { !trigger_on } else { *trigger_on }
            },
        };
    }
    if let Some(ref trigger_name) = node_common.call_trigger_name {
        off = off || !run_call_trigger(call_triggers, trigger_name, &query_context_manager);
    }
    return off;
}

impl<StC: StateChooser> Iterator for MarkovChainGenerator<StC> {
    type Item = SmolStr;

    fn next(&mut self) -> Option<Self::Item> {
        self.next(&mut QueryContextManager::new(), &mut MarkovModel::new())
    }
}