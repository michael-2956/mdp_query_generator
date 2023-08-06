pub mod state_choosers;
pub mod dynamic_models;
pub mod markov_chain;
mod dot_parser;
mod error;

use std::{path::PathBuf, collections::{HashMap, HashSet, VecDeque, BTreeMap}, sync::{Arc, Mutex}};

use core::fmt::Debug;
use smol_str::SmolStr;
use take_until::TakeUntilExt;

use crate::{unwrap_variant, query_creation::{state_generators::markov_chain_generator::markov_chain::FunctionTypes, random_query_generator::{query_info::ClauseContext, call_triggers::CallTriggerTrait}}, config::TomlReadable};

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
struct CallTrigger(fn(&ClauseContext) -> bool);

impl Debug for CallTrigger {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ptr to {{fn(&QueryContentsManager) -> bool}}")
    }
}

#[derive(Clone)]
struct CallTriggerAffector(fn(&mut ClauseContext) -> ());

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
    call_stack: Vec<StackFrame>,
    pending_call: Option<CallParams>,
    state_chooser: Box<StC>,
    call_triggers: HashMap<SmolStr, Box<dyn CallTriggerTrait>>,
    /// if a dead_end_info was collected for the specific function once,
    /// with set arguments (types & modifiers), and all call modifier
    /// states, depending on which affectors affected what and how,
    /// we can re-use that information.
    dead_end_infos: HashMap<(CallParams, BTreeMap<SmolStr, BTreeMap<SmolStr, bool>>), Arc<Mutex<HashMap<SmolStr, bool>>>>,
}

/// function context, like the current node, and the
/// current function arguments (call params)
#[derive(Debug, Clone)]
pub struct FunctionContext {
    /// the call params (arguments) of the current function
    pub call_params: CallParams,
    /// the last node that was outputted by the state generator
    pub current_node: NodeParams,
}

impl FunctionContext {
    fn new(call_params: CallParams) -> Self {
        let func_name = call_params.func_name.clone();
        Self {
            call_params,
            current_node: NodeParams {
                node_common: NodeCommon::with_name(func_name),
                call_params: None,
                literal: false,
                min_calls_until_function_exit: 0,
            },
        }
    }

    /// create a new FunctionContext with the provided node_params as the current_node
    fn with_current_node(&self, node_params: NodeParams) -> Self {
        Self {
            call_params: self.call_params.clone(),
            current_node: node_params,
        }
    }
}

/// This structure stores all the info about
/// call triggers in the current funciton
#[derive(Debug)]
struct CallTriggerInfo {
    /// the call trigger inner state memory
    inner_states: HashMap<SmolStr, Box<dyn std::any::Any>>,
    /// call trigger configuration: How each call trigger affector
    /// node affects every node with a call trigger
    node_relations: BTreeMap<SmolStr, BTreeMap<SmolStr, bool>>,
    /// stores current call trigger node states
    node_states: BTreeMap<SmolStr, Option<bool>>,
    /// stores a hashset of all node trigger affectors
    affector_nodes: HashSet<SmolStr>,
}

impl CallTriggerInfo {
    fn new(node_relations: BTreeMap<SmolStr, BTreeMap<SmolStr, bool>>) -> Self {
        Self {
            inner_states: HashMap::new(),
            node_states: node_relations.iter().map(|x| (x.0.clone(), None)).collect(),
            affector_nodes: node_relations.iter().map(|x| x.1.iter()).flatten().map(|x| x.0.clone()).collect(),
            node_relations,
        }
    }

    fn update_inner_state(&mut self, call_trigger: &mut Box<dyn CallTriggerTrait>, clause_context: &ClauseContext, function_context: &FunctionContext) {
        let new_state = call_trigger.get_new_trigger_state(clause_context, function_context);
        self.inner_states.insert(call_trigger.get_trigger_name(), new_state);
        self.node_states.extend(
            self.node_relations.iter()
                .filter_map(|(affected_node_name, node_config)| {
                    node_config.get(&function_context.current_node.node_common.name).map(|&val| (affected_node_name.clone(), Some(val)))
                })
        );
    }
}

#[derive(Debug)]
pub struct StackFrame {
    /// function context, like the current node, and the
    /// current function arguments (call params)
    pub function_context: FunctionContext,
    /// various call trigger info
    call_trigger_info: CallTriggerInfo,
    /// Cached version of all the dead ends
    /// in the current function
    dead_end_info: Arc<Mutex<HashMap<SmolStr, bool>>>,
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

    pub fn get_call_trigger_state(&self, trigger_name: &SmolStr) -> Option<&Box<dyn std::any::Any>> {
        self.call_stack.last().unwrap().call_trigger_info.inner_states.get(trigger_name)
    }

    /// used to print the call stack of the markov chain functions
    pub fn print_stack(&self) {
        println!("Call stack:");
        for stack_item in &self.call_stack {
            println!("{} ({:?} | {:?}) [{}]:", stack_item.function_context.call_params.func_name, stack_item.function_context.call_params.selected_types, stack_item.function_context.call_params.modifiers, stack_item.function_context.current_node.node_common.name)
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

    fn start_function(&mut self, mut call_params: CallParams, clause_context: &ClauseContext) -> SmolStr {
        // if the last node requested a call, update stack and return function's first state
        let mut are_wrapped = false;
        call_params.selected_types = match call_params.selected_types {
            CallTypes::KnownList => CallTypes::TypeList(self.pop_known_list()),
            CallTypes::Compatible => CallTypes::TypeList(self.pop_compatible_list()),
            CallTypes::PassThrough => self.get_fn_selected_types_unwrapped().clone(),
            CallTypes::PassThroughTypeNameRelated => {
                let parent_fn_args = unwrap_variant!(self.get_fn_selected_types_unwrapped(), CallTypes::TypeList);
                let call_node_type_name = &self.get_current_state_type_name_unwrapped();
                CallTypes::TypeList(parent_fn_args.iter()
                    .map(|x| x.to_owned())
                    .filter(|x| x.is_same_or_more_determined_or_undetermined(call_node_type_name))
                    .map(|x| if x == SubgraphType::Undetermined { call_node_type_name.clone() } else { x })
                    .collect::<Vec<_>>())
            },
            CallTypes::PassThroughRelated => {
                let parent_fn_args = unwrap_variant!(self.get_fn_selected_types_unwrapped(), CallTypes::TypeList);
                are_wrapped = true;  // Function name related types don't need to be unwrapped
                CallTypes::TypeList(parent_fn_args.iter()
                    .map(|x| x.to_owned())
                    .filter(|x| x.get_subgraph_func_name() == call_params.func_name)
                    .collect::<Vec<_>>())
            },
            CallTypes::PassThroughRelatedInner => {
                let parent_fn_args = unwrap_variant!(self.get_fn_selected_types_unwrapped(), CallTypes::TypeList);
                CallTypes::TypeList(parent_fn_args.iter()
                    .map(|x| x.to_owned())
                    .filter(|x| x.get_subgraph_func_name() == call_params.func_name)
                    .map(|x| x.inner())
                    .collect::<Vec<_>>())
            },
            any => any,
        };

        let new_function = self.markov_chain.functions.get(&call_params.func_name).unwrap();

        // if Undetermined is in the parameter list, replace the list with all acceptable arguments.
        match &call_params.selected_types {
            CallTypes::TypeList(type_list) if type_list.contains(&SubgraphType::Undetermined) => {
                are_wrapped = true;  // the accepted function types are already wrapped
                call_params.selected_types = CallTypes::TypeList(unwrap_variant!(new_function.accepted_types.clone(), FunctionTypes::TypeList));
            },
            _ => {},
        };

        if new_function.uses_wrapped_types && !are_wrapped {
            call_params.selected_types = match call_params.selected_types {
                CallTypes::TypeList(type_list) => CallTypes::TypeList(
                    type_list.into_iter().map(|x| x.wrap_in_func(&call_params.func_name)).collect()
                ),
                CallTypes::None => CallTypes::None,
                any => panic!("Unexpected call_params.selected_types: {:?}", any),
            }
        }

        let function_context = FunctionContext::new(call_params);

        let node_relations: BTreeMap<SmolStr, BTreeMap<SmolStr, bool>> = new_function.call_trigger_nodes_and_affectors.iter().map(|x|
            (x.0.node_common.name.clone(), x.1.iter().map(|affector_node| {
                let trigger_name = x.0.node_common.call_trigger_name.as_ref().unwrap();
                let new_state = self.call_triggers.get(trigger_name).unwrap().get_new_trigger_state(
                    clause_context, &function_context.with_current_node(affector_node.clone())
                );
                (affector_node.node_common.name.clone(), self.call_triggers.get(trigger_name).unwrap().run(
                    clause_context,
                    &function_context.with_current_node(x.0.clone()),
                    &new_state,
                ))
            }).collect())
        ).collect();

        let dead_end_info = self.dead_end_infos.entry(
            (function_context.call_params.to_owned(), node_relations.clone())
        ).or_insert(Arc::new(Mutex::new(HashMap::new()))).to_owned();

        let func_name = function_context.call_params.func_name.clone();

        self.call_stack.push(StackFrame {
            function_context,
            call_trigger_info: CallTriggerInfo::new(node_relations),
            dead_end_info,
        });

        func_name
    }

    /// get current function inputs list
    pub fn get_fn_selected_types_unwrapped(&self) -> CallTypes {
        let function_context = &self.call_stack.last().unwrap().function_context;
        let mut selected_types = function_context.call_params.selected_types.clone();
        let function = self.markov_chain.functions.get(&function_context.call_params.func_name).unwrap();
        if function.uses_wrapped_types {
            if let CallTypes::TypeList(type_list) = selected_types {
                selected_types = CallTypes::TypeList(type_list.into_iter().map(|x| x.inner()).collect());
            }
        }
        selected_types
    }

    pub fn get_current_state_type_name_unwrapped(&self) -> SubgraphType {
        let function_context = &self.call_stack.last().unwrap().function_context;
        let mut call_node_type_name = function_context.current_node.node_common.type_name.as_ref().unwrap().clone();
        let function = self.markov_chain.functions.get(&function_context.call_params.func_name).unwrap();
        if function.uses_wrapped_types {
            call_node_type_name = call_node_type_name.inner();
        }
        call_node_type_name
    }

    /// get crrent function modifiers list
    pub fn get_fn_modifiers(&self) -> &CallModifiers {
       &self.call_stack.last().unwrap().function_context.call_params.modifiers
    }

    pub fn get_pending_call_accepted_types(&self) -> FunctionTypes {
        self.markov_chain.functions.get(&self.pending_call.as_ref().unwrap().func_name).unwrap().accepted_types.clone()
    }

    pub fn get_pending_call_accepted_modifiers(&self) -> Option<Vec<SmolStr>> {
        self.markov_chain.functions.get(&self.pending_call.as_ref().unwrap().func_name).unwrap().accepted_modifiers.clone()
    }

    /// push the known type list for the next node that will use the types=[known]
    /// these will be wrapped automatically if uses_wrapped_types=true
    pub fn push_known_list(&mut self, type_list: Vec<SubgraphType>) {
        self.known_type_list_stack.push(type_list);
    }

    /// push the compatible type list for the next node that will use the type=[compatible]
    /// /// these will be wrapped automatically if uses_wrapped_types=true
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
    pub fn next(&mut self, clause_context: &ClauseContext, dynamic_model: &mut (impl DynamicModel + ?Sized)) -> Option<<Self as Iterator>::Item> {
        if let Some(call_params) = self.pending_call.take() {
            return Some(self.start_function(call_params, clause_context));
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

                let last_node = stack_item.function_context.current_node.clone();
                let function = self.markov_chain.functions.get(&stack_item.function_context.call_params.func_name).unwrap();

                if last_node.node_common.name != function.exit_node_name {
                    break (function, last_node);
                }
            }
            self.call_stack.pop();
        };

        dynamic_model.notify_call_stack_length(self.call_stack.len());
        dynamic_model.update_current_state(&last_node.node_common.name);

        let stack_frame = self.call_stack.last_mut().unwrap();

        let last_node_outgoing = function.chain.get(&last_node.node_common.name).unwrap();

        let last_node_outgoing = last_node_outgoing.iter().map(|el| {
            (check_node_off_dfs(function, stack_frame, &el.1.node_common), el.0, el.1.clone())
        }).collect::<Vec<_>>();

        let last_node_outgoing = dynamic_model.assign_log_probabilities(last_node_outgoing);

        let destination = self.state_chooser.choose_destination(last_node_outgoing);

        if let Some(destination) = destination {
            if check_node_off_dfs(function, stack_frame, &destination.node_common) {
                panic!("Chosen node is off: {} (after {})", destination.node_common.name, last_node.node_common.name);
            }
            stack_frame.function_context.current_node = destination.clone();
        } else {
            self.print_stack();
            panic!("No destination found for {}.", last_node.node_common.name);
        }

        // stack_item.function_context.current_node is now the current node

        if let Some(ref trigger_name) = stack_frame.function_context.current_node.node_common.affects_call_trigger_name {
            stack_frame.call_trigger_info.update_inner_state(self.call_triggers.get_mut(trigger_name).unwrap(), clause_context, &stack_frame.function_context);
        }

        if let Some(call_params) = &stack_frame.function_context.current_node.call_params {
            // if it is a call node, we have a new pending call
            let mut prepared_call_params = call_params.clone();
            prepared_call_params.modifiers = match prepared_call_params.modifiers {
                CallModifiers::PassThrough => stack_frame.function_context.call_params.modifiers.clone(),
                any => any,
            };
            self.pending_call = Some(prepared_call_params);
        }

        Some(stack_frame.function_context.current_node.node_common.name.clone())
    }
}

fn check_node_off_dfs(function: &Function, stack_frame: &StackFrame, node_common: &NodeCommon) -> bool {
    if check_node_off(&stack_frame.function_context.call_params, &stack_frame.call_trigger_info.node_states, node_common) {
        return true
    }

    let mut dead_end_info = stack_frame.dead_end_info.lock().unwrap();
    if let Some(is_dead_end) = dead_end_info.get(&node_common.name) {
        return *is_dead_end;
    }

    // only update dead_end_info for paths that were not affected yet
    let path_is_not_affected = stack_frame.call_trigger_info.inner_states.is_empty();

    let mut visited = HashSet::new();
    let mut to_mark_as_dead_ends = HashSet::new();
    let mut stack = VecDeque::new();
    stack.push_back((
        vec![node_common.name.clone()], stack_frame.call_trigger_info.node_states.clone()
    ));

    while let Some((path, mut call_trigger_states)) = stack.pop_back() {
        let current_node_name = path.last().unwrap().clone();
        let path_until_affector: Option<Vec<SmolStr>> = if path_is_not_affected { Some(path.iter().take_until(
            |&x| stack_frame.call_trigger_info.affector_nodes.contains(x)
        ).map(|x| x.clone()).collect()) } else { None };
        if let Some(is_dead_end) = dead_end_info.get(&current_node_name) {
            if !*is_dead_end {
                if path_is_not_affected {
                    if node_common.name == "types_select_type_noexpr" {
                        println!("\nNew dead_end_info 1: {:#?}", dead_end_info);
                    }
                    dead_end_info.extend(path_until_affector.unwrap().iter().map(|x| (x.clone(), false)));
                    if node_common.name == "types_select_type_noexpr" {
                        println!("New dead_end_info 1: {:#?}", dead_end_info);
                    }
                }
                return false;
            }
        }
        if current_node_name == function.exit_node_name {
            if path_is_not_affected {
                if node_common.name == "types_select_type_noexpr" {
                    println!("\nOld dead_end_info 2: {:#?}", dead_end_info);
                }
                dead_end_info.extend(path_until_affector.unwrap().iter().map(|x| (x.clone(), false)));
                if node_common.name == "types_select_type_noexpr" {
                    println!("New dead_end_info 2: {:#?}", dead_end_info);
                }
            }
            return false;
        }
        call_trigger_states.extend(
            stack_frame.call_trigger_info.node_relations.iter()
                .filter_map(|(affected_node_name, node_config)| {
                    node_config.get(&current_node_name).map(|&val| (affected_node_name.clone(), Some(val)))
                })
        );
        if path_is_not_affected {
            to_mark_as_dead_ends.extend(path_until_affector.unwrap().into_iter());
        }
        if visited.insert(current_node_name.clone()) {
            if let Some(node_outgoing) = function.chain.get(&current_node_name) {
                stack.extend(node_outgoing
                    .iter()
                    .filter(|x| !visited.contains(&x.1.node_common.name) && !check_node_off(
                        &stack_frame.function_context.call_params, &call_trigger_states, &x.1.node_common
                    ))
                    .map(|x| (
                        [&path[..], &[x.1.node_common.name.clone()]].concat(),
                        call_trigger_states.clone()
                    ))
                );
            }
        }
    }
    if path_is_not_affected {
        if node_common.name == "types_select_type_noexpr" {
            println!("\nOld dead_end_info 3: {:#?}\nto_mark_as_dead_ends: {:#?}", dead_end_info, to_mark_as_dead_ends);
        }
        dead_end_info.extend(to_mark_as_dead_ends.into_iter().map(|x| (x, true)));
        if node_common.name == "types_select_type_noexpr" {
            println!("New dead_end_info 3: {:#?}", dead_end_info);
        }
    }
    true
}

fn check_node_off(
        call_params: &CallParams,
        call_trigger_states: &BTreeMap<SmolStr, Option<bool>>,
        node_common: &NodeCommon
    ) -> bool {
    let mut off = false;
    if let Some(ref type_name) = node_common.type_name {
        off = match call_params.selected_types {
            CallTypes::None => true,
            CallTypes::TypeList(ref t_name_list) => !t_name_list.iter()
                .any(|x| x.is_same_or_more_determined_or_undetermined(type_name)),
            _ => panic!("Expected None or TypeNameList for function selected types")
        };
    }
    if let Some((ref trigger_name, ref trigger_on)) = node_common.trigger {
        off = off || match call_params.modifiers {
            CallModifiers::None => *trigger_on,
            CallModifiers::PassThrough => panic!("CallModifiers::PassThrough was not substituted for CallModifiers::StaticList!"),
            CallModifiers::StaticList(ref modifiers) => {
                if modifiers.contains(&trigger_name) { !trigger_on } else { *trigger_on }
            },
        };
    }
    if let Some(ref trigger_name) = node_common.call_trigger_name {
        off = off || !call_trigger_states.get(&node_common.name).unwrap().unwrap_or_else(
            || panic!("State of call trigger {trigger_name} was not set (node {})", node_common.name)
        )
    }
    return off;
}

impl<StC: StateChooser> Iterator for MarkovChainGenerator<StC> {
    type Item = SmolStr;

    fn next(&mut self) -> Option<Self::Item> {
        self.next(&mut ClauseContext::new(), &mut MarkovModel::new())
    }
}