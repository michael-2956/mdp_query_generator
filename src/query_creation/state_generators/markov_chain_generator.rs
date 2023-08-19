pub mod state_choosers;
pub mod dynamic_models;
pub mod markov_chain;
mod dot_parser;
mod error;

use std::{path::PathBuf, collections::{HashMap, HashSet, VecDeque, BTreeMap}, sync::{Arc, Mutex}};

use core::fmt::Debug;
use rand::{seq::SliceRandom, SeedableRng};
use rand_chacha::ChaCha8Rng;
use smol_str::SmolStr;
use take_until::TakeUntilExt;

use crate::{unwrap_variant, query_creation::{state_generators::markov_chain_generator::markov_chain::FunctionTypes, random_query_generator::{query_info::ClauseContext, call_modifiers::{CallModifierTrait, StatefulCallModifierTrait}}}, config::TomlReadable};

use self::{
    markov_chain::{
        MarkovChain, NodeParams, CallParams, CallModifiers, Function
    }, error::SyntaxError, dot_parser::{NodeCommon, TypeWithFields}
};

use state_choosers::StateChooser;
use dynamic_models::{MarkovModel, DynamicModel};

pub use self::markov_chain::CallTypes;
pub use dot_parser::SubgraphType;

#[derive(Clone)]
struct CallModifier(fn(&ClauseContext) -> bool);

impl Debug for CallModifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ptr to {{fn(&QueryContentsManager) -> bool}}")
    }
}

#[derive(Clone)]
struct CallModifierAffector(fn(&mut ClauseContext) -> ());

impl Debug for CallModifierAffector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ptr to {{fn(&QueryContentsManager) -> ()}}")
    }
}

#[derive(Debug)]
struct StatefulCallModifierCreator(fn() -> Box<dyn StatefulCallModifierTrait>);

/// The markov chain generator. Runs the functional
/// subgraphs parsed from the .dot file. Manages the
/// probabilities, outputs states, disables nodes if
/// needed.
#[derive(Debug)]
pub struct MarkovChainGenerator<StC: StateChooser> {
    markov_chain: MarkovChain,
    call_stack: Vec<StackFrame>,
    pending_call: Option<CallParams>,
    state_chooser: Box<StC>,
    stateless_call_modifiers: HashMap<SmolStr, Box<dyn CallModifierTrait>>,
    stateful_call_modifier_creators: HashMap<SmolStr, StatefulCallModifierCreator>,
    /// if a dead_end_info was collected for the specific function once,
    /// with set arguments (types & modifiers), and all call modifier
    /// states, depending on which affectors affected what and how,
    /// we can re-use that information.
    dead_end_infos: HashMap<(CallParams, BTreeMap<SmolStr, BTreeMap<SmolStr, bool>>), Arc<Mutex<HashMap<SmolStr, bool>>>>,
}

/// function context: the current node, and the
/// current function arguments (call params)
/// NOTE: Whatever data is added here in the future
/// should be PATH INDEPENDANT
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
    fn with_node(&self, node_params: NodeParams) -> Self {
        Self {
            call_params: self.call_params.clone(),
            current_node: node_params,
        }
    }
}

/// This structure stores all the info about
/// call modifiers in the current funciton
#[derive(Debug)]
struct CallModifierInfo {
    /// stores current node states that are affected by call modifiers
    node_states: BTreeMap<SmolStr, Option<bool>>,
    /// the call modifier inner state memory
    stateless_inner_states: HashMap<SmolStr, Box<dyn std::any::Any>>,
    /// call modifier configuration: How each call modifier affector
    /// node affects every node with a call modifier (STATELESS)
    stateless_node_relations: BTreeMap<SmolStr, BTreeMap<SmolStr, bool>>,
    /// stores all the stateful modifiers of the current function in
    /// their current state
    stateful_modifiers: HashMap<SmolStr, Box<dyn StatefulCallModifierTrait>>,
    /// call modifier configuration: Which nodes affect which call modifiers
    /// with which nodes.
    stateful_affectors_and_modified_nodes: HashMap<NodeParams, HashMap<SmolStr, Vec<NodeParams>>>
}

trait DynClone {
    fn dyn_clone(&self) -> Self;
}

impl DynClone for HashMap<SmolStr, Box<dyn StatefulCallModifierTrait>> {
    fn dyn_clone(&self) -> Self {
        self.iter().map(|x| (x.0.clone(), x.1.dyn_box_clone())).collect()
    }
}

impl CallModifierInfo {
    fn new(
        modifier_names: Vec<SmolStr>,
        stateful_modifiers: HashMap<SmolStr, Box<dyn StatefulCallModifierTrait>>,
        stateful_affectors_and_modifiered_nodes: HashMap<NodeParams, HashMap<SmolStr, Vec<NodeParams>>>,
        stateless_node_relations: BTreeMap<SmolStr, BTreeMap<SmolStr, bool>>
    ) -> Self {
        Self {
            node_states: modifier_names.into_iter().map(|x| (x, None)).collect(),
            stateless_inner_states: HashMap::new(),
            stateless_node_relations,
            stateful_modifiers: stateful_modifiers,
            stateful_affectors_and_modified_nodes: stateful_affectors_and_modifiered_nodes,
        }
    }

    fn update_stateless_modifier_state(&mut self, stateless_modifier: &mut Box<dyn CallModifierTrait>, clause_context: &ClauseContext, function_context: &FunctionContext) {
        self.node_states.extend(
            self.stateless_node_relations.get(&function_context.current_node.node_common.name).unwrap().clone().into_iter()
            .map(|(affected_node_name, how)| (affected_node_name, Some(how)))
        );
        self.stateless_inner_states.insert(
            stateless_modifier.get_name(),
            stateless_modifier.get_new_state(clause_context, function_context)
        );
    }

    fn update_stateful_modifier_state(&mut self, clause_context: &ClauseContext, function_context: &FunctionContext) {
        let affector_node = &function_context.current_node;
        for (modifier_name, affected_nodes) in self.stateful_affectors_and_modified_nodes.get(affector_node).unwrap().clone().into_iter() {
            let modifier = self.stateful_modifiers.get_mut(&modifier_name).unwrap();
            modifier.update_state(clause_context, &function_context);
            self.node_states.extend(affected_nodes.into_iter().map(|affected_node| {
                (affected_node.node_common.name.clone(), Some(modifier.run(
                    clause_context, &function_context.with_node(affected_node.clone())
                )))
            }));
        }
    }
}

#[derive(Debug)]
pub struct StackFrame {
    /// function context, like the current node, and the
    /// current function arguments (call params)
    pub function_context: FunctionContext,
    /// various call modifier info
    call_modifier_info: CallModifierInfo,
    /// Cached version of all the dead ends
    /// in the current function
    dead_end_info: Arc<Mutex<HashMap<SmolStr, bool>>>,
    /// this contains information about the known type name list
    /// inferred when [known] is used in TYPES
    known_type_list: Option<Vec<SubgraphType>>,
    /// this contains information about the compatible type name list
    /// inferred when [compatible] is used in [TYPES]
    compatible_type_list: Option<Vec<SubgraphType>>,
}

impl StackFrame {
    fn new(function_context: FunctionContext, call_modifier_info: CallModifierInfo, dead_end_info: Arc<Mutex<HashMap<SmolStr, bool>>>) -> Self {
        Self {
            function_context,
            call_modifier_info,
            dead_end_info,
            known_type_list: None,
            compatible_type_list: None,
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
            state_chooser: Box::new(StC::new()),
            stateless_call_modifiers: HashMap::new(),
            stateful_call_modifier_creators: HashMap::new(),
            dead_end_infos: HashMap::new(),
        };
        _self.reset();
        Ok(_self)
    }

    pub fn register_call_modifier<T: CallModifierTrait + 'static>(&mut self, modifier: T) {
        self.stateless_call_modifiers.insert(modifier.get_name(), Box::new(modifier));
    }

    pub fn register_stateful_call_modifier<T: StatefulCallModifierTrait + 'static>(&mut self) {
        self.stateful_call_modifier_creators.insert(T::new().get_name(), StatefulCallModifierCreator(
            T::new
        ));
    }

    pub fn get_call_modifier_state(&self, modifier_name: &SmolStr) -> Option<&Box<dyn std::any::Any>> {
        self.call_stack.last().unwrap().call_modifier_info.stateless_inner_states.get(modifier_name)
    }

    /// used to print the call stack of the markov chain functions
    pub fn print_stack(&self) {
        println!("Call stack:");
        for stack_item in &self.call_stack {
            println!("{} ({:?} | {:?}) [{}]:", stack_item.function_context.call_params.func_name, stack_item.function_context.call_params.selected_types, stack_item.function_context.call_params.modifiers, stack_item.function_context.current_node.node_common.name)
        }
    }

    /// used when the markov chain reaches end, for the object to be iterable multiple times
    /// resets the stack to only have the entry function, query, as its pending call.
    pub fn reset(&mut self) {
        let accepted_types = unwrap_variant!(self.markov_chain.functions.get("Query").expect(
            "Graph should have an entry function named Query, with TYPES=[...]"
        ).accepted_types.clone(), FunctionTypes::TypeList);

        self.pending_call = Some(CallParams {
            func_name: SmolStr::new("Query"),
            selected_types: CallTypes::TypeList(accepted_types),  // CallTypes::TypeList(vec![SubgraphType::Numeric]),  // 
            modifiers: CallModifiers::None  // CallModifiers::StaticList(vec![SmolStr::new("single value")])  //
        });
    }

    /// push all the known data to fields of call params that
    /// were previously left unfilled
    fn fill_call_params_fields(&self, mut call_params: CallParams) -> CallParams {
        let called_function = self.markov_chain.functions.get(&call_params.func_name).unwrap();

        let mut are_wrapped = false;
        call_params.selected_types = match call_params.selected_types {
            CallTypes::KnownList => CallTypes::TypeList(self.get_known_list()),
            CallTypes::Compatible => CallTypes::TypeList(self.get_compatible_list()),
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
            CallTypes::TypeListWithFields(type_list) => CallTypes::TypeList({
                type_list.into_iter().map(|x| match x {
                    TypeWithFields::Type(tp) => vec![tp],
                    TypeWithFields::CompatibleInner(outer_tp) => self.get_compatible_list().into_iter().map(
                        |x| x.wrap_in_type(&outer_tp)
                    ).collect(),
                }).collect::<Vec<_>>().concat()
            }),
            any => any,
        };

        // if Undetermined is in the parameter list, replace the list with all acceptable arguments.
        match &call_params.selected_types {
            CallTypes::TypeList(type_list) if type_list.contains(&SubgraphType::Undetermined) => {
                are_wrapped = true;  // the accepted function types are already wrapped
                call_params.selected_types = CallTypes::TypeList(unwrap_variant!(called_function.accepted_types.clone(), FunctionTypes::TypeList));
            },
            _ => {},
        };

        // wrap all types is needed
        if called_function.uses_wrapped_types && !are_wrapped {
            call_params.selected_types = match call_params.selected_types {
                CallTypes::TypeList(type_list) => CallTypes::TypeList(
                    type_list.into_iter().map(|x| x.wrap_in_func(&call_params.func_name)).collect()
                ),
                CallTypes::None => CallTypes::None,
                any => panic!("Unexpected call_params.selected_types: {:?}", any),
            }
        }

        // finally, deal with modifiers
        call_params.modifiers = match call_params.modifiers {
            CallModifiers::StaticListWithParentMods(modifiers) => {
                CallModifiers::StaticList([
                    &{ match self.get_fn_modifiers().clone() {
                        CallModifiers::None => vec![],
                        CallModifiers::StaticListWithParentMods(_) => panic!(
                            "CallModifiers::StaticListWithParentMods was not substituted with StaticList for parent function"
                        ),
                        CallModifiers::StaticList(list) => list,
                    }}[..],
                    &modifiers[..]
                ].concat())
            },
            any => any,
        };

        call_params
    }

    /// update stack, preparing the stack frame by filling it out with all the
    /// cached data
    fn update_stack(&mut self, call_params: CallParams, clause_context: &ClauseContext) {
        let function = self.markov_chain.functions.get(&call_params.func_name).unwrap();
        let function_context = FunctionContext::new(call_params);

        let (
            stateful_affectors_and_modifiered_nodes, stateless_affectors
        ): (_, HashMap<_, _>) = function.call_modifier_affector_nodes_and_modifiered_nodes.iter().cloned().partition(
            |(affector, _)| {
                let modifier_name = affector.node_common.affects_call_modifier_name.as_ref().unwrap();
                self.stateful_call_modifier_creators.contains_key(modifier_name)
            }
        );

        let stateful_modifiers = function.call_modifier_nodes_map.iter()
            .filter_map(|(modifier_name, _)| self.stateful_call_modifier_creators.get(modifier_name).map(
                |x| (modifier_name.clone(), x.0())
            )).collect();

        let stateless_node_relations: BTreeMap<SmolStr, BTreeMap<SmolStr, bool>> = stateless_affectors.into_iter()
            .map(|(affector_node, affected)|
                (affector_node.node_common.name.clone(), affected.into_iter().flat_map(|(modifier_name, affected_nodes)| {
                    let stateless_modifier = self.stateless_call_modifiers.get(&modifier_name).unwrap();
                    let new_state = stateless_modifier.get_new_state(
                        clause_context, &function_context.with_node(affector_node.clone())
                    );
                    affected_nodes.into_iter().map(|affected_node| (affected_node.node_common.name.clone(), stateless_modifier.run(
                        clause_context,
                        &function_context.with_node(affected_node.clone()),
                        &new_state,
                    ))).collect::<Vec<_>>().into_iter()
                }).collect())
            ).collect();

        let dead_end_info = self.dead_end_infos.entry(
            (function_context.call_params.to_owned(), stateless_node_relations.clone())
        ).or_insert(Arc::new(Mutex::new(HashMap::new()))).to_owned();

        self.call_stack.push(StackFrame::new(
            function_context,
            CallModifierInfo::new(
                function.call_modifier_nodes_map.keys().cloned().collect(),
                stateful_modifiers,
                stateful_affectors_and_modifiered_nodes,
                stateless_node_relations
            ),
            dead_end_info,
        ));
    }

    /// update stack and return function's first state
    fn start_function(&mut self, call_params: CallParams, clause_context: &ClauseContext) -> SmolStr {
        let call_params = self.fill_call_params_fields(call_params);

        self.update_stack(call_params, clause_context);

        self.call_stack.last().unwrap().function_context.current_node.node_common.name.clone()
    }

    /// choose a new node among the available destibation nodes with the fynamic model
    fn update_current_node(&mut self, rng: &mut ChaCha8Rng, clause_context: &ClauseContext, dynamic_model: &mut (impl DynamicModel + ?Sized)) {
        dynamic_model.notify_call_stack_length(self.call_stack.len());

        let stack_frame = self.call_stack.last_mut().unwrap();
        // last_node guaranteed not to be an exit node
        let last_node = stack_frame.function_context.current_node.clone();
        let function = self.markov_chain.functions.get(&stack_frame.function_context.call_params.func_name).unwrap();

        let last_node_outgoing = function.chain.get(&last_node.node_common.name).unwrap();

        let last_node_outgoing = last_node_outgoing.iter().map(|el| {
            (check_node_off_dfs(rng, function, clause_context, stack_frame, &el.1), el.0, el.1.clone())
        }).collect::<Vec<_>>();

        dynamic_model.update_current_state(&last_node.node_common.name);
        let last_node_outgoing = dynamic_model.assign_log_probabilities(last_node_outgoing);

        let destination = self.state_chooser.choose_destination(last_node_outgoing);

        if let Some(destination) = destination {
            if check_node_off_dfs(rng, function, clause_context, stack_frame, &destination) {
                panic!("Chosen node is off: {} (after {})", destination.node_common.name, last_node.node_common.name);
            }
            stack_frame.function_context.current_node = destination.clone();
        } else {
            self.print_stack();
            panic!("No destination found for {}.", last_node.node_common.name);
        }
    }

    /// run all the node modifier affectors associated with the current node
    fn run_current_node_modifiers(&mut self, clause_context: &ClauseContext) {
        let stack_frame = self.call_stack.last_mut().unwrap();

        if let Some(ref modifier_name) = stack_frame.function_context.current_node.node_common.affects_call_modifier_name {
            if let Some(stateless_modifier) = self.stateless_call_modifiers.get_mut(modifier_name) {
                stack_frame.call_modifier_info.update_stateless_modifier_state(stateless_modifier, clause_context, &stack_frame.function_context);
            } else if stack_frame.call_modifier_info.stateful_modifiers.contains_key(modifier_name) {
                stack_frame.call_modifier_info.update_stateful_modifier_state(clause_context, &stack_frame.function_context);
            } else {
                panic!("No such modifier: {modifier_name}")
            }
        }
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
    pub fn set_known_list(&mut self, type_list: Vec<SubgraphType>) {
        self.call_stack.last_mut().unwrap().known_type_list = Some(type_list);
    }

    /// push the compatible type list for the next node that will use the type=[compatible]
    /// these will be wrapped automatically if uses_wrapped_types=true
    pub fn set_compatible_list(&mut self, type_list: Vec<SubgraphType>) {
        self.call_stack.last_mut().unwrap().compatible_type_list = Some(type_list);
    }

    /// pop the known types for the current node which will uses type=[known]
    fn get_known_list(&self) -> Vec<SubgraphType> {
        self.call_stack.last().unwrap().known_type_list.clone().unwrap_or_else(|| {
            self.print_stack();
            panic!("No known type list found!")
        })
    }

    /// pop the compatible types for the current node which will uses type=[compatible]
    fn get_compatible_list(&self) -> Vec<SubgraphType> {
        self.call_stack.last().unwrap().compatible_type_list.clone().unwrap_or_else(|| {
            self.print_stack();
            panic!("No compatible type list found!")
        })
    }
}

impl<StC: StateChooser> MarkovChainGenerator<StC> {
    pub fn next(&mut self, rng: &mut ChaCha8Rng, clause_context: &ClauseContext, dynamic_model: &mut (impl DynamicModel + ?Sized)) -> Option<<Self as Iterator>::Item> {
        if let Some(call_params) = self.pending_call.take() {
            return Some(self.start_function(call_params, clause_context));
        }

        if self.call_stack.is_empty() {
            self.reset();
            return None
        }

        let (is_an_exit, new_node_name) = {
            self.update_current_node(rng, clause_context, dynamic_model);
            self.run_current_node_modifiers(clause_context);

            let stack_frame = self.call_stack.last_mut().unwrap();
            self.pending_call = stack_frame.function_context.current_node.call_params.clone();
            let new_node_name = stack_frame.function_context.current_node.node_common.name.clone();
            let function = self.markov_chain.functions.get(&stack_frame.function_context.call_params.func_name).unwrap();

            (new_node_name == function.exit_node_name, new_node_name)
        };

        if is_an_exit {
            self.call_stack.pop();
        }

        Some(new_node_name)
    }
}

fn check_node_off_dfs(rng: &mut ChaCha8Rng, function: &Function, clause_context: &ClauseContext, stack_frame: &StackFrame, node_params: &NodeParams) -> bool {
    if check_node_off(&stack_frame.function_context.call_params, &stack_frame.call_modifier_info.node_states, &node_params.node_common) {
        return true
    }

    let mut dead_end_info = stack_frame.dead_end_info.lock().unwrap();
    if let Some(is_dead_end) = dead_end_info.get(&node_params.node_common.name) {
        return *is_dead_end;
    }

    // only update dead_end_info for paths that were not affected yet
    let path_is_not_affected = stack_frame.call_modifier_info.stateless_inner_states.is_empty();
    let cycles_can_unlock_paths = !stack_frame.call_modifier_info.stateful_affectors_and_modified_nodes.is_empty();

    let mut visited = HashSet::new();
    let mut to_mark_as_dead_ends = HashSet::new();
    let mut stack = VecDeque::new();
    stack.push_back((
        vec![node_params.clone()],
        stack_frame.call_modifier_info.stateful_modifiers.dyn_clone(),
        stack_frame.call_modifier_info.node_states.clone()
    ));

    while let Some((path, mut stateful_modifiers, mut affected_node_states)) = stack.pop_back() {
        let current_node = path.last().unwrap().clone();
        let current_node_name = current_node.node_common.name.clone();

        if cycles_can_unlock_paths && check_node_off(
            &stack_frame.function_context.call_params, &affected_node_states, &current_node.node_common
        ) {  // if we are ok with cycling, the node state wasn't checked while inserting
            continue;
        }

        if cycles_can_unlock_paths || visited.insert(current_node_name.clone()) {
            let mut path_until_affector: Option<Vec<SmolStr>> = if path_is_not_affected { Some(path.iter().take_until(
                |&x| stack_frame.call_modifier_info.stateless_node_relations.contains_key(&x.node_common.name)
            ).map(|x| x.node_common.name.clone()).collect()) } else { None };
            if path.iter().any(|x| stack_frame.call_modifier_info.stateful_affectors_and_modified_nodes.contains_key(x)) {
                path_until_affector = None;
            }
    
            if let Some(is_dead_end) = dead_end_info.get(&current_node_name) {
                if !*is_dead_end {
                    if let Some(path_until_affector) = path_until_affector {
                        dead_end_info.extend(path_until_affector.iter().map(|x| (x.clone(), false)));
                    }
                    return false;
                }
            }

            if current_node_name == function.exit_node_name {
                if let Some(path_until_affector) = path_until_affector {
                    dead_end_info.extend(path_until_affector.iter().map(|x| (x.clone(), false)));
                }
                return false;
            }

            if let Some(affected) = stack_frame.call_modifier_info.stateless_node_relations.get(&current_node_name) {
                affected_node_states.extend(
                    affected.clone().into_iter().map(|(affected_node_name, how)| (affected_node_name, Some(how)))
                );
            }
            if let Some(affected) = stack_frame.call_modifier_info.stateful_affectors_and_modified_nodes.get(&current_node) {
                for (modifier_name, affected_nodes) in affected.clone().into_iter() {
                    let modifier = stateful_modifiers.get_mut(&modifier_name).unwrap();
                    modifier.update_state(clause_context, &stack_frame.function_context.with_node(current_node.clone()));
                    affected_node_states.extend(affected_nodes.into_iter().map(|affected_node| {
                        (affected_node.node_common.name.clone(), Some(modifier.run(
                            clause_context, &stack_frame.function_context.with_node(affected_node.clone())
                        )))
                    }));
                }
            }
    
            if let Some(path_until_affector) = path_until_affector {
                to_mark_as_dead_ends.extend(path_until_affector.into_iter());
            }

            if let Some(node_outgoing) = function.chain.get(&current_node_name) {
                let mut outgoing: Vec<_> = node_outgoing.iter()
                    .map(|(_, node)| node.clone())
                    .filter(|x| cycles_can_unlock_paths || !check_node_off(
                        &stack_frame.function_context.call_params, &affected_node_states, &x.node_common
                    )).collect();
                if cycles_can_unlock_paths {
                    outgoing.shuffle(rng);
                } // prevent DFS looping
                stack.extend(outgoing.into_iter()
                    .map(|x| (
                        [&path[..], &[x]].concat(),
                        stateful_modifiers.dyn_clone(),
                        affected_node_states.clone()
                    ))
                );
            }
        }
    }
    if path_is_not_affected {
        dead_end_info.extend(to_mark_as_dead_ends.into_iter().map(|x| (x, true)));
    }
    true
}

fn check_node_off(
        call_params: &CallParams,
        call_modifier_states: &BTreeMap<SmolStr, Option<bool>>,
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
    if let Some((ref modifier_name, ref modifier_on)) = node_common.modifier {
        off = off || match call_params.modifiers {
            CallModifiers::None => *modifier_on,
            CallModifiers::StaticListWithParentMods(..) => panic!("CallModifiers::StaticListWithParentArgs was not substituted for CallModifiers::StaticList!"),
            CallModifiers::StaticList(ref modifiers) => {
                if modifiers.contains(&modifier_name) { !modifier_on } else { *modifier_on }
            },
        };
    }
    if let Some(ref modifier_name) = node_common.call_modifier_name {
        off = off || !call_modifier_states.get(&node_common.name).unwrap().unwrap_or_else(
            || panic!("State of call modifier {modifier_name} was not set (node {})", node_common.name)
        )
    }
    return off;
}

impl<StC: StateChooser> Iterator for MarkovChainGenerator<StC> {
    type Item = SmolStr;

    fn next(&mut self) -> Option<Self::Item> {
        self.next(&mut ChaCha8Rng::seed_from_u64(1), &mut ClauseContext::new(), &mut MarkovModel::new())
    }
}