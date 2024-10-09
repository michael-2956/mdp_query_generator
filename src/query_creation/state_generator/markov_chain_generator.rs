pub mod state_choosers;
pub mod substitute_models;
pub mod markov_chain;
pub mod subgraph_type;
pub mod error;
mod dot_parser;

use std::{path::PathBuf, collections::{HashMap, HashSet, VecDeque, BTreeMap}, sync::{Arc, Mutex}, error::Error, fmt};

use core::fmt::Debug;
use markov_chain::QueryTypes;
use rand::seq::SliceRandom;
use rand_chacha::ChaCha8Rng;
use smol_str::SmolStr;
use sqlparser::ast::Query;
use take_until::TakeUntilExt;

use crate::{config::TomlReadable, query_creation::{query_generator::{call_modifiers::{AvailableTableNamesValueSetter, CanAddMoreColumnsModifier, CanAddMoreColumnsValueSetter, CanSkipLimitModifier, CanSkipLimitValueSetter, DistinctAggregationModifier, DistinctAggregationValueSetter, FromTableNamesAvailableModifier, GroupingEnabledValueSetter, GroupingModeSwitchModifier, HasAccessibleColumnsModifier, HasAccessibleColumnsValueSetter, IsColumnTypeAvailableModifier, IsColumnTypeAvailableValueSetter, IsEmptySetAllowedModifier, IsGroupingSetsValueSetter, IsWildcardAvailableModifier, NameAccessibilityOfSelectedTypesValueSetter, NamedValue, QueryTypeNotExhaustedModifier, QueryTypeNotExhaustedValueSetter, SelectAccessibleColumnsValueSetter, SelectHasAccessibleColumnsModifier, SelectIsNotDistinctModifier, SelectIsNotDistinctValueSetter, SelectedTypesAccessibleByNamingMethodModifier, StatefulCallModifier, StatelessCallModifier, ValueSetter, ValueSetterValue, WildcardRelationsValueSetter}, query_info::ClauseContext}, state_generator::markov_chain_generator::markov_chain::FunctionTypes}, training::models::{ModelPredictionResult, PathwayGraphModel}, unwrap_variant};

use self::{
    dot_parser::{NodeCommon, TypeWithFields}, error::SyntaxError, markov_chain::{
        CallModifiers, CallParams, Function, MarkovChain, ModifierWithFields, NodeParams
    }, subgraph_type::{ContainsSubgraphType, SubgraphType}
};

use state_choosers::StateChooser;
use substitute_models::SubstituteModel;

pub use self::markov_chain::CallTypes;

#[derive(Clone)]
pub struct StateGeneratorConfig {
    pub graph_file_path: PathBuf,
}

impl TomlReadable for StateGeneratorConfig {
    fn from_toml(toml_config: &toml::Value) -> Self {
        let section = &toml_config["state_generator"];
        Self {
            graph_file_path: PathBuf::from(section["graph_file_path"].as_str().unwrap()),
        }
    }
}

#[derive(Debug)]
struct StatefulCallModifierCreator(fn() -> Box<dyn StatefulCallModifier>);

/// The markov chain generator. Runs the functional
/// subgraphs parsed from the .dot file. Manages the
/// probabilities, outputs states, disables nodes if
/// needed.
#[derive(Debug)]
pub struct MarkovChainGenerator<StC: StateChooser> {
    markov_chain: MarkovChain,
    call_stack: Vec<StackFrame>,
    pending_call: Option<CallParams>,
    /// needed if we want to know the function that we just received
    /// an exit node from
    popped_stack_frame: Option<StackFrame>,
    state_chooser: Box<StC>,
    value_setters: HashMap<SmolStr, Box<dyn ValueSetter>>,
    stateless_call_modifiers: HashMap<SmolStr, Box<dyn StatelessCallModifier>>,
    stateful_call_modifier_creators: HashMap<SmolStr, StatefulCallModifierCreator>,
    function_modifier_info: HashMap<SmolStr, FunctionModifierInfo>,
    /// if a dead_end_info was collected for the specific function once,
    /// with set arguments (types & modifiers), and all call modifier
    /// states, depending on which affectors affected what and how,
    /// we can re-use that information.
    dead_end_infos: HashMap<(CallParams, BTreeMap<SmolStr, BTreeMap<SmolStr, bool>>), Arc<Mutex<HashMap<SmolStr, bool>>>>,
}

#[derive(Debug)]
pub enum ChainStateCheckpoint {
    Full {
        call_stack: Vec<StackFrame>,
        pending_call: Option<CallParams>,
    },
    Cutoff {
        call_stack_length: usize,
        last_frame: StackFrame,
        pending_call: Option<CallParams>,
    }
}

#[derive(Debug)]
pub struct StackFrame {
    /// function context, like the current node, and the
    /// current function arguments (call params)
    pub function_context: FunctionContext,
    /// various call modifier info
    pub call_modifier_info: CallModifierInfo,
    /// Cached version of all the dead ends
    /// in the current function
    dead_end_info: Arc<Mutex<HashMap<SmolStr, bool>>>,
    /// this contains information about the known type name list
    /// inferred when [known] is used in TYPES
    known_type_list: Option<Vec<SubgraphType>>,
    /// this contains information about the known type list
    /// inferred when Q[known] is used in TYPES. This one
    /// specifies a types list for every query column
    known_query_types: Option<QueryTypes>,
    /// this contains information about the compatible type name list
    /// inferred when [compatible] is used in TYPES
    compatible_type_list: Option<Vec<SubgraphType>>,
    /// this contains information about the compatible type list
    /// inferred when Q[compatible] is used in TYPES. This one
    /// specifies a types list for every query column
    compatible_query_types: Option<QueryTypes>,
}

impl StackFrame {
    fn new(
        function_context: FunctionContext,
        call_modifier_info: CallModifierInfo,
        dead_end_info: Arc<Mutex<HashMap<SmolStr, bool>>>
    ) -> Self {
        Self {
            function_context,
            call_modifier_info,
            dead_end_info,
            known_type_list: None,
            known_query_types: None,
            compatible_type_list: None,
            compatible_query_types: None,
        }
    }
}

#[derive(Debug)]
pub struct FunctionModifierInfo {
    /// names of all stateful modifiers present in function
    stateful_modifier_names: HashSet<SmolStr>,
    /// names of all stateless modifiers present in function
    stateless_modifier_names: HashSet<SmolStr>,
    /// Modified nodes based on modifier name
    modified_nodes_map: HashMap<SmolStr, Vec<NodeParams>>,
    /// every node that affects a modifier, with the affected modifier names and vector of
    /// corresponding affected nodes
    affector_nodes_and_affected_nodes: HashMap<NodeParams, HashMap<SmolStr, Vec<NodeParams>>>,
}

impl FunctionModifierInfo {
    fn new(
        function_node_params: &HashMap<SmolStr, NodeParams>,
        stateless_call_modifiers: &HashMap<SmolStr, Box<dyn StatelessCallModifier>>,
        stateful_call_modifier_creators: &HashMap<SmolStr, StatefulCallModifierCreator>,
    ) -> Self {
        // modifier name -> modified nodes
        let modified_nodes_map = function_node_params.values()
            .filter_map(|node| {
                node.node_common.call_modifier_name.clone().map(|modifier_name| (
                    modifier_name, node.clone()
                ))
            })
            .fold(HashMap::new(), |mut acc, (key, value)| {
                acc.entry(key).or_insert_with(Vec::new).push(value);
                acc
            });

        let (stateful_modifier_names, stateless_modifier_names): (HashSet<_>, _) = modified_nodes_map.keys().cloned().partition(
            |modifier_name| stateful_call_modifier_creators.contains_key(modifier_name)
        );

        // modifier name -> associated value
        let associated_values_map: HashMap<_, _> = stateless_modifier_names.iter().filter_map(
            |modifier_name| stateless_call_modifiers.get(modifier_name).unwrap_or_else(|| panic!(
                "Stateless modifier {modifier_name} wasn't registered"
            )).get_associated_value_name().map(|associated_value| (
                modifier_name, associated_value
            ))
        ).collect();

        let affector_nodes_and_affected_nodes = function_node_params.values()
            .map(|node| {
                (
                    node.node_common.affects_call_modifier_name.as_ref(),
                    node.node_common.sets_value_name.as_ref(),
                    node
                )
            })
            .filter(|(mod_name, v_name, _)| mod_name.is_some() || v_name.is_some())
            .map(|(affects_modifier_name, sets_value_name, affector_node)| {
                let mut affected = HashMap::new();

                if let Some(modifier_name) = affects_modifier_name {
                    affected.extend(modified_nodes_map.iter()
                        .filter(|(affected_modifier_name, _)| *affected_modifier_name == modifier_name)
                        .map(|x| (x.0.clone(), x.1.clone()))
                    );
                }

                if let Some(value_name) = sets_value_name {
                    affected.extend(modified_nodes_map.iter()
                        .filter(|(affected_modifier_name, _)| associated_values_map.get(affected_modifier_name)
                            .map_or(false,|accosiated_value| accosiated_value == value_name)
                        )
                        .map(|x| (x.0.clone(), x.1.clone()))
                    );
                }

                (
                    affector_node.clone(),
                    affected
                )
            })
            .collect();

        Self {
            stateful_modifier_names,
            stateless_modifier_names,
            modified_nodes_map,
            affector_nodes_and_affected_nodes,
        }
    }

    /// return all the stateful modifiers of the current function in
    /// their initial state
    fn create_stateful_modifiers(
        &self, stateful_call_modifier_creators: &HashMap<SmolStr, StatefulCallModifierCreator>
    ) -> HashMap<SmolStr, Box<dyn StatefulCallModifier>> {
        self.stateful_modifier_names.iter()
            .filter_map(|modifier_name| stateful_call_modifier_creators.get(modifier_name).map(
                |x| (modifier_name.clone(), x.0())
            )).collect()
    }

    /// Returns how running of each value setter affects each stateless-modified node,
    /// And all the states of the valueless call modifiers
    fn get_stateless_node_relations(
        &self,
        function_context: &FunctionContext,
        clause_context: &ClauseContext,
        value_setters: &HashMap<SmolStr, Box<dyn ValueSetter>>,
        stateless_call_modifiers: &HashMap<SmolStr, Box<dyn StatelessCallModifier>>,
    ) -> (BTreeMap<SmolStr, BTreeMap<SmolStr, bool>>, BTreeMap<SmolStr, bool>) {
        let stateless_call_modifiers: HashMap<_, _> = stateless_call_modifiers.iter().filter(
            |(modifier_name, _)| self.stateless_modifier_names.contains(*modifier_name)
        ).collect();

        let stateless_affectors_iter = self.affector_nodes_and_affected_nodes.iter()
            .filter(|(affector_node, _)| {
                affector_node.node_common.sets_value_name.as_ref().map_or(false, |value_name| {
                    stateless_call_modifiers.iter().any(|(_, modifier)| {
                        if let Some(ref associated_value) = modifier.get_associated_value_name() {
                            associated_value == value_name
                        } else { false }
                    })
                })
            });

        let valueless_modifier_node_states = stateless_call_modifiers.iter().filter(
            |(_, x)| x.get_associated_value_name().is_none()
        ).flat_map(|(mod_name, modifier)| (
            self.modified_nodes_map.get(*mod_name).unwrap().iter().map(
                |node| (
                    node.node_common.name.clone(),
                    modifier.run(
                        &function_context.with_node(node.clone()),
                        None
                    )
                )
            ).collect::<Vec<_>>().into_iter()
        )).collect();

        (
            stateless_affectors_iter
                .map(|(affector_node, affected_mods)|
                    (affector_node.node_common.name.clone(), affected_mods.into_iter().flat_map(|(modifier_name, affected_nodes)| {
                        let stateless_modifier = stateless_call_modifiers.get(modifier_name).unwrap();
                        let value_name = affector_node.node_common.sets_value_name.as_ref().unwrap();
                        let value_setter = value_setters.get(value_name).unwrap_or_else(
                            || panic!("Didn't find {value_name} in the value setters (available value setters: {:?})", value_setters)
                        );
                        let new_state = value_setter.get_value(
                            clause_context, &function_context.with_node(affector_node.clone())
                        );
                        affected_nodes.into_iter().map(|affected_node| (affected_node.node_common.name.clone(), stateless_modifier.run(
                            &function_context.with_node(affected_node.clone()),
                            Some(&new_state),
                        ))).collect::<Vec<_>>().into_iter()
                    }).collect())
                ).collect(),
            valueless_modifier_node_states
        )
    }

    /// returns all modifier states in an uninitialised form, only for modifiers that
    /// have associated values, others are ignored.
    fn get_initial_affected_node_states(&self) -> HashMap<SmolStr, Option<bool>> {
        self.affector_nodes_and_affected_nodes.iter()
            .flat_map(|(_, affected_modifiers)| affected_modifiers.values())
            .flat_map(|x| x.iter())
            .map(|x| (x.node_common.name.clone(), None))
            .collect()
    }

    /// checks whether stateful affectors are present
    fn has_stateful_affectors(&self) -> bool {
        !self.stateful_modifier_names.is_empty()
    }

    /// checks whether node is a stateful affector
    fn is_a_stateful_affector(&self, node: &NodeParams) -> bool {
        if !self.affector_nodes_and_affected_nodes.contains_key(node) {
            false
        } else {
            if let Some(ref call_modifier_name) = node.node_common.affects_call_modifier_name {
                self.stateful_modifier_names.contains(call_modifier_name)
            } else {
                false
            }
        }
    }

    /// checks whether node is a stateful affector
    fn get_affected_nodes(&self, node: &NodeParams) -> &HashMap<SmolStr, Vec<NodeParams>> {
        self.affector_nodes_and_affected_nodes.get(node).unwrap()
    }
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
/// call modifiers in the current function
#[derive(Debug)]
pub struct CallModifierInfo {
    call_modifier_states: CallModifierStates,
    /// the call modifier inner state memory
    values: HashMap<SmolStr, ValueSetterValue>,
    /// call modifier configuration: How each STATELESS call modifier
    /// affector node affects every node with a call modifier (STATELESS)
    stateless_node_relations: BTreeMap<SmolStr, BTreeMap<SmolStr, bool>>,
    /// this is kept for the models so that we can retrieve the entire context
    valueless_modifier_node_states: BTreeMap<SmolStr, bool>,
}

impl CallModifierInfo {
    fn new(
        function_modifier_info: &FunctionModifierInfo,
        stateful_call_modifier_creators: &HashMap<SmolStr, StatefulCallModifierCreator>,
        stateless_node_relations: BTreeMap<SmolStr, BTreeMap<SmolStr, bool>>,
        valueless_modifier_node_states: BTreeMap<SmolStr, bool>,
    ) -> Self {
        Self {
            call_modifier_states: CallModifierStates::new(
                function_modifier_info, stateful_call_modifier_creators, valueless_modifier_node_states.clone()
            ),
            values: HashMap::new(),
            stateless_node_relations,
            valueless_modifier_node_states,
        }
    }

    /// useful for the models to get info about the
    /// function's call modifier context
    pub fn get_context(&self) -> (
        BTreeMap<SmolStr, BTreeMap<SmolStr, bool>>, BTreeMap<SmolStr, bool>,
    ) {
        (
            self.stateless_node_relations.clone(),
            self.valueless_modifier_node_states.clone(),
        )
    }

    fn update_modifier_states(
        &mut self,
        function_context: &FunctionContext,
        function_modifier_info: &FunctionModifierInfo,
        clause_context: &ClauseContext,
        value_setters: &HashMap<SmolStr, Box<dyn ValueSetter>>,
    ) {
        self.call_modifier_states.update_modifier_states(
            function_context,
            function_modifier_info,
            &self.stateless_node_relations,
            clause_context
        );
        if let Some(ref value_name) = function_context.current_node.node_common.sets_value_name {
            self.values.insert(
                value_name.clone(),
                value_setters.get(value_name).unwrap().get_value(clause_context, &function_context)
            );
        }
    }
}

/// Stires the states of all affected nodes and all stateful call modifiers
#[derive(Debug)]
struct CallModifierStates {
    /// stores current node states that are affected by call modifiers
    affected_node_states: HashMap<SmolStr, Option<bool>>,
    /// stores all the stateful modifiers of the current function in
    /// their current state
    stateful_modifiers: HashMap<SmolStr, Box<dyn StatefulCallModifier>>,
}

pub trait DynClone {
    fn dyn_clone(&self) -> Self;
}

impl DynClone for HashMap<SmolStr, Box<dyn StatefulCallModifier>> {
    fn dyn_clone(&self) -> Self {
        self.iter().map(|x| (x.0.clone(), x.1.dyn_box_clone())).collect()
    }
}

impl DynClone for CallModifierStates {
    fn dyn_clone(&self) -> Self {
        Self {
            affected_node_states: self.affected_node_states.clone(),
            stateful_modifiers: self.stateful_modifiers.dyn_clone(),
        }
    }
}

impl DynClone for CallModifierInfo {
    fn dyn_clone(&self) -> Self {
        Self {
            call_modifier_states: self.call_modifier_states.dyn_clone(),
            values: self.values.clone(),
            stateless_node_relations: self.stateless_node_relations.clone(),
            valueless_modifier_node_states: self.valueless_modifier_node_states.clone(),
        }
    }
}

impl DynClone for StackFrame {
    fn dyn_clone(&self) -> Self {
        Self {
            function_context: self.function_context.clone(),
            call_modifier_info: self.call_modifier_info.dyn_clone(),
            dead_end_info: self.dead_end_info.clone(),
            known_type_list: self.known_type_list.clone(),
            known_query_types: self.known_query_types.clone(),
            compatible_type_list: self.compatible_type_list.clone(),
            compatible_query_types: self.compatible_query_types.clone(),
        }
    }
}

impl DynClone for Vec<StackFrame> {
    fn dyn_clone(&self) -> Self {
        self.iter().map(|x| x.dyn_clone()).collect()
    }
}

impl DynClone for ChainStateCheckpoint {
    fn dyn_clone(&self) -> Self {
        match self {
            Self::Full { call_stack, pending_call } => Self::Full {
                call_stack: call_stack.dyn_clone(),
                pending_call: pending_call.clone(),
            },
            Self::Cutoff { call_stack_length, last_frame, pending_call } => Self::Cutoff {
                call_stack_length: *call_stack_length,
                last_frame: last_frame.dyn_clone(),
                pending_call: pending_call.clone(),
            },
        }
    }
}

impl CallModifierStates {
    fn new(
        function_modifier_info: &FunctionModifierInfo,
        stateful_call_modifier_creators: &HashMap<SmolStr, StatefulCallModifierCreator>,
        valueless_modifier_node_states: BTreeMap<SmolStr, bool>,
    ) -> Self {
        let mut _self = Self {
            affected_node_states: function_modifier_info.get_initial_affected_node_states(),
            stateful_modifiers: function_modifier_info.create_stateful_modifiers(stateful_call_modifier_creators)
        };
        _self.affected_node_states.extend(valueless_modifier_node_states.into_iter().map(
            |(node_name, state)| (node_name, Some(state))
        ));
        _self
    }

    fn update_modifier_states(
        &mut self,
        function_context: &FunctionContext,
        function_modifier_info: &FunctionModifierInfo,
        stateless_node_relations: &BTreeMap<SmolStr, BTreeMap<SmolStr, bool>>,
        clause_context: &ClauseContext,
    ) {
        let node = &function_context.current_node;

        // update with a cached influence result
        if let Some(affected) = stateless_node_relations.get(&node.node_common.name) {
            // check is any value setter affects a node twice. This is not
            // permitted and one should use a stateful call modifier instead
            // TODO: refactor so that this actually can be forbidden
            // if let Some((node_name, Some(state))) = self.affected_node_states.iter().find(|(node_name, state)| {
            //     state.is_some() && affected.contains_key(*node_name)
            // }) {
            //     panic!(
            //         "A value setter {} tried to affect node {node_name}, but its state\
            //         has been already set to: {state}, and cannot be set twice",
            //         node.node_common.sets_value_name.as_ref().unwrap()
            //     )
            // };

            // update node states
            self.affected_node_states.extend(
                affected.clone().into_iter().map(|(affected_node_name, how)| (affected_node_name, Some(how)))
            );
        }

        // update stateful affector's states
        if function_modifier_info.is_a_stateful_affector(node) {
            let affected_modifiers = function_modifier_info.get_affected_nodes(node);
            for (modifier_name, affected_nodes) in affected_modifiers.iter() {
                if let Some(modifier) = self.stateful_modifiers.get_mut(modifier_name) {
                    modifier.update_state(clause_context, &function_context.with_node(node.clone()));
                    self.affected_node_states.extend(affected_nodes.into_iter().map(|affected_node| {
                        (affected_node.node_common.name.clone(), Some(modifier.run(
                            clause_context, &function_context.with_node(affected_node.clone())
                        )))
                    }));
                }
            }
        }
    }

    fn has_path_been_affected(&self) -> bool {
        self.affected_node_states.iter().any(|(_, state)| state.is_some())
    }
}

impl<StC: StateChooser> MarkovChainGenerator<StC> {
    pub fn with_config(config: &StateGeneratorConfig) -> Result<Self, SyntaxError> {
        let chain = MarkovChain::parse_dot(&config.graph_file_path)?;
        let mut _self = MarkovChainGenerator::<StC> {
            markov_chain: chain,
            call_stack: vec![],
            pending_call: None,
            popped_stack_frame: None,
            state_chooser: Box::new(StC::new()),
            value_setters: HashMap::new(),
            stateless_call_modifiers: HashMap::new(),
            stateful_call_modifier_creators: HashMap::new(),
            function_modifier_info: HashMap::new(),
            dead_end_infos: HashMap::new(),
        };

        _self.register_value_setter(CanSkipLimitValueSetter {});
        _self.register_stateless_call_modifier(CanSkipLimitModifier {});
        _self.register_value_setter(IsGroupingSetsValueSetter {});
        _self.register_stateless_call_modifier(IsEmptySetAllowedModifier {});
        _self.register_value_setter(GroupingEnabledValueSetter {});
        _self.register_stateless_call_modifier(GroupingModeSwitchModifier {});
        _self.register_value_setter(CanAddMoreColumnsValueSetter {});
        _self.register_stateless_call_modifier(CanAddMoreColumnsModifier {});
        _self.register_value_setter(QueryTypeNotExhaustedValueSetter {});
        _self.register_stateless_call_modifier(QueryTypeNotExhaustedModifier {});
        _self.register_value_setter(WildcardRelationsValueSetter {});
        _self.register_stateless_call_modifier(IsWildcardAvailableModifier {});
        _self.register_value_setter(AvailableTableNamesValueSetter {});
        _self.register_stateless_call_modifier(FromTableNamesAvailableModifier {});
        _self.register_value_setter(DistinctAggregationValueSetter {});
        _self.register_stateless_call_modifier(DistinctAggregationModifier {});
        _self.register_value_setter(SelectIsNotDistinctValueSetter {});
        _self.register_stateless_call_modifier(SelectIsNotDistinctModifier {});
        _self.register_value_setter(HasAccessibleColumnsValueSetter {});
        _self.register_stateless_call_modifier(HasAccessibleColumnsModifier {});
        _self.register_value_setter(IsColumnTypeAvailableValueSetter {});
        _self.register_stateless_call_modifier(IsColumnTypeAvailableModifier {});
        _self.register_value_setter(SelectAccessibleColumnsValueSetter {});
        _self.register_stateless_call_modifier(SelectHasAccessibleColumnsModifier {});
        _self.register_value_setter(NameAccessibilityOfSelectedTypesValueSetter {});
        _self.register_stateless_call_modifier(SelectedTypesAccessibleByNamingMethodModifier {});

        _self.fill_function_modifier_info();
        _self.reset();

        Ok(_self)
    }

    pub fn get_last_popped_stack_frame_ref(&self) -> Option<&StackFrame> {
        self.popped_stack_frame.as_ref()
    }

    pub fn markov_chain_ref(&self) -> &MarkovChain {
        &self.markov_chain
    }

    pub fn call_stack_ref(&self) -> &Vec<StackFrame> {
        &self.call_stack
    }

    fn register_value_setter<T: ValueSetter + 'static>(&mut self, value_setter: T) {
        self.value_setters.insert(value_setter.get_value_name(), Box::new(value_setter));
    }

    fn register_stateless_call_modifier<T: StatelessCallModifier + 'static>(&mut self, modifier: T) {
        self.stateless_call_modifiers.insert(modifier.get_name(), Box::new(modifier));
    }

    /// Do not use.\
    /// Unstable feature, left so that we can re-implement it\
    /// once and if it is actually ever needed
    fn _register_stateful_call_modifier<T: StatefulCallModifier + 'static>(&mut self) {
        self.stateful_call_modifier_creators.insert(T::new().get_name(), StatefulCallModifierCreator(
            T::new
        ));
        todo!("
            Unstable feature, left so that we can re-implement it once and if
            it is actually ever needed.
            This may work fine, but has not been tested to do so in a while

            The problem is that the models won't be able to get the full function
            characteristic with call modifiers in place, since they can be called
            unlimited number of times, unlike stateless modifiers that depend on
            a limited number of value setters.

            This can be solved if we add 'characteristic' that the stateful modifiers
            would produce for themselves, so that models would be able to differentiate
            between different contexts.
            
            Until then, this function should not be used
        ");
    }

    fn fill_function_modifier_info(&mut self) {
        self.function_modifier_info = self.markov_chain.functions.iter().map(|(function_name, function)| {
            (function_name.clone(), FunctionModifierInfo::new(
                &function.node_params,
                &self.stateless_call_modifiers,
                &self.stateful_call_modifier_creators
            ))
        }).collect();
    }

    pub fn has_pending_call(&self) -> bool {
        self.pending_call.is_some()
    }

    pub fn get_named_value<T: NamedValue + 'static>(&self) -> Option<&ValueSetterValue> {
        self.call_stack.last().unwrap().call_modifier_info.values.get(&T::name())
    }

    /// used to print the call stack of the markov chain functions
    pub fn print_stack(&self) {
        eprintln!("Call stack:");
        for stack_item in &self.call_stack {
            eprintln!("{} ({:?} | {:?}) [{}]:", stack_item.function_context.call_params.func_name, stack_item.function_context.call_params.selected_types, stack_item.function_context.call_params.modifiers, stack_item.function_context.current_node.node_common.name)
        }
    }

    /// used when the markov chain reaches end, for the object to be iterable multiple times
    /// resets the stack to only have the entry function, query, as its pending call.
    pub fn reset(&mut self) {
        let accepted_types = unwrap_variant!(self.markov_chain.functions.get("Query").expect(
            "Graph should have an entry function named Query, with TYPES=[...]"
        ).accepted_types.clone(), FunctionTypes::QueryTypeList);

        self.pending_call = Some(CallParams {
            func_name: SmolStr::new("Query"),
            selected_types: CallTypes::QueryTypes(QueryTypes::TypeList {
                type_list: accepted_types,
            }),
            types_have_columns: true,
            modifiers: CallModifiers::None,
        });
    }

    pub fn get_chain_state_checkpoint(&self, cutoff: bool) -> ChainStateCheckpoint {
        if cutoff {
            ChainStateCheckpoint::Cutoff {
                call_stack_length: self.call_stack.len(),
                last_frame: self.call_stack.last().as_ref().unwrap().dyn_clone(),
                pending_call: self.pending_call.clone(),
            }
        } else {
            ChainStateCheckpoint::Full {
                call_stack: self.call_stack.dyn_clone(),
                pending_call: self.pending_call.clone(),
            }
        }
    }

    pub fn restore_chain_state(&mut self, chain_state_checkpoint: ChainStateCheckpoint) {
        match chain_state_checkpoint {
            ChainStateCheckpoint::Full { call_stack, pending_call } => {
                self.call_stack = call_stack;
                self.pending_call = pending_call;
            },
            ChainStateCheckpoint::Cutoff { call_stack_length, last_frame, pending_call } => {
                self.call_stack.truncate(call_stack_length);
                *self.call_stack.last_mut().unwrap() = last_frame;
                self.pending_call = pending_call;
            },
        }
    }

    /// push all the known data to fields of call params that
    /// were previously left unfilled
    fn fill_call_params_fields(&self, mut call_params: CallParams) -> CallParams {
        let called_function = self.markov_chain.functions.get(&call_params.func_name).unwrap();

        call_params.selected_types = if call_params.types_have_columns {
            match call_params.selected_types {
                CallTypes::PassThrough => self.get_fn_selected_types_unwrapped().clone(),
                CallTypes::Compatible => CallTypes::QueryTypes(self.get_compatible_query_type_list()),
                CallTypes::KnownList => CallTypes::QueryTypes(self.get_known_query_type_list()),
                // this can be a list of types for every of the columns, but for now unspecified
                CallTypes::TypeListWithFields(..) => unimplemented!(),
                CallTypes::TypeList(_) => panic!("Not allowed: call_params.types_have_columns is true"),
                any @ (CallTypes::None | CallTypes::QueryTypes(..)) => any,
            }
        } else {
            match call_params.selected_types {
                CallTypes::PassThrough => self.get_fn_selected_types_unwrapped().clone(),
                CallTypes::Compatible => CallTypes::TypeList(self.get_compatible_list()),
                CallTypes::KnownList => CallTypes::TypeList(self.get_known_list()),
                CallTypes::TypeListWithFields(type_list) => CallTypes::TypeList(
                    type_list.into_iter().map(|TypeWithFields::Type(tp)| tp).collect()
                ),
                CallTypes::QueryTypes(_) => panic!("Not allowed: call_params.types_have_columns is false"),
                any @ (CallTypes::None | CallTypes::TypeList(..)) => any,
            }
        };

        // if Undetermined is in the parameter list, replace the list with all acceptable arguments.
        match &call_params.selected_types {
            CallTypes::TypeList(type_list) if type_list.contains(&SubgraphType::Undetermined) => {
                call_params.selected_types = CallTypes::TypeList(unwrap_variant!(called_function.accepted_types.clone(), FunctionTypes::TypeList));
            },
            _ => {},
        };

        // finally, deal with modifiers
        call_params.modifiers = match call_params.modifiers {
            CallModifiers::PassThroughWithAddedMods(modifiers) => {
                let mut parent_mods = self.get_fn_modifiers().clone().to_vec();
                let cancel_mods = modifiers.iter().filter_map(|m| match m {
                    ModifierWithFields::CancelModifier(ref cancel_mod) => Some(cancel_mod.clone()),
                    _ => None,
                }).collect::<Vec<_>>();
                parent_mods.retain(|m| !cancel_mods.contains(m));
                CallModifiers::StaticList([
                    parent_mods,
                    modifiers.into_iter().filter_map(|x| match x {
                        ModifierWithFields::Modifier(modifier_name) => Some(modifier_name),
                        ModifierWithFields::CancelModifier(..) => None,
                        ModifierWithFields::PassThrough(..) => None,
                    }).collect(),
                ].concat())
            },
            CallModifiers::StaticListWithFields(modifiers) => {
                let parent_mods = self.get_fn_modifiers();
                CallModifiers::StaticList(modifiers.into_iter().filter_map(|x| match x {
                    ModifierWithFields::Modifier(modifier_name) => Some(modifier_name),
                    ModifierWithFields::PassThrough(modifier_name) => {
                        if parent_mods.contains(&modifier_name) {
                            Some(modifier_name)
                        } else { None }
                    },
                    ModifierWithFields::CancelModifier(..) => None,
                }).collect())
            },
            CallModifiers::None => {
                if called_function.accepted_modifiers.is_some() {
                    CallModifiers::StaticList(vec![])
                } else {
                    CallModifiers::None
                }
            },
            any @ CallModifiers::StaticList(..) => any,
        };

        call_params
    }

    /// update stack, preparing the stack frame by filling it out with all the
    /// cached data
    fn update_stack(&mut self, call_params: CallParams, clause_context: &ClauseContext) {
        let function_modifier_info = self.function_modifier_info.get(&call_params.func_name).unwrap();
        let function_context = FunctionContext::new(call_params);

        let (
            stateless_node_relations,
            valueless_modifier_node_states
        ) = function_modifier_info.get_stateless_node_relations(
            &function_context, clause_context, &self.value_setters, &self.stateless_call_modifiers
        );

        let dead_end_info = self.dead_end_infos.entry(
            (function_context.call_params.to_owned(), stateless_node_relations.clone())
        ).or_insert(Arc::new(Mutex::new(HashMap::new()))).to_owned();

        self.call_stack.push(StackFrame::new(
            function_context,
            CallModifierInfo::new(
                function_modifier_info,
                &self.stateful_call_modifier_creators, 
                stateless_node_relations,
                valueless_modifier_node_states,
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

    /// choose a new node among the available destibation nodes with the dynamic model
    fn update_current_node(
            &mut self,
            rng: &mut ChaCha8Rng,
            clause_context: &ClauseContext,
            substitute_model: &mut (impl SubstituteModel + ?Sized),
            predictor_model_opt: Option<&mut Box<dyn PathwayGraphModel>>,
            current_query_ast_opt: Option<&Query>,
        ) -> Result<(), StateGenerationError> {
        substitute_model.notify_call_stack_length(self.call_stack.len());

        let (last_node, last_node_outgoing) = {
            let stack_frame = self.call_stack.last().unwrap();
            // last_node guaranteed not to be an exit node
            let last_node = stack_frame.function_context.current_node.clone();
            let function = self.markov_chain.functions.get(&stack_frame.function_context.call_params.func_name).unwrap();
            let function_modifier_info = self.function_modifier_info.get(
                &stack_frame.function_context.call_params.func_name
            ).unwrap();

            let last_node_outgoing = function.chain.get(&last_node.node_common.name).unwrap();

            let last_node_outgoing = last_node_outgoing.iter().filter_map(|el| {
                if check_node_off_dfs(rng, function, function_modifier_info, clause_context, stack_frame, &el.1) {
                    None
                } else { Some(el.1.clone()) }
            }).collect::<Vec<_>>();

            substitute_model.update_current_state(&last_node.node_common.name);

            (last_node, last_node_outgoing)
        };

        // probability distribution recorded in model
        let last_node_outgoing = if let Some(predictor_model) = predictor_model_opt {
            predictor_model.predict(&self.call_stack, last_node_outgoing, current_query_ast_opt)
        } else { ModelPredictionResult::None(last_node_outgoing) };

        // transform to log probabulities, if no model is present run a dynamic one
        let last_node_outgoing = match last_node_outgoing {
            ModelPredictionResult::Some(predictions) => {
                let prob_sum: f64 = predictions.iter().map(|x| x.0).sum();
                if (prob_sum - 1f64).abs() > std::f64::EPSILON {
                    panic!("Model predicted probabilities with a sum of {}, should have been close to 1.", prob_sum)
                }
                let preds = predictions.into_iter().map(|(w, node)| (
                    w.ln(), node
                )).collect();
                // substitute_model.trasform_log_probabilities(preds)?
                preds
            },
            ModelPredictionResult::None(outgoing_nodes) => {
                let fill_with = - (outgoing_nodes.len() as f64).ln();
                let uniform = outgoing_nodes.into_iter().map(|node| (fill_with, node)).collect();
                substitute_model.trasform_log_probabilities(uniform)?
            },
        };

        // normalize the log-probs and choose
        let destination = self.state_chooser.choose_destination(rng, last_node_outgoing);

        let stack_frame = self.call_stack.last_mut().unwrap();

        if let Some(destination) = destination {
            stack_frame.function_context.current_node = destination.clone();
        } else {
            let function_context = stack_frame.function_context.clone();
            self.print_stack();
            panic!("No destination found for {} in {:#?}.\n\nCLAUSE CONTEXT: {:#?}", last_node.node_common.name, function_context, clause_context);
        }
        Ok(())
    }

    /// get current function inputs list
    pub fn get_fn_selected_types_unwrapped(&self) -> CallTypes {
        let function_context = &self.call_stack.last().unwrap().function_context;
        let selected_types = function_context.call_params.selected_types.clone();
        selected_types
    }

    /// get crrent function modifiers list
    pub fn get_fn_modifiers(&self) -> &CallModifiers {
       &self.call_stack.last().unwrap().function_context.call_params.modifiers
    }

    /// get crrent function modifiers list, if any function is running now
    /// (would be None right before and after the main function was called for the first time)
    pub fn get_fn_modifiers_opt(&self) -> Option<&CallModifiers> {
        self.call_stack.last().map(
            |sf| &sf.function_context.call_params.modifiers     
        )
     }

    pub fn get_pending_call_accepted_types(&self) -> FunctionTypes {
        self.markov_chain.functions.get(&self.pending_call.as_ref().unwrap().func_name).unwrap().accepted_types.clone()
    }

    pub fn get_pending_call_accepted_modifiers(&self) -> Option<Vec<SmolStr>> {
        self.markov_chain.functions.get(&self.pending_call.as_ref().unwrap().func_name).unwrap().accepted_modifiers.clone()
    }

    /// set the known type list for the next node that will use the TYPES="[known]"
    /// these will be wrapped automatically if uses_wrapped_types=true
    pub fn set_known_list(&mut self, type_list: Vec<SubgraphType>) {
        self.call_stack.last_mut().unwrap().known_type_list = Some(type_list);
    }

    /// set the compatible type list for the next node that will use the TYPES="[compatible]"
    /// these will be wrapped automatically if uses_wrapped_types=true
    pub fn set_compatible_list(&mut self, type_list: Vec<SubgraphType>) {
        self.call_stack.last_mut().unwrap().compatible_type_list = Some(type_list);
    }
    
    /// set the compatible column type list for the next node that will use the TYPES="Q[compatible]"
    pub fn set_compatible_query_type_list(&mut self, column_type_list: QueryTypes) {
        self.call_stack.last_mut().unwrap().compatible_query_types = Some(column_type_list);
    }

    /// set the known column type list for the next node that will use the TYPES="Q[known]"
    pub fn set_known_query_type_list(&mut self, column_type_list: QueryTypes) {
        self.call_stack.last_mut().unwrap().known_query_types = Some(column_type_list);
    }

    /// pop the known types for the current node which will uses TYPES="[known]"
    fn get_known_list(&self) -> Vec<SubgraphType> {
        self.call_stack.last().unwrap().known_type_list.clone().unwrap_or_else(|| {
            self.print_stack();
            panic!("No known type list found!")
        })
    }

    /// pop the compatible types for the current node which will uses TYPES="[compatible]"
    fn get_compatible_list(&self) -> Vec<SubgraphType> {
        self.call_stack.last().unwrap().compatible_type_list.clone().unwrap_or_else(|| {
            self.print_stack();
            panic!("No compatible type list found!")
        })
    }

    /// pop the compatible types for the current node which will uses TYPES="Q[compatible]"
    fn get_compatible_query_type_list(&self) -> QueryTypes {
        self.call_stack.last().unwrap().compatible_query_types.clone().unwrap_or_else(|| {
            self.print_stack();
            panic!("No compatible query type list found!")
        })
    }

    /// pop the knoen types for the current node which will uses TYPES="Q[known]"
    fn get_known_query_type_list(&self) -> QueryTypes {
        self.call_stack.last().unwrap().known_query_types.clone().unwrap_or_else(|| {
            self.print_stack();
            panic!("No compatible query type list found!")
        })
    }
}

#[derive(Debug)]
pub struct StateGenerationError {
    pub reason: String,
}

impl Error for StateGenerationError { }

impl fmt::Display for StateGenerationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "State generation convertion error: {}", self.reason)
    }
}

impl StateGenerationError {
    pub fn new(reason: String) -> Self {
        Self { reason }
    }
}

impl<StC: StateChooser> MarkovChainGenerator<StC> {
    pub fn next_node_name(
            &mut self, rng: &mut ChaCha8Rng,
            clause_context: &ClauseContext,
            substitute_model: &mut (impl SubstituteModel + ?Sized),
            predictor_model_opt: Option<&mut Box<dyn PathwayGraphModel>>,
            current_query_ast_opt: Option<&Query>,
        ) -> Result<Option<SmolStr>, StateGenerationError> {
        if let Some(call_params) = self.pending_call.take() {
            return Ok(Some(self.start_function(call_params, clause_context)));
        }

        if self.call_stack.is_empty() {
            self.reset();
            return Ok(None)
        }

        let (is_an_exit, new_node_name) = {
            self.update_current_node(rng, clause_context, substitute_model, predictor_model_opt, current_query_ast_opt)?;

            let stack_frame = self.call_stack.last_mut().unwrap();

            let function_modifier_info = self.function_modifier_info.get(&stack_frame.function_context.call_params.func_name).unwrap();
            stack_frame.call_modifier_info.update_modifier_states(
                &stack_frame.function_context, function_modifier_info, clause_context, &self.value_setters
            );

            self.pending_call = stack_frame.function_context.current_node.call_params.clone();

            let new_node_name = stack_frame.function_context.current_node.node_common.name.clone();
            let function = self.markov_chain.functions.get(&stack_frame.function_context.call_params.func_name).unwrap();
            (new_node_name == function.exit_node_name, new_node_name)
        };

        if is_an_exit {
            self.popped_stack_frame = self.call_stack.pop();
        }

        Ok(Some(new_node_name))
    }
}

fn check_node_off_dfs(
        rng: &mut ChaCha8Rng,
        function: &Function,
        function_modifier_info: &FunctionModifierInfo,
        clause_context: &ClauseContext,
        stack_frame: &StackFrame,
        node_params: &NodeParams
    ) -> bool {
    if check_node_off(&stack_frame.function_context.call_params, &stack_frame.call_modifier_info.call_modifier_states.affected_node_states, &node_params.node_common) {
        return true
    }

    let mut dead_end_info = stack_frame.dead_end_info.lock().unwrap();
    if let Some(is_dead_end) = dead_end_info.get(&node_params.node_common.name) {
        return *is_dead_end;
    }

    // only update dead_end_info for paths that were not affected yet
    let path_not_yet_affected = !stack_frame.call_modifier_info.call_modifier_states.has_path_been_affected();
    let cycles_can_unlock_paths = function_modifier_info.has_stateful_affectors();

    let mut visited = HashSet::new();
    let mut cacheable_as_dead_ends = HashSet::new();
    let mut stack = VecDeque::new();
    stack.push_back((
        vec![node_params.clone()],
        stack_frame.call_modifier_info.call_modifier_states.dyn_clone(),
    ));

    while let Some((path, mut call_modifier_states)) = stack.pop_back() {
        let current_node = path.last().unwrap().clone();
        let current_node_name = current_node.node_common.name.clone();

        if cycles_can_unlock_paths || visited.insert(current_node_name.clone()) {
            let cacheable_path_part: Vec<SmolStr> = if path_not_yet_affected && path.iter().all(
                    |x| !function_modifier_info.is_a_stateful_affector(x)
                ) {
                path.iter().take_until(
                    |&x| stack_frame.call_modifier_info.stateless_node_relations.contains_key(&x.node_common.name)
                ).map(|x| x.node_common.name.clone()).collect()
            } else {
                vec![]
            };

            if let Some(is_dead_end) = dead_end_info.get(&current_node_name) {
                if !*is_dead_end {
                    dead_end_info.extend(cacheable_path_part.into_iter().map(|x| (x, false)));
                    return false;
                }
            }

            if current_node_name == function.exit_node_name {
                dead_end_info.extend(cacheable_path_part.into_iter().map(|x| (x, false)));
                return false;
            }

            call_modifier_states.update_modifier_states(
                &stack_frame.function_context.with_node(current_node.clone()),
                function_modifier_info,
                &stack_frame.call_modifier_info.stateless_node_relations,
                clause_context
            );
    
            cacheable_as_dead_ends.extend(cacheable_path_part.into_iter());

            if let Some(node_outgoing) = function.chain.get(&current_node_name) {
                let mut outgoing: Vec<_> = node_outgoing.iter()
                    .map(|(_, node)| node.clone())
                    .filter(|x| !check_node_off(
                        &stack_frame.function_context.call_params, &call_modifier_states.affected_node_states, &x.node_common
                    )).collect();
                if cycles_can_unlock_paths {
                    outgoing.shuffle(rng);
                } // prevent DFS looping
                stack.extend(outgoing.into_iter()
                    .map(|x| (
                        [&path[..], &[x]].concat(),
                        call_modifier_states.dyn_clone(),
                    ))
                );
            }
        }
    }
    dead_end_info.extend(cacheable_as_dead_ends.into_iter().map(|x| (x, true)));
    true
}

fn check_node_off(
        call_params: &CallParams,
        affected_node_states: &HashMap<SmolStr, Option<bool>>,
        node_common: &NodeCommon
    ) -> bool {
    let mut off = false;
    if let Some(ref type_names) = node_common.type_names {
        off = match call_params.selected_types {
            CallTypes::None => true,
            CallTypes::TypeList(ref t_name_list) => !t_name_list.iter()
                .any(|x| type_names.contains_generator_of(x)),
            _ => panic!("Expected None or TypeNameList for function selected types")
        };
    }
    if let Some((ref modifier_name, ref modifier_on)) = node_common.modifier {
        off = off || if call_params.modifiers.contains(&modifier_name) {
            !modifier_on
        } else {
            *modifier_on
        }
    }
    if let Some(ref modifier_name) = node_common.call_modifier_name {
        off = off || !affected_node_states.get(&node_common.name).unwrap_or_else(
            || panic!("Didn't find {} in affected_node_states. Check if the node's modifier associated value was set properly.", node_common.name)
        ).unwrap_or_else(
            || panic!("State of call modifier {modifier_name} was not set (node {})", node_common.name)
        )
    }
    return off;
}
