pub mod state_choosers;
pub mod dynamic_models;
mod markov_chain;
mod dot_parser;
mod error;

use std::{path::Path, collections::HashMap};

use core::fmt::Debug;
use smol_str::SmolStr;

use crate::{unwrap_variant, query_creation::{state_generators::markov_chain_generator::markov_chain::FunctionTypes, random_query_generator::query_info::QueryContextManager}};

use self::{
    markov_chain::{
        MarkovChain, NodeParams, CallParams, CallModifiers
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

/// The markov chain generator. Runs the functional
/// subgraphs parsed from the .dot file. Manages the
/// probabilities, outputs states, disables nodes if
/// needed.
#[derive(Debug, Clone)]
pub struct MarkovChainGenerator<StC: StateChooser> {
    /// this stack contains information about the known type name lists
    /// inferred when [known] is used in TYPES
    known_type_list_stack: Vec<Vec<SubgraphType>>,
    /// this stack contains information about the known type names
    /// inferred when known is used in TYPE
    known_type_name_stack: Vec<SubgraphType>,
    /// this stack contains information about the compatible type names
    /// inferred when [compatible] is used in [TYPES]
    compatible_type_name_stack: Vec<Vec<SubgraphType>>,
    markov_chain: MarkovChain,
    call_stack: Vec<StackItem>,
    pending_call: Option<CallParams>,
    state_chooser: Box<StC>,
    call_triggers: HashMap<SmolStr, CallTrigger>,
}

#[derive(Clone, Debug)]
struct StackItem {
    function_params: CallParams,
    current_node_name: SmolStr,
    next_node: NodeParams,
}

impl StackItem {
    fn from_call_params(call_params: CallParams) -> Self {
        let func_name = call_params.func_name.clone();
        Self {
            function_params: call_params,
            current_node_name: SmolStr::new("-"),
            next_node: NodeParams {
                node_common: NodeCommon::with_name(func_name),
                call_params: None,
                literal: false,
                min_calls_until_function_exit: 0,
            }
        }
    }
}

impl<StC: StateChooser> MarkovChainGenerator<StC> {
    pub fn parse_graph_from_file<P: AsRef<Path>>(source_path: P) -> Result<Self, SyntaxError> {
        let chain = MarkovChain::parse_dot(source_path)?;
        let mut self_ = MarkovChainGenerator::<StC> {
            markov_chain: chain,
            call_stack: vec![],
            pending_call: None,
            known_type_list_stack: vec![],
            known_type_name_stack: vec![],
            compatible_type_name_stack: vec![],
            state_chooser: Box::new(StC::new()),
            call_triggers: HashMap::new(),
        };
        self_.reset();
        Ok(self_)
    }

    pub fn register_call_trigger(&mut self, trigger_name: SmolStr, call_trigger: fn(&QueryContextManager) -> bool) {
        self.call_triggers.insert(trigger_name, CallTrigger(call_trigger));
    }

    /// used to print the call stack of the markov chain functions
    pub fn print_stack(&self) {
        println!("Call stack:");
        for stack_item in &self.call_stack {
            println!("{} ({:?}) [{}]:", stack_item.function_params.func_name, stack_item.function_params.selected_types, stack_item.current_node_name)
        }
    }

    /// used when the markov chain reaches end, for the object to be iterable multiple times
    pub fn reset(&mut self) {
        let accepted_types = unwrap_variant!(self.markov_chain.functions.get("Query").expect(
            "Graph should have an entry function named Query, with TYPES=[...]"
        ).accepted_types.clone(), FunctionTypes::TypeList);
        self.call_stack = vec![StackItem::from_call_params(CallParams {
            func_name: SmolStr::new("Query"),
            selected_types: CallTypes::TypeList(accepted_types),  // CallTypes::TypeList(vec![SubgraphType::Numeric]),  // 
            modifiers: CallModifiers::None  // CallModifiers::StaticList(vec![SmolStr::new("single value")])  //
        })];
        self.known_type_list_stack = vec![];
        self.compatible_type_name_stack = vec![];
    }

    /// get current function inputs list
    pub fn get_fn_selected_types(&self) -> CallTypes {
        self.call_stack.last().unwrap().function_params.selected_types.clone()
    }

    /// get crrent function modifiers list
    pub fn get_fn_modifiers(&self) -> CallModifiers {
        self.call_stack.last().unwrap().function_params.modifiers.clone()
    }

    /// push the known type for the next node that will use the type=known
    pub fn push_known(&mut self, type_name: SubgraphType) {
        self.known_type_name_stack.push(type_name);
    }

    /// push the known type list for the next node that will use the types=[known]
    pub fn push_known_list(&mut self, type_list: Vec<SubgraphType>) {
        self.known_type_list_stack.push(type_list);
    }

    /// push the compatible type list for the next node that will use the type=[compatible]
    pub fn push_compatible_list(&mut self, type_list: Vec<SubgraphType>) {
        self.compatible_type_name_stack.push(type_list);
    }

    /// pop the known type for the current node which will uses type=[known]
    fn pop_known(&mut self) -> SubgraphType {
        self.known_type_name_stack.pop().unwrap_or_else(|| {
            self.print_stack();
            panic!("No known type name found!")
        })
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
        match self.compatible_type_name_stack.pop() {
            Some(item) => item,
            None => {
                self.print_stack();
                panic!("No known type name found!")
            },
        }
    }
}

fn check_node_off(
        function_call_params: &CallParams,
        call_triggers: &HashMap<SmolStr, CallTrigger>,
        query_context_manager: &QueryContextManager,
        node_common: &NodeCommon
    ) -> bool {
    let mut off = false;
    if let Some(ref option_name) = node_common.type_name {
        off = match function_call_params.selected_types {
            CallTypes::None => true,
            CallTypes::Type(ref t_name) => if t_name != option_name { true } else { false },
            CallTypes::TypeList(ref t_name_list) => if !t_name_list.contains(&option_name) { true } else { false },
            _ => panic!("Expected None, TypeName or a TypeNameList for function selected types")
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
        off = off || match call_triggers.get(trigger_name) {
            Some(call_trigger) => !(call_trigger.0)(query_context_manager),
            None => panic!("Call trigger wasn't registered: {}", trigger_name),
        }
    }
    return off;
}

impl<StC: StateChooser> MarkovChainGenerator<StC> {
    pub fn next(&mut self, query_context_manager: &QueryContextManager, dynamic_model: &mut (impl DynamicModel + ?Sized)) -> Option<<Self as Iterator>::Item> {
        if let Some(call_params) = self.pending_call.take() {
            let inputs = match call_params.selected_types {
                CallTypes::Known => CallTypes::Type(self.pop_known()),
                CallTypes::KnownList => CallTypes::TypeList(self.pop_known_list()),
                CallTypes::Compatible => CallTypes::TypeList(self.pop_compatible_list()),
                CallTypes::PassThrough => self.call_stack.last().unwrap().function_params.selected_types.clone(),
                any => any,
            };
            self.call_stack.push(StackItem::from_call_params(CallParams {
                func_name: call_params.func_name,
                selected_types: inputs,
                modifiers: call_params.modifiers,
            }));
        }

        dynamic_model.notify_call_stack_length(self.call_stack.len());

        let stack_item = match self.call_stack.last_mut() {
            Some(stack_item) => stack_item,
            None => {
                self.reset();
                return None;
            },
        };

        let current_node = stack_item.next_node.clone();

        dynamic_model.update_current_state(&current_node.node_common.name);

        let function = self.markov_chain.functions.get(&stack_item.function_params.func_name).unwrap();
        if current_node.node_common.name == function.exit_node_name {
            self.call_stack.pop();
            return Some(current_node.node_common.name)
        }
        // let mut hashmap: HashMap<String, fn() -> bool> = HashMap::new();
        // // Call functions based on the key
        // if let Some(func) = hashmap.get("one") {
        //     let a = func();
        // }

        let cur_node_outgoing = function.chain.get(&current_node.node_common.name).unwrap();

        let cur_node_outgoing = cur_node_outgoing.iter().map(|el| {
            (check_node_off(&stack_item.function_params, &self.call_triggers, query_context_manager, &el.1.node_common), el.0, el.1.clone())
        }).collect::<Vec<_>>();

        let cur_node_outgoing = dynamic_model.assign_probabilities(cur_node_outgoing);

        let destination = self.state_chooser.choose_destination(cur_node_outgoing);

        if let Some(destination) = destination {
            if check_node_off(&stack_item.function_params, &self.call_triggers, query_context_manager, &destination.node_common) {
                panic!("Chosen node is off: {} (after {})", destination.node_common.name, current_node.node_common.name);
            }
            stack_item.current_node_name = current_node.node_common.name.clone();
            stack_item.next_node = destination.clone();
        } else {
            self.print_stack();
            panic!("No destination found for {}.", current_node.node_common.name);
        }

        if let Some(call_params) = &current_node.call_params {
            let mut prepared_call_params = call_params.clone();
            prepared_call_params.modifiers = match prepared_call_params.modifiers {
                CallModifiers::PassThrough => stack_item.function_params.modifiers.clone(),
                any => any,
            };
            self.pending_call = Some(prepared_call_params);
        }

        Some(current_node.node_common.name)
    }
}

impl<StC: StateChooser> Iterator for MarkovChainGenerator<StC> {
    type Item = SmolStr;

    fn next(&mut self) -> Option<Self::Item> {
        self.next(&QueryContextManager::new(), &mut MarkovModel::new())
    }
}