use std::{collections::{HashMap, BinaryHeap}, path::Path, cmp::Ordering};

use regex::Regex;
use serde::{Serialize, Deserialize};
use core::fmt::Debug;
use smol_str::SmolStr;

use super::{
    dot_parser, dot_parser::{DotTokenizer, FunctionInputsType, CodeUnit, NodeCommon, FunctionDeclaration, TypeWithFields}, error::SyntaxError, subgraph_type::SubgraphType,
};

/// this structure contains all the parsed graph functions
#[derive(Clone, Debug)]
pub struct MarkovChain {
    pub functions: HashMap<SmolStr, Function>,
}

/// this structure represents a single node with all of
/// its possible properties and modifiers
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct NodeParams {
    pub node_common: NodeCommon,
    pub call_params: Option<CallParams>,
    pub literal: bool,
    pub min_calls_until_function_exit: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord)]
pub enum ModifierWithFields {
    Modifier(SmolStr),
    PassThrough(SmolStr),
    CancelModifier(SmolStr),
}

impl ModifierWithFields {
    fn from_strings(modifiers: Vec<SmolStr>) -> Vec<Self> {
        modifiers.into_iter().map(|x|
            if x.starts_with('-') {
                ModifierWithFields::CancelModifier(SmolStr::new(&x[1..x.len()]))
            } else if x.starts_with('?') {
                ModifierWithFields::PassThrough(SmolStr::new(&x[1..x.len()]))
            } else {
                ModifierWithFields::Modifier(x)
            }
        ).collect()
    }
}

/// represents modifiers passed in call parameters.
/// Can be either None, list of static modifiers, or a
/// pass-through (pass current funciton's arguments)
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord)]
pub enum CallModifiers {
    /// none of the modifiers are activated
    None,
    StaticList(Vec<SmolStr>),
    /// Used to specify a static list of modifiers
    /// a list of modifiers with optional fields (?mod name)
    StaticListWithFields(Vec<ModifierWithFields>),
    /// Used when we want to pass the function's modifiers further
    /// Optionally, adds new modifiers. ([..., added modifier])
    PassThroughWithAddedMods(Vec<ModifierWithFields>),
}

impl CallModifiers {
    fn panic_if_not_static_list(&self) {
        match self {
            CallModifiers::None |
            CallModifiers::StaticList(..) => {},
            CallModifiers::PassThroughWithAddedMods(_) => panic!(
                "CallModifiers::StaticListWithParentMods was not substituted with CallModifiers::StaticList for parent function"
            ),
            CallModifiers::StaticListWithFields(_) => panic!(
                "CallModifiers::StaticListWithFields was not substituted with CallModifiers::StaticList for parent function"
            ),
        }
    }

    pub fn to_vec(self) -> Vec<SmolStr> {
        self.panic_if_not_static_list();
        match self {
            CallModifiers::None => vec![],
            CallModifiers::StaticList(list) => list,
            _ => panic!(),
        }
    }

    pub fn contains(&self, modifier: &SmolStr) -> bool {
        self.panic_if_not_static_list();
        match self {
            CallModifiers::None => false,
            CallModifiers::StaticList(list) => list.contains(modifier),
            _ => panic!(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord)]
pub enum CallTypes {
    /// Сhoose none of the types out of the allowed type list
    None,
    /// Used to be able to set the type later in the handler code..
    /// Is transformed into a TypeList(..).
    KnownList,
    /// Used to be able to set the type later. Similar to known, but indicates type compatibility.
    /// Will be extended in the future to link to the types call node which would supply the type.
    /// Is later transformed into a TypeList(..).
    Compatible,
    /// Used when we want to pass the function's type constraints further
    PassThrough,
    /// Query specific type selection. Allows to specify
    /// types for each column
    QueryTypes(QueryTypes),
    /// Select multiple types among possible type variants.
    TypeList(Vec<SubgraphType>),
    /// Select multiple types among possible type variants.
    TypeListWithFields(Vec<TypeWithFields>),
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord)]
pub enum QueryTypes {
    /// the number of columns is fixed, set of types for each one is fixed
    ColumnTypeLists {
        column_type_lists: Vec<Vec<SubgraphType>>,
    },
    /// applies to all columns and any number of acolumns is allowed
    TypeList {
        type_list: Vec<SubgraphType>,
    }
}

impl QueryTypes {
    /// splits itself into the first type and remaining types
    pub fn split_first(self) -> (Vec<SubgraphType>, Self) {
        match self {
            QueryTypes::ColumnTypeLists {
                mut column_type_lists
            } => {
                let first = column_type_lists.remove(0);
                (first, QueryTypes::ColumnTypeLists {
                    column_type_lists
                })
            },
            QueryTypes::TypeList { type_list } => {
                (type_list.clone(), QueryTypes::TypeList { type_list })
            },
        }
    }
}

impl CallTypes {
    fn from_function_inputs_type(node_name: &SmolStr, accepted_types: &FunctionTypes, input: FunctionInputsType) -> CallTypes {
        match input {
            FunctionInputsType::Any => {
                match accepted_types {
                    FunctionTypes::None => panic!("Incorrect input type for node {node_name}: Any. Function does not accept arguments."),
                    FunctionTypes::QueryTypeList(list) => CallTypes::QueryTypes(QueryTypes::TypeList { type_list: list.clone() }),
                    FunctionTypes::TypeList(list) => CallTypes::TypeList(list.clone()),
                }
            },
            FunctionInputsType::None => CallTypes::None,
            FunctionInputsType::KnownList => CallTypes::KnownList,
            FunctionInputsType::Compatible => CallTypes::Compatible,
            FunctionInputsType::PassThrough => CallTypes::PassThrough,
            FunctionInputsType::TypeListWithFields(tp_list) => CallTypes::TypeListWithFields(tp_list),
        }
    }
}

/// represents the call parameters passed to a function
/// called with a call node
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord)]
pub struct CallParams {
    /// which function this call node calls
    pub func_name: SmolStr,
    /// the output types the called function
    /// should be restricted to generate
    pub selected_types: CallTypes,
    /// whether types can have multiple columns
    /// Indicates that FunctionInputsType is either
    /// None or QueryTypes in its final form,
    /// instead of None and TypeList by default
    pub types_have_columns: bool,
    /// the special modifiers passed to the
    /// function called, which affect function
    /// behaviour
    pub modifiers: CallModifiers,
}

#[derive(Clone, Debug)]
pub enum FunctionTypes {
    /// Indicate that no types are accepted.
    None,
    /// Specify the allowed type list, of which multiple can be selected.
    TypeList(Vec<SubgraphType>),
    /// Specify the allowed type list, of which multiple can be selected.
    /// This one is for functions that generate multiple columns, such as queries
    QueryTypeList(Vec<SubgraphType>),
}

impl FunctionTypes {
    fn from_function_inputs_type(source_node_name: &SmolStr, input: FunctionInputsType, types_have_columns: bool) -> FunctionTypes {
        match input {
            FunctionInputsType::None => FunctionTypes::None,
            FunctionInputsType::TypeListWithFields(list) => {
                let tp_list = list.into_iter().map(|TypeWithFields::Type(x)| x).collect();
                if types_have_columns {
                    FunctionTypes::QueryTypeList(tp_list)
                } else {
                    FunctionTypes::TypeList(tp_list)
                }
            },
            any => panic!("Incorrect input type for function {}: {:?}", source_node_name, any),
        }
    }
}

/// represents a functional subgraph
#[derive(Clone, Debug)]
pub struct Function {
    /// this is the source node of a function
    /// the function execution starts here
    pub source_node_name: SmolStr,
    /// this is the function exit node. When this node
    /// is reached, the function exits and the parent
    /// function continues execution
    pub exit_node_name: SmolStr,
    /// the output types the function supports to
    /// be restricted to generate, affecting optional
    /// nodes
    pub accepted_types: FunctionTypes,
    /// a vector of special function modifiers. Affects
    /// the function behaviour, but not the nodes
    pub accepted_modifiers: Option<Vec<SmolStr>>,
    /// the chain of the function, contains all of the
    /// function nodes and connectiona between them.
    /// We suppose that it's cheaper by time to clone
    /// NodeParams than to use a separate name -> NodeParams
    /// hashmap, which is why this map stores the
    /// NodeParams directly.
    pub chain: HashMap<SmolStr, Vec<(f64, NodeParams)>>,
    /// function node params
    pub node_params: HashMap<SmolStr, NodeParams>,
}

impl Function {
    /// create Function struct from its parsed parameters
    fn new(declaration: FunctionDeclaration) -> Self {
        Function {
            accepted_types: FunctionTypes::from_function_inputs_type(&declaration.source_node_name, declaration.input_type, declaration.types_have_columns),
            accepted_modifiers: declaration.modifiers,
            source_node_name: declaration.source_node_name,
            exit_node_name: declaration.exit_node_name,
            chain: HashMap::<_, _>::new(),
            node_params: HashMap::new(),
        }
    }
}


#[derive(Clone, Eq, PartialEq)]
struct BinaryHeapNode {
    calls_from_start_node: i64,
    node_name: SmolStr
}

impl Ord for BinaryHeapNode {
    fn cmp(&self, other: &Self) -> Ordering {
        other.calls_from_start_node.cmp(&self.calls_from_start_node).then_with(
            || self.node_name.cmp(&other.node_name)
        )
    }
}

impl PartialOrd for BinaryHeapNode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl MarkovChain {
    /// parse the chain from a dot file; Initialise all the weights uniformly
    pub fn parse_dot<P: AsRef<Path>>(source_path: P) -> Result<Self, SyntaxError> {
        let source = std::fs::read_to_string(source_path).unwrap();
        let source = MarkovChain::remove_fake_edges(source);
        let (
            mut functions,
            node_params
        ) = MarkovChain::parse_functions_and_params(&source)?;
        MarkovChain::perform_type_checks(&functions, &node_params)?;
        MarkovChain::fill_paths_to_exit_with_call_nums(&mut functions);
        Ok(MarkovChain { functions })
    }

    /// manually removes all the [color=none] edges. Works breaking the lexer paradigm because of a bug.
    /// See https://github.com/maciejhirsz/logos/issues/258 for details.
    fn remove_fake_edges(source: String) -> String {
        let fake_edge_regex = Regex::new(r"[^\r\n]+\[[\s]*color[\s]*=[\s]*none[\s]*\]").unwrap();
        fake_edge_regex.replace_all(&source, "").into_owned()
    }

    /// parse all the functions from the code using the lexer; perform all the nessesary type checks
    fn parse_functions_and_params(source: &str) -> Result<(HashMap<SmolStr, Function>, HashMap<SmolStr, NodeParams>), SyntaxError> {
        let mut functions = HashMap::<_, _>::new();
        // stores node parameters map, needed for faster graph creation & type checks
        let mut node_params = HashMap::<SmolStr, NodeParams>::new();
        let mut current_function = Option::<Function>::None;
        let tokens = DotTokenizer::from_str(&source).collect::<Result<Vec<CodeUnit>, SyntaxError>>()?;
        // Run through all function definitions
        for token in &tokens {
            match token {
                dot_parser::CodeUnit::FunctionDeclaration(declaration) => {
                    let mut function = Some(Function::new(declaration.clone()));
                    define_node(&mut function, &mut node_params, NodeCommon::with_name(declaration.source_node_name.clone()), None, false)?;
                    define_node(&mut function, &mut node_params, NodeCommon::with_name(declaration.exit_node_name.clone()), None, false)?;
                    let function = function.unwrap();
                    functions.insert(function.source_node_name.clone(), function);
                },
                _ => {}
            }
        }
        // Fill in function bodies
        for token in tokens {
            match token {
                dot_parser::CodeUnit::FunctionDeclaration(declaration) => {
                    current_function = Some(functions.remove(&declaration.source_node_name).unwrap());
                }
                dot_parser::CodeUnit::CloseDeclaration => {
                    if let Some(mut function) = current_function.take() {
                        function.node_params = function.chain.keys().map(|node_name| (
                            node_name.clone(), node_params.get(node_name).unwrap().clone()
                        )).collect();

                        functions.insert(function.source_node_name.clone(), function);
                    } else {
                        return Err(SyntaxError::new(format!("Unexpected CloseDeclaration")));
                    }
                }
                dot_parser::CodeUnit::RegularNode { node_common, literal} => {
                    define_node(&mut current_function, &mut node_params, node_common, None, literal)?;
                }
                dot_parser::CodeUnit::CallNode {
                    node_common,
                    func_name,
                    inputs,
                    types_have_columns,
                    modifiers,
                } => {
                    let modifiers = match modifiers {
                        Some(mut modifiers) => {
                            if modifiers.contains(&SmolStr::new("...")) {
                                modifiers.retain(|x| x != "...");
                                CallModifiers::PassThroughWithAddedMods(ModifierWithFields::from_strings(modifiers))
                            } else if modifiers.iter().any(|x| x.starts_with('?')) {
                                CallModifiers::StaticListWithFields(ModifierWithFields::from_strings(modifiers))
                            } else  {
                                CallModifiers::StaticList(modifiers)
                            }
                        },
                        None => CallModifiers::None,
                    };
                    let selected_types = {
                        let accepted_types = if let Some(ref function) = current_function {
                            if function.source_node_name == func_name {
                                &function.accepted_types
                            } else {
                                &functions.get(&func_name).ok_or_else(|| SyntaxError::new(format!(
                                    "Call node {} calls non-existing function: {func_name}", node_common.name
                                )))?.accepted_types
                            }
                        } else {
                            return Err(SyntaxError::new(format!(
                                "Unexpected node definition: {} [...]", node_common.name
                            )));
                        };
                        CallTypes::from_function_inputs_type(
                            &node_common.name, accepted_types, inputs
                        )
                    };
                    define_node(&mut current_function, &mut node_params, node_common, Some(
                        CallParams { func_name, selected_types, types_have_columns, modifiers }
                    ), false)?;
                }
                dot_parser::CodeUnit::Edge {
                    node_name_from,
                    node_name_to,
                } => {
                    if let Some(ref mut function) = current_function {
                        if !function.chain.contains_key(&node_name_to) {
                            return Err(SyntaxError::new(format!(
                                "Cannot build edge: destination node does not exist: {}",
                                node_name_to
                            )));
                        }
                        if let Some(dest_list) = function.chain.get_mut(&node_name_from) {
                            dest_list.push((0f64, node_params.get(&node_name_to).unwrap().clone()));
                        } else {
                            return Err(SyntaxError::new(format!(
                                "Cannot build edge: source node does not exist: {}",
                                node_name_from
                            )));
                        }
                    } else {
                        return Err(SyntaxError::new(format!(
                            "Unexpected Edge: {} -> {}",
                            node_name_from, node_name_to
                        )));
                    }
                }
            };
        }
        Ok((functions, node_params))
    }

    /// performs all the type checks to ensure that the .dot code is correct
    fn perform_type_checks(functions: &HashMap<SmolStr, Function>, node_params: &HashMap<SmolStr, NodeParams>) -> Result<(), SyntaxError> {
        if functions.get("Query").is_none() {
            return Err(SyntaxError::new(format!(
                "Query is the entry point and must be defined: subgraph def_Query {{...}}"
            )));
        }
        for (node_name, params) in node_params {
            if let Some(ref call_params) = params.call_params {
                if let Some(function) = functions.get(&call_params.func_name) {
                    let gen_type_error = |error_msg: String| {
                        format!(
                            "Call node {node_name}: Invalid function arguments; {error_msg} (function source node {})",
                            function.source_node_name
                        )
                    };

                    match (&call_params.selected_types, &function.accepted_types) {
                        (
                            CallTypes::QueryTypes(QueryTypes::TypeList { type_list }),
                            FunctionTypes::QueryTypeList(func_type_list)
                        ) if type_list.iter().all(|t| func_type_list.contains(t)) => {},
                        (
                            CallTypes::TypeList(types_list),
                            FunctionTypes::TypeList(func_types_list)
                        ) if types_list.iter().all(|t| func_types_list.contains(t)) => {},
                        (
                            CallTypes::TypeListWithFields(types_list),
                            FunctionTypes::TypeList(func_types_list)
                        ) if types_list.iter().all(|t| func_types_list.contains(t.inner_ref())) => {},
                        (
                            CallTypes::None,
                            FunctionTypes::None | FunctionTypes::TypeList(..) | FunctionTypes::QueryTypeList(..)
                        ) => {},
                        (
                            CallTypes::PassThrough | CallTypes::Compatible | CallTypes::KnownList,
                            FunctionTypes::TypeList(..) | FunctionTypes::QueryTypeList(..)
                        ) => {},
                        _ => return Err(SyntaxError::new(gen_type_error(format!(
                            "Got {:?}, but the function type is {:?}", call_params.selected_types, function.accepted_types,
                        ))))
                    }

                    if call_params.modifiers != CallModifiers::None {
                        if let Some(ref func_modifiers) = function.accepted_modifiers {
                            let checked_modifiers = match &call_params.modifiers {
                                CallModifiers::None => vec![],
                                CallModifiers::StaticList(modifiers) => modifiers.iter().collect(),
                                CallModifiers::PassThroughWithAddedMods(ref modifiers) |
                                CallModifiers::StaticListWithFields(ref modifiers) => {
                                    let function = functions.iter().find(
                                        |(_, function)| function.chain.contains_key(node_name)
                                    ).unwrap().1;
                                    for pass_through_mod in modifiers.iter().filter_map(|x| match x {
                                        ModifierWithFields::Modifier(..) => None,
                                        ModifierWithFields::PassThrough(mod_name) => Some(mod_name),
                                        ModifierWithFields::CancelModifier(mod_name) => Some(mod_name),
                                    }) {
                                        if !function.accepted_modifiers.as_ref().unwrap().contains(pass_through_mod) {
                                            return Err(SyntaxError::new(gen_type_error(format!(
                                                "the pass-through modifier '?{pass_through_mod}' is not present in the parent\
                                                function's ({}) accepted modifiers list: {:?}", function.source_node_name, function.accepted_modifiers
                                            ))))
                                        }
                                    }
                                    modifiers.iter()
                                        .filter_map(|x| match x {
                                            ModifierWithFields::Modifier(mod_name) |
                                            ModifierWithFields::PassThrough(mod_name) => Some(mod_name),
                                            ModifierWithFields::CancelModifier(..) => None,
                                        }).collect()
                                },
                            };
                            for c_mod in checked_modifiers {
                                if !func_modifiers.contains(&c_mod) {
                                    return Err(SyntaxError::new(gen_type_error(format!(
                                        "Modifier {} is not in {:?}", c_mod, func_modifiers
                                    ))));
                                }
                            }
                        } else {
                            return Err(SyntaxError::new(gen_type_error(format!(
                                "Call has modifiers {:?} but no modifiers are present in the function declaration", call_params.modifiers
                            ))));
                        }
                    }
                } else {
                    return Err(SyntaxError::new(format!(
                        "Function is not defined: {node_name} (function {})",
                        params.node_common.name
                    )));
                }
            }
        }
        Ok(())
    }

    fn get_node_min_calls_till_exit(node_name: &SmolStr, node_params: &NodeParams, function: &Function, func_min_calls: &HashMap<SmolStr, i64>) -> i64 {
        let mut answer = i64::MAX;

        let mut least_calls_first_pq = BinaryHeap::<BinaryHeapNode>::new();
        least_calls_first_pq.push(BinaryHeapNode {
            calls_from_start_node: 0, node_name: node_name.clone(),
        });

        let mut min_calls_from_start_map = HashMap::<SmolStr, i64>::new();
        min_calls_from_start_map.insert(node_name.clone(), 0);

        while let Some(BinaryHeapNode { calls_from_start_node, node_name }) = least_calls_first_pq.pop() {
            if node_name == function.exit_node_name {
                answer = calls_from_start_node + node_params.call_params.as_ref().map_or(
                    0i64, |c| 1 + *func_min_calls.get(&c.func_name).unwrap()
                );
                break;
            }

            if let Some(min_calls_from_start) = min_calls_from_start_map.get(&node_name) {
                if *min_calls_from_start < calls_from_start_node {
                    continue;
                }
            }

            for (_, out_node_params) in function.chain.get(&node_name).unwrap() {
                let out_node_calls_from_src = calls_from_start_node + out_node_params.call_params.as_ref().map_or(
                    0i64, |c| 1 + *func_min_calls.get(&c.func_name).unwrap()
                );

                let mut min_updated = false;

                if let Some(current_min_calls_from_start) = min_calls_from_start_map.get_mut(&out_node_params.node_common.name) {
                    if out_node_calls_from_src < *current_min_calls_from_start {
                        *current_min_calls_from_start = out_node_calls_from_src;
                        min_updated = true;
                    }
                } else {
                    min_calls_from_start_map.insert(out_node_params.node_common.name.clone(), out_node_calls_from_src);
                    min_updated = true;
                }

                if min_updated {
                    least_calls_first_pq.push(BinaryHeapNode {
                        calls_from_start_node: out_node_calls_from_src,
                        node_name: out_node_params.node_common.name.clone(),
                    });
                }
            }
        }

        answer
    }

    fn fill_paths_to_exit_with_call_nums(functions: &mut HashMap<SmolStr, Function>) {
        // find min calls till exit of all function source nodes
        // in this code, we disregard modifiers and assume every node is on
        // as this approach doesn't work, we actually should think about usual modifiers and
        // call modifiers. When FromContents has been filled out, that changes the 
        // behaviour of all TYPES subgraphs, SELECT subgraph, etc.
        // at that point, all "calls will exit" should be recalculated...
        let mut func_min_calls: HashMap<SmolStr, i64> = functions.keys().map(|func_name| (func_name.clone(), 0i64)).collect();
        // hack to encourage columns (these subgraphs have no inner calls)
        func_min_calls.insert(SmolStr::new("literals"), -1);
        func_min_calls.insert(SmolStr::new("column_spec"), -1);
        func_min_calls.insert(SmolStr::new("select_datetime_field"), -1); 
        // for i in 0..1000 {
        //     let mut did_update = false;
        //     for function in functions.values() {
        //         let node_name = &function.source_node_name;
        //         let new_val = Self::get_node_min_calls_till_exit(
        //             node_name, function.node_params.get(node_name).unwrap(),
        //             function, &func_min_calls
        //         );
        //         let old_val = func_min_calls.insert(node_name.clone(), new_val);
        //         did_update = did_update || if let Some(old_val) = old_val {
        //             old_val != new_val
        //         } else { true };
        //     }
        //     if did_update {
        //         break
        //     }
        //     if i == 999 {
        //         eprintln!("min calls till exit didn't converge after 1000 iterations");
        //     }
        // }
        
        // fill all the nodes in with min calls
        let mut min_calls_till_exit = HashMap::<SmolStr, i64>::new();
        for function in functions.values() {
            min_calls_till_exit.insert(function.exit_node_name.clone(), 0);
            for (node_name, node_params) in &function.node_params {
                if min_calls_till_exit.contains_key(node_name) {
                    continue;
                }
                min_calls_till_exit.insert(
                    node_name.clone(),
                    Self::get_node_min_calls_till_exit(node_name, node_params, function, &func_min_calls)
                );
            }
        }

        // validate & put all the results to the node params
        for function in functions.values_mut() {
            for out_nodes in function.chain.values_mut() {
                for (_, out_node) in out_nodes {
                    if let Some(min_calls) = min_calls_till_exit.get(&out_node.node_common.name) {
                        if *min_calls == i64::MAX {
                            panic!("Node {} does not reach function exit", out_node.node_common.name);
                        }
                        out_node.min_calls_until_function_exit = *min_calls;
                    } else {
                        panic!("Node {} was not marked with minimum distance.", out_node.node_common.name);
                    }
                }
            }
            for node in function.node_params.values_mut() {
                if let Some(min_calls) = min_calls_till_exit.get(&node.node_common.name) {
                    node.min_calls_until_function_exit = *min_calls;
                }
            }
        }
    }
}

/// add a node to the current function graph & node_params map; Perform syntax checks for optional nodes
fn define_node(
        current_function: &mut Option<Function>, node_params: &mut HashMap<SmolStr, NodeParams>,
        node_common: NodeCommon, call_params: Option<CallParams>, literal: bool
    ) -> Result<(), SyntaxError> {
    if let Some(function) = current_function {
        check_type(&function, &node_common.name, &node_common.type_names)?;
        check_modifier(&function, &node_common.name, &node_common.modifier)?;
        function.chain.insert(node_common.name.clone(), Vec::<_>::new());
        node_params.insert(node_common.name.clone(), NodeParams {
            node_common: node_common, call_params,
            literal, min_calls_until_function_exit: 0,
        });
    } else {
        return Err(SyntaxError::new(format!(
            "Unexpected node definition: {} [...]", node_common.name
        )));
    }
    Ok(())
}

/// Perform checks for typed nodes
fn check_type(current_function: &Function, node_name: &SmolStr, type_names_opt: &Option<Vec<SubgraphType>>) -> Result<(), SyntaxError> {
    if let Some(type_names) = type_names_opt {
        let acc_types_list = match &current_function.accepted_types {
            FunctionTypes::TypeList(list) => list,
            _ => return Err(SyntaxError::new(format!(
                "Unexpected typed node: {node_name}. Function does not acccept arguments"
            )))
        };
        if !(type_names.iter().any(|type_name| acc_types_list.contains(type_name))) {
            return Err(SyntaxError::new(format!(
                "Unexpected type names: {:?}. Expected one of: {:?}, which were declared in function definition.",
                type_names, current_function.accepted_types
            )))
        }
    }
    Ok(())
}

/// Perform syntax checks for modifier nodes
fn check_modifier(current_function: &Function, node_name: &SmolStr, modifier: &Option<(SmolStr, bool)>) -> Result<(), SyntaxError> {
    if let Some((modifier_name, _)) = modifier {
        if !(match &current_function.accepted_modifiers {
            Some(list) => list,
            None => return Err(SyntaxError::new(format!(
                "Unexpected modifier node: {node_name}. Function does not acccept modifiers"
            )))
        }.contains(modifier_name)) {
            return Err(SyntaxError::new(format!(
                "Unexpected modifier: {modifier_name} Expected one of: {:?}",
                current_function.accepted_modifiers
            )))
        }
    }
    Ok(())
}