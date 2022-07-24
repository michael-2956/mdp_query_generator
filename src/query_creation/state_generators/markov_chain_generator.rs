mod dot_parser;
mod error;
use std::{collections::HashMap, fs::File, io::Read, path::PathBuf};

use smol_str::SmolStr;

use self::{
    dot_parser::{DotTokenizer, FunctionInputsType},
    error::SyntaxError,
};

struct MarkovChain {
    functions: HashMap<SmolStr, Function>,
}

enum Destination {
    Node(NodeParams),
    Call(CallParams),
}

#[derive(Clone)]
struct NodeParams {
    name: SmolStr,
    optional: bool,
}

#[derive(Clone)]
struct CallParams {
    name: SmolStr,
    inputs: FunctionInputsType,
    modifiers: Option<Vec<SmolStr>>,
    optional: bool,
}

struct Function {
    source_node_name: SmolStr,
    exit_node_name: SmolStr,
    input_type: FunctionInputsType,
    modifiers: Option<Vec<SmolStr>>,
    chain: HashMap<SmolStr, Vec<(f64, Destination)>>,
}

impl Function {
    fn new(definition: dot_parser::Function) -> Self {
        Function {
            source_node_name: definition.source_node_name,
            exit_node_name: definition.exit_node_name,
            input_type: definition.input_type,
            modifiers: definition.modifiers,
            chain: HashMap::<_, _>::new(),
        }
    }
}

impl MarkovChain {
    fn parse_dot(source_path: PathBuf) -> Result<Self, SyntaxError> {
        let mut file = File::open(source_path).unwrap();
        let mut source = String::new();
        file.read_to_string(&mut source).unwrap();
        let mut functions = MarkovChain::parse_functions(&source)?;
        MarkovChain::fill_probs_equal(&mut functions);
        Ok(MarkovChain { functions })
    }

    fn parse_functions(source: &str) -> Result<HashMap<SmolStr, Function>, SyntaxError> {
        let mut functions = HashMap::<_, _>::new();
        let mut node_params = HashMap::<SmolStr, NodeParams>::new();
        let mut call_params = HashMap::<SmolStr, CallParams>::new();
        let mut current_function = Option::<Function>::None;
        for token in DotTokenizer::from_str(&source) {
            match token? {
                dot_parser::CodeUnit::Function(definition) => {
                    current_function = Some(Function::new(definition));
                }
                dot_parser::CodeUnit::CloseDeclaration => {
                    if let Some(function) = current_function {
                        functions.insert(function.source_node_name.clone(), function);
                        current_function = None;
                    } else {
                        return Err(SyntaxError::new(format!("Unexpected CloseDeclaration")));
                    }
                }
                dot_parser::CodeUnit::NodeDef { name, optional } => {
                    if let Some(ref mut function) = current_function {
                        function.chain.insert(name.clone(), Vec::<_>::new());
                        node_params.insert(name.clone(), NodeParams { name, optional });
                    } else {
                        return Err(SyntaxError::new(format!(
                            "Unexpected NodeDef: {name} [...]"
                        )));
                    }
                }
                dot_parser::CodeUnit::Call {
                    node_name,
                    name,
                    inputs,
                    modifiers,
                    optional,
                } => {
                    call_params.insert(
                        node_name,
                        CallParams {
                            name,
                            inputs,
                            modifiers,
                            optional,
                        },
                    );
                }
                dot_parser::CodeUnit::Edge {
                    node_name_from,
                    node_to,
                } => {
                    let (node_name_to, destination) = match node_to {
                        dot_parser::EdgeVertex::Node(node_name) => {
                            let dest_params = node_params.get(&node_name).unwrap().clone();
                            (node_name, Destination::Node(dest_params))
                        }
                        dot_parser::EdgeVertex::Call {
                            node_name,
                            call_name,
                        } => {
                            let dest_params = call_params.get(&call_name).unwrap().clone();
                            (node_name, Destination::Call(dest_params))
                        }
                    };
                    if let Some(ref mut function) = current_function {
                        if !function.chain.contains_key(&node_name_to) {
                            return Err(SyntaxError::new(format!(
                                "Cannot build edge: destination node does not exist: {}",
                                node_name_to
                            )));
                        }
                        if let Some(dest_list) = function.chain.get_mut(&node_name_from) {
                            dest_list.push((0f64, destination));
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

        // type checks
        for (node_name, params) in call_params {
            if let Some(function) = functions.get(&params.name) {
                if params.inputs != function.input_type {
                    return Err(SyntaxError::new(format!(
                        "Invalid function arguments; Expected as {:?}, got {:?}",
                        function.input_type, params.inputs
                    )));
                }
                if params.modifiers != function.modifiers {
                    return Err(SyntaxError::new(format!(
                        "Invalid function modifiers; Expected as {:?}, got {:?}",
                        function.modifiers, params.modifiers
                    )));
                }
            } else {
                return Err(SyntaxError::new(format!(
                    "Function is not defined: {node_name} (function {})",
                    params.name
                )));
            }
        }

        Ok(functions)
    }

    fn fill_probs_equal(functions: &mut HashMap<SmolStr, Function>) {
        for (_, function) in functions {
            for (_, out) in function.chain.iter_mut() {
                let fill_with = 1f64 / (out.len() as f64);
                for (weight, _) in out {
                    *weight = fill_with;
                }
            }
        }
    }
}

pub struct MarkovChainGenerator {
    #[allow(dead_code)]
    markov_chain: MarkovChain,
}

impl MarkovChainGenerator {
    pub fn parse_graph_from_file(source_path: PathBuf) -> Result<Self, SyntaxError> {
        Ok(MarkovChainGenerator {
            markov_chain: MarkovChain::parse_dot(source_path)?,
        })
    }
}
