mod dot_parser;
mod error;
use std::{collections::HashMap, fs::File, io::Read, path::Path};
use rand::{prelude::ThreadRng, thread_rng, Rng};

use regex::Regex;
use smol_str::SmolStr;

use self::{
    dot_parser::DotTokenizer, error::SyntaxError,
};

pub use dot_parser::FunctionInputsType;

#[derive(Clone, Debug)]
struct MarkovChain {
    functions: HashMap<SmolStr, Function>,
}

#[derive(Clone, Debug)]
struct NodeParams {
    name: SmolStr,
    call_params: Option<CallParams>,
    option_name: Option<SmolStr>,
}

#[derive(Clone, Debug)]
struct CallParams {
    func_name: SmolStr,
    inputs: FunctionInputsType,
    modifiers: Option<Vec<SmolStr>>,
}

#[derive(Clone, Debug)]
struct Function {
    source_node_name: SmolStr,
    exit_node_name: SmolStr,
    input_type: FunctionInputsType,
    modifiers: Option<Vec<SmolStr>>,
    chain: HashMap<SmolStr, Vec<(f64, NodeParams)>>,
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
    fn parse_dot<P: AsRef<Path>>(source_path: P) -> Result<Self, SyntaxError> {
        let mut file = File::open(source_path).unwrap();
        let mut source = String::new();
        file.read_to_string(&mut source).unwrap();
        let source = MarkovChain::remove_fake_edges(source);
        let mut functions = MarkovChain::parse_functions(&source)?;
        MarkovChain::fill_probs_equal(&mut functions);
        Ok(MarkovChain { functions })
    }

    fn remove_fake_edges(source: String) -> String {
        // see https://github.com/maciejhirsz/logos/issues/258
        let fake_edge_regex = Regex::new(r"[^\r\n]+\[[\s]*color[\s]*=[\s]*none[\s]*\]").unwrap();
        fake_edge_regex.replace_all(&source, "").into_owned()
    }

    fn parse_functions(source: &str) -> Result<HashMap<SmolStr, Function>, SyntaxError> {
        let mut functions = HashMap::<_, _>::new();
        let mut node_params = HashMap::<SmolStr, NodeParams>::new();
        let mut current_function = Option::<Function>::None;
        for token in DotTokenizer::from_str(&source) {
            match token? {
                dot_parser::CodeUnit::Function(definition) => {
                    current_function = Some(Function::new(definition.clone()));
                    define_node(&mut current_function, &mut node_params, definition.source_node_name, None, None)?;
                    define_node(&mut current_function, &mut node_params, definition.exit_node_name, None, None)?;
                }
                dot_parser::CodeUnit::CloseDeclaration => {
                    if let Some(function) = current_function {
                        functions.insert(function.source_node_name.clone(), function);
                        current_function = None;
                    } else {
                        return Err(SyntaxError::new(format!("Unexpected CloseDeclaration")));
                    }
                }
                dot_parser::CodeUnit::NodeDef { name, option_name } => {
                    define_node(&mut current_function, &mut node_params, name, None, option_name)?;
                }
                dot_parser::CodeUnit::Call {
                    node_name,
                    func_name,
                    inputs,
                    modifiers,
                    option_name,
                } => {
                    define_node(&mut current_function, &mut node_params, node_name, Some(CallParams {
                        func_name, inputs, modifiers
                    }), option_name)?;
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

        // type checks
        if functions.get("Query").is_none() {
            return Err(SyntaxError::new(format!(
                "Query is the entry point and must be defined: subgraph def_Query {{...}}"
            )));
        }
        for (node_name, params) in node_params {
            if let Some(call_params) = params.call_params {
                if let Some(function) = functions.get(&call_params.func_name) {
                    let gen_type_error = |error_msg: String| {
                        format!(
                            "Call node {node_name}: Invalid function arguments; {error_msg} (function source node {})",
                            function.source_node_name
                        )
                    };

                    match call_params.inputs.clone() {
                        FunctionInputsType::TypeName(name) => {
                            if let FunctionInputsType::TypeNameVariants(variants) = &function.input_type {
                                if !variants.contains(&name) {
                                    return Err(SyntaxError::new(gen_type_error(format!(
                                        "Expected one of {:?}, got {:?}", function.input_type, call_params.inputs
                                    ))));
                                }
                            } else {
                                return Err(SyntaxError::new(gen_type_error(format!(
                                    "Got {:?}, but the function type is {:?}", call_params.inputs, function.input_type,
                                ))));
                            }
                        },
                        FunctionInputsType::TypeNameList(types_list) => {
                            if let FunctionInputsType::TypeNameList(func_types_list) = &function.input_type {
                                for c_type in types_list {
                                    if !func_types_list.contains(&c_type) {
                                        return Err(SyntaxError::new(gen_type_error(format!(
                                            "type {} is not in {:?}", c_type, function.input_type
                                        ))));
                                    }
                                }
                            } else {
                                return Err(SyntaxError::new(gen_type_error(format!(
                                    "Got {:?}, but the function type is {:?}", call_params.inputs, function.input_type,
                                ))));
                            }
                        },
                        _ => {}
                    };

                    if let Some(modifiers) = call_params.modifiers {
                        if let Some(ref func_modifiers) = function.modifiers {
                            for c_mod in modifiers {
                                if !func_modifiers.contains(&c_mod) {
                                    return Err(SyntaxError::new(gen_type_error(format!(
                                        "modifier {} is not in {:?}", c_mod, function.input_type
                                    ))));
                                }
                            }
                        } else {
                            return Err(SyntaxError::new(gen_type_error(format!(
                                "Call has modifiers {:?} which are not present in the declaration", modifiers
                            ))));
                        }
                    }
                } else {
                    return Err(SyntaxError::new(format!(
                        "Function is not defined: {node_name} (function {})",
                        params.name
                    )));
                }
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

fn define_node(
        current_function: &mut Option<Function>, node_params: &mut HashMap<SmolStr, NodeParams>,
        node_name: SmolStr, call_params: Option<CallParams>, option_name: Option<SmolStr>
    ) -> Result<(), SyntaxError> {
    if let Some(ref mut function) = current_function {
        check_option(&function, &node_name, &option_name)?;
        function.chain.insert(node_name.clone(), Vec::<_>::new());
        node_params.insert(node_name.clone(), NodeParams { name: node_name, call_params, option_name });
    } else {
        return Err(SyntaxError::new(format!(
            "Unexpected node definition: {node_name} [...]"
        )));
    }
    Ok(())
}

fn check_option(current_function: &Function, node_name: &SmolStr, option_name: &Option<SmolStr>) -> Result<(), SyntaxError> {
    if let Some(option_name) = option_name {
        if !(match &current_function.input_type {
            FunctionInputsType::TypeNameList(list) => list,
            FunctionInputsType::TypeNameVariants(list) => list,
            _ => return Err(SyntaxError::new(format!(
                "Unexpected optional node: {node_name}. Function does not acccept arguments"
            )))
        }.contains(option_name)) {
            return Err(SyntaxError::new(format!(
                "Unexpected option: {option_name} Expected one of: {:?}",
                current_function.input_type
            )))
        }
    }
    Ok(())
}

#[derive(Clone, Debug)]
pub struct MarkovChainGenerator {
    pub known_type_name_stack: Vec<Vec<SmolStr>>,
    pub compatible_type_name_stack: Vec<Vec<SmolStr>>,
    markov_chain: MarkovChain,
    call_stack: Vec<StackItem>,
    rng: ThreadRng,
}

#[derive(Clone, Debug)]
struct StackItem {
    current_function: CallParams,
    next_node: SmolStr,
    return_handler: Option<SmolStr>,
}

impl MarkovChainGenerator {
    pub fn parse_graph_from_file<P: AsRef<Path>>(source_path: P) -> Result<Self, SyntaxError> {
        let chain = MarkovChain::parse_dot(source_path)?;
        let mut self_ = MarkovChainGenerator {
            markov_chain: chain,
            call_stack: vec![],
            known_type_name_stack: vec![],
            compatible_type_name_stack: vec![],
            rng: thread_rng(),
        };
        self_.reset();
        Ok(self_)
    }

    pub fn reset(&mut self) {
        self.call_stack = vec![StackItem {
            current_function: CallParams {
                func_name: SmolStr::new("Query"),
                inputs: FunctionInputsType::None,
                modifiers: None
            },
            next_node: SmolStr::new("Query"),
            return_handler: None
        }];
        self.known_type_name_stack = vec![vec![SmolStr::new("")]];
        self.compatible_type_name_stack = vec![vec![SmolStr::new("")]];
    }

    pub fn get_inputs(&self) -> FunctionInputsType {
        self.call_stack.last().unwrap().current_function.inputs.clone()
    }

    pub fn get_modifiers(&self) -> Option<Vec<SmolStr>> {
        self.call_stack.last().unwrap().current_function.modifiers.clone()
    }
}

fn check_node_off(function_inputs_conv: &FunctionInputsType, option_name: &Option<SmolStr>) -> bool {
    if let Some(option_name) = option_name {
        match function_inputs_conv {
            FunctionInputsType::None => return true,
            FunctionInputsType::TypeName(t_name) => if t_name != option_name { return true },
            FunctionInputsType::TypeNameList(t_name_list) => if !t_name_list.contains(&option_name) { return true },
            _ => panic!("Must be None, TypeName or a TypeNameList")
        }
    }
    return false;
}

impl Iterator for MarkovChainGenerator {
    type Item = SmolStr;

    fn next(&mut self) -> Option<Self::Item> {
        let stack_item = match self.call_stack.last_mut() {
            Some(stack_item) => stack_item,
            None => {
                self.reset();
                return None;
            },
        };

        // check if we have just returned from a function
        if let Some(ref handler) = stack_item.return_handler {
            return Some(handler.to_owned());
        }

        let cur_node_name = stack_item.next_node.clone();
        let function = self.markov_chain.functions.get(&stack_item.current_function.func_name).unwrap();
        if cur_node_name == function.exit_node_name {
            self.call_stack.pop();
            return Some(cur_node_name)
        }

        let cur_node_outgoing = function.chain.get(&cur_node_name).unwrap();

        let level: f64 = self.rng.gen::<f64>() * (
            cur_node_outgoing.iter().map(|el| {
                if check_node_off(&stack_item.current_function.inputs, &el.1.option_name) {
                    return 0f64
                }
                el.0
            }).sum::<f64>()
        );
        let mut cumulative_prob = 0f64;
        let mut destination = Option::<&NodeParams>::None;
        for (prob, dest) in cur_node_outgoing {
            if check_node_off(&stack_item.current_function.inputs, &dest.option_name) {
                continue;
            }
            cumulative_prob += prob;
            if level < cumulative_prob {
                destination = Some(dest);
                break;
            }
        }
        let destination = destination.unwrap();

        if let Some(call_params) = &destination.call_params {
            stack_item.return_handler = Some(SmolStr::new(format!("R{cur_node_name}")));
            self.call_stack.push(StackItem {
                current_function: CallParams {
                    func_name: call_params.func_name.clone(),
                    inputs: match &call_params.inputs {
                        FunctionInputsType::TypeName(t_name) => FunctionInputsType::TypeName(t_name.clone()),
                        FunctionInputsType::Known => FunctionInputsType::TypeNameList(self.known_type_name_stack.pop().unwrap()),
                        FunctionInputsType::Compatible => FunctionInputsType::TypeNameList(self.compatible_type_name_stack.pop().unwrap()),
                        FunctionInputsType::TypeNameList(t_name_list) => FunctionInputsType::TypeNameList(t_name_list.clone()),
                        FunctionInputsType::Any => {
                            let function = self.markov_chain.functions.get(&call_params.func_name).unwrap();
                            function.input_type.clone()
                        },
                        _ => FunctionInputsType::None
                    },
                    modifiers: call_params.modifiers.clone(),
                },
                next_node: call_params.func_name.clone(),
                return_handler: None
            });
        } else {
            stack_item.next_node = destination.name.clone();
        }

        Some(cur_node_name)
    }
}
