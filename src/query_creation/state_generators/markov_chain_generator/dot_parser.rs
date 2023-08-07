use std::{collections::HashMap, fmt::Display};

use regex::Regex;
use smol_str::SmolStr;
use logos::{Filter, Lexer, Logos};
use sqlparser::ast::{DataType, ExactNumberInfo, ObjectName, Ident};

use super::error::SyntaxError;

#[derive(Logos, Debug, PartialEq)]
enum DotToken {
    #[regex(r"#[^\n]*", logos::skip)] // Skip # line comments
    #[regex(r"//[^\n]*", logos::skip)] // Skip // line comments
    #[regex(r"/\*", block_comment)] // Skip block comments
    #[regex(r"[^\S\n\r]+", logos::skip)] // Skip whitespace
    #[regex(r"[\n\r]+", logos::skip)] // Skip newline
    #[error]
    Error,

    #[regex(r"digraph")]
    Digraph,

    #[regex(r"subgraph")]
    Subgraph,

    #[regex(r"[A-Za-z_][A-Za-z0-9_]*", identifier)]
    Identifier(SmolStr),

    #[regex(r#""[^"]*""#, quoted_identifier)]
    QuotedIdentifiers(SmolStr),

    #[regex(r#""\[[^"]*\]""#, quoted_identifier_with_brackets)]
    QuotedIdentifiersWithBrackets(SmolStr),

    #[regex(r"\{")]
    OpenDeclaration,

    #[regex(r"}")]
    CloseDeclaration,

    #[regex(r"\[")]
    OpenSpecification,

    #[regex(r"\]")]
    CloseSpecification,

    #[regex(r",")]
    Comma,

    #[regex(r"=")]
    Equals,

    #[regex(r"->")]
    Arrow,
}

/// extracts value from an identifier
fn identifier(lex: &mut Lexer<'_, DotToken>) -> SmolStr {
    SmolStr::new(lex.slice())
}

/// extracts value from an identifier with quotes
fn quoted_identifier(lex: &mut Lexer<'_, DotToken>) -> SmolStr {
    let quoted_ident = lex.slice();
    SmolStr::new(quoted_ident.get(1..quoted_ident.len() - 1).unwrap())
}

/// extracts value from an identifier with quotes and square brackets
fn quoted_identifier_with_brackets(lex: &mut Lexer<'_, DotToken>) -> SmolStr {
    let quoted_ident = lex.slice();
    let opening_bracket_pos = quoted_ident.find('[').unwrap();
    let closing_bracket_pos = quoted_ident.find(']').unwrap();
    SmolStr::new(
        quoted_ident
            .get(opening_bracket_pos+1..closing_bracket_pos)
            .unwrap(),
    )
}

/// callback that skips block comments
fn block_comment(lex: &mut Lexer<'_, DotToken>) -> Filter<()> {
    if let Some(end) = lex.remainder().find("*/") {
        lex.bump(end + 2);
        Filter::Skip
    } else {
        Filter::Emit(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SubgraphType {
    /// This type is used as an array inner type
    /// if the inner type is yet to be determined
    Undetermined,
    Numeric,
    Val3,
    Array((Box<SubgraphType>, Option<usize>)),
    ListExpr(Box<SubgraphType>),
    String,
}

impl SubgraphType {
    pub fn wrap_in_func(&self, func_name: &str) -> SubgraphType {
        let outer = SubgraphType::from_func_name(func_name);
        match outer {
            SubgraphType::Array(..) => SubgraphType::Array((Box::new(self.clone()), None)),
            SubgraphType::ListExpr(..) => SubgraphType::ListExpr(Box::new(self.clone())),
            any => panic!("Cannot wrap into {any}")
        }
    }

    pub fn from_func_name(func_name: &str) -> Self {
        match func_name {
            "numeric" => SubgraphType::Numeric,
            "VAL_3" => SubgraphType::Val3,
            "array" => SubgraphType::Array((Box::new(SubgraphType::Undetermined), None)),
            "list_expr" => SubgraphType::ListExpr(Box::new(SubgraphType::Undetermined)),
            "string" => SubgraphType::String,
            any => panic!("Unexpected function name, can't convert to a SubgraphType: {any}")
        }
    }

    fn from_type_name(s: &str) -> Result<Self, SyntaxError> {
        match s {
            "numeric" => Ok(SubgraphType::Numeric),
            "3VL Value" => Ok(SubgraphType::Val3),
            "array" => Ok(SubgraphType::Array((Box::new(SubgraphType::Undetermined), None))),
            "list expr" => Ok(SubgraphType::ListExpr(Box::new(SubgraphType::Undetermined))),
            "string" => Ok(SubgraphType::String),
            any => Err(SyntaxError::new(format!("Type {any} does not exist!")))
        }
    }

    pub fn get_subgraph_func_name(&self) -> &str {
        match *self {
            SubgraphType::Numeric => "numeric",
            SubgraphType::Val3 => "VAL_3",
            SubgraphType::Array(..) => "array",
            SubgraphType::ListExpr(..) => "list_expr",
            SubgraphType::String => "string",
            SubgraphType::Undetermined => panic!("SubgraphType::Undetermined does not have an associated subgraph"),
        }
    }

    pub fn inner(&self) -> SubgraphType {
        match self {
            SubgraphType::Array((inner, _)) => *inner.clone(),
            SubgraphType::ListExpr(inner) => *inner.clone(),
            any => panic!("{any} has no inner type"),
        }
    }

    pub fn from_data_type(data_type: &DataType) -> Self {
        match data_type {
            DataType::Integer(_) => Self::Numeric,  /// TODO
            DataType::Varchar(_) => Self::String,
            DataType::CharVarying(_) => Self::String,
            DataType::Char(_) => Self::String,
            DataType::Numeric(_) => Self::Numeric,
            DataType::Date => Self::Numeric,  /// TODO
            DataType::Boolean => Self::Val3,
            DataType::Array(inner) => Self::Array((Box::new((*inner.to_owned().unwrap()).into()), None)),
            any => panic!("DataType not implemented: {any}"),
        }
    }

    /// CURRENTLY NOT USED
    pub fn to_data_type(&self) -> DataType {
        // TODO: this is a temporary solution
        // Should randomly select among type variants.
        match self {
            SubgraphType::Numeric => DataType::Numeric(ExactNumberInfo::None),
            SubgraphType::Val3 => DataType::Boolean,
            SubgraphType::Array((inner, _)) => DataType::Array(Some(Box::new(inner.to_data_type()))),
            SubgraphType::ListExpr(_) => DataType::Custom(ObjectName(vec![Ident::new("row_expression")]), vec![]),
            SubgraphType::String => DataType::String,
            SubgraphType::Undetermined => panic!("Can't convert SubgraphType::Undetermined to DataType"),
        }
    }
}

impl From<DataType> for SubgraphType {
    fn from(value: DataType) -> Self {
        SubgraphType::from_data_type(&value)
    }
}

impl Display for SubgraphType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let str = match self {
            SubgraphType::Numeric => "numeric".to_string(),
            SubgraphType::Val3 => "3VL Value".to_string(),
            SubgraphType::Array((inner, length)) => format!("array[{}]({:?})", inner, length),
            SubgraphType::ListExpr(inner) => format!("list expr[{}]", inner),
            SubgraphType::String => "string".to_string(),
            SubgraphType::Undetermined => "undetermined".to_string(),
        };
        write!(f, "{}", str)
    }
}

/// this structure can be treated in two ways: either
/// as the output types the function should be restricted
/// to generate, when in the context of a call node, or
/// what input types the function supports, when mentioned
/// in function definition
#[derive(Debug, Clone, PartialEq)]
pub enum FunctionInputsType {
    /// Used in function calls to choose all types out of the allowed type list
    Any,
    /// Used in function calls to choose none of the types out of the allowed type list
    /// Used in function declarations to indicate that no types are accepted.
    None,
    /// Used in function calls to be able to set the type list in handlers
    KnownList,
    /// Used in function calls to be able to set the type later. Similar to known, but indicates type compatibility.
    /// Will be extended in the future to link to the types call node which would supply the type
    Compatible,
    /// Used in function calls to pass the function's type constraints further
    PassThrough,
    /// Used in function calls to pass the function's type constraints further, but only those
    /// that are related to the called function's name.
    /// Example:
    /// - Args: [Numeric, Array[Val3]]
    /// - Func.: Array.
    /// => Passed arguments: Array[Val3]
    PassThroughTypeNameRelated,
    /// Used in function calls to pass the function's type constraints further, but only those
    /// that are related to the called function's name
    /// Example:
    /// - Args: [Numeric, Array[Val3]]
    /// - Func.: Array.
    /// => Passed arguments: Array[Val3]
    /// NOTE: if uses_wrapped_types is ON, the arguments are not wrapped again.
    PassThroughRelated,
    /// Used in function calls to pass the function's type constraints further, but only those
    /// that are related to the called function's name, and also taking the inner type
    /// Example:
    /// - Args: [Numeric, Array[Val3]]
    /// - Func.: Array.
    /// => Passed arguments: Val3
    PassThroughRelatedInner,
    /// Used in function calls to select multiple types among the allowed type list.
    /// Used in function declarations to specify the allowed type list, of which multiple can be selected.
    TypeNameList(Vec<SubgraphType>),
}

/// splits a string with commas into a list of trimmed identifiers
fn get_identifier_names(name_list_str: SmolStr) -> Vec<SmolStr> {
    name_list_str
        .split(',')
        .map(|string| SmolStr::new(string.trim()))
        .collect::<Vec<_>>()
}

impl FunctionInputsType {
    /// tries to match a special type (any, known, compatible)
    fn try_extract_special_type(idents: &SmolStr) -> Option<FunctionInputsType> {
        match idents.as_str() {
            "any" => Some(FunctionInputsType::Any),
            "TR..." => Some(FunctionInputsType::PassThroughTypeNameRelated),
            "R..." => Some(FunctionInputsType::PassThroughRelated),
            "RI..." => Some(FunctionInputsType::PassThroughRelatedInner),
            "..." => Some(FunctionInputsType::PassThrough),
            "compatible" => Some(FunctionInputsType::Compatible),
            "known" => Some(FunctionInputsType::KnownList),
            _ => None,
        }
    }

    /// splits a string with commas into a list of trimmed identifiers
    fn parse_types(name_list_str: SmolStr) -> Result<Vec<SubgraphType>, SyntaxError> {
        name_list_str
            .split(',')
            .map(|x| x.trim())
            .map(SubgraphType::from_type_name)
            .collect::<Result<Vec<_>, _>>()
    }
}

/// This structure contains common fields for a node definition.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct NodeCommon {
    /// Node identifier
    pub name: SmolStr,
    /// Type name if specified (basically an "on" trigger)
    pub type_name: Option<SubgraphType>,
    /// Graph trigger with mode if specified
    pub trigger: Option<(SmolStr, bool)>,
    /// Name of the call trigger if specified
    pub call_trigger_name: Option<SmolStr>,
    /// Name of the call trigger that this node affects, if specified
    pub affects_call_trigger_name: Option<SmolStr>,
}

impl NodeCommon {
    pub fn with_name(name: SmolStr) -> Self {
        Self { name: name, type_name: None, trigger: None, call_trigger_name: None, affects_call_trigger_name: None }
    }
}

/// a code unit represents a distinct command in code.
#[derive(Debug, Clone)]
pub enum CodeUnit {
    FunctionDeclaration(FunctionDeclaration),
    RegularNode {
        node_common: NodeCommon,
        literal: bool,
    },
    CallNode {
        node_common: NodeCommon,
        func_name: SmolStr,
        inputs: FunctionInputsType,
        modifiers: Option<Vec<SmolStr>>,
    },
    Edge {
        node_name_from: SmolStr,
        node_name_to: SmolStr,
    },
    CloseDeclaration,
}

/// this structure contains function details extracted
/// from function declaration
#[derive(Clone, Debug)]
pub struct FunctionDeclaration {
    pub source_node_name: SmolStr,
    pub exit_node_name: SmolStr,
    pub input_type: FunctionInputsType,
    pub modifiers: Option<Vec<SmolStr>>,
    pub uses_wrapped_types: bool,
}

/// this structure stores the interral parser values needed
/// during code parsing
pub struct DotTokenizer<'a> {
    lexer: Lexer<'a, DotToken>,
    digraph_defined: bool,
    current_function_definition: Option<FunctionDeclaration>,
    call_ident_regex: Regex,
}

impl<'a> DotTokenizer<'a> {
    /// creates a DotTokenizer from a string with code
    pub fn from_str(source: &'a str) -> Self {
        DotTokenizer {
            lexer: DotToken::lexer(source),
            digraph_defined: false,
            current_function_definition: None,
            call_ident_regex: Regex::new(r"^call[0-9]+_[A-Za-z_][A-Za-z0-9_]*$").unwrap(),
        }
    }
}

macro_rules! return_some_err {
    ($expr:expr) => {{
        match $expr {
            Ok(value) => value,
            Err(err) => break Some(Err(err)),
        }
    }};
}

impl<'a> Iterator for DotTokenizer<'a> {
    type Item = Result<CodeUnit, SyntaxError>;

    /// parse the tokens returned by lexer and return parsed code units one by one
    fn next(&mut self) -> Option<Self::Item> {
        let mut ignore_subgraph = false;
        loop {
            if ignore_subgraph {
                match self.lexer.next()? {
                    DotToken::OpenDeclaration => {
                        break Some(Err(SyntaxError::new(format!(
                            "Nested braces {{}} in subgraphs are not supported"
                        ))))
                    }
                    DotToken::CloseDeclaration => ignore_subgraph = false,
                    _ => continue,
                }
            }
            match self.lexer.next()? {
                DotToken::Error => continue,
                DotToken::Digraph => {
                    if self.digraph_defined {
                        break Some(Err(SyntaxError::new(
                            format!("multiple digraph definitions are not supported")
                        )))
                    }
                    return_some_err!(handle_digraph(&mut self.lexer));
                    self.digraph_defined = true;
                },
                DotToken::Subgraph => {
                    match return_some_err!(try_parse_function_def(&mut self.lexer)) {
                        Some(mut func) => {
                            if self.current_function_definition.is_some() {
                                break Some(Err(SyntaxError::new(
                                    format!("nested functional node definitions are not supported")
                                )))
                            } else {
                                if func.uses_wrapped_types {
                                    if let FunctionInputsType::TypeNameList(type_list) = func.input_type {
                                        func.input_type = FunctionInputsType::TypeNameList(
                                            type_list.into_iter().map(|x| x.wrap_in_func(&func.source_node_name)).collect()
                                        );
                                    }
                                }
                                self.current_function_definition = Some(func.clone());
                                break Some(Ok(CodeUnit::FunctionDeclaration(func)))
                            }
                        },
                        _ => match self.lexer.next()? {
                            DotToken::OpenDeclaration => ignore_subgraph = true,
                            _ => break Some(Err(SyntaxError::new(
                                format!("Expected subgraph body: {}", self.lexer.slice())
                            )))
                        },
                    }
                },
                DotToken::Identifier(node_name) => {
                    match self.lexer.next() {
                        Some(DotToken::OpenSpecification) => {
                            let mut node_spec = return_some_err!(
                                read_opened_node_specification(&mut self.lexer, &node_name)
                            );

                            let mut type_name = match node_spec.remove("TYPE_NAME") {
                                Some(DotToken::QuotedIdentifiers(idents)) => Some(
                                    return_some_err!(SubgraphType::from_type_name(&idents))
                                ),
                                _ => None
                            };

                            if let Some(ref definition) = self.current_function_definition {
                                if definition.uses_wrapped_types {
                                    type_name = type_name.map(|x| x.wrap_in_func(&definition.source_node_name));
                                }
                            } else {
                                return Some(Err(SyntaxError::new(format!(
                                    "Node definitions outside of subgraphs are not allowed"
                                ))))
                            }

                            let trigger = match node_spec.remove("TRIGGER") {
                                Some(DotToken::QuotedIdentifiers(idents)) => {
                                    let trigger_on = match node_spec.remove("TRIGGER_MODE") {
                                        Some(DotToken::QuotedIdentifiers(mode_name)) => match mode_name.as_str() {
                                            mode_name @ ("on" | "off") => Some(mode_name == "on"), _ => None
                                        }, _ => None,
                                    };
                                    let trigger_on = match trigger_on {
                                        Some(v) => v,
                                        None => break Some(Err(SyntaxError::new(format!(
                                            "Expected trigger_mode=\"on\" or trigger_mode=\"off\" after trigger=\"{idents}\" for node {node_name}"
                                        )))),
                                    };
                                    Some((idents, trigger_on))
                                },
                                _ => None
                            };

                            let call_trigger_name = match node_spec.remove("CALL_TRIGGER") {
                                Some(DotToken::QuotedIdentifiers(idents)) => {
                                    Some(idents)
                                },
                                _ => None,
                            };

                            let affects_call_trigger_name = match node_spec.remove("AFFECTS_CALL_TRIGGER") {
                                Some(DotToken::QuotedIdentifiers(idents)) => {
                                    Some(idents)
                                },
                                _ => None,
                            };

                            let literal = match node_spec.remove("LITERAL") {
                                Some(DotToken::QuotedIdentifiers(idents)) => idents == SmolStr::new("t"),
                                _ => false
                            };

                            let node_common = NodeCommon {
                                name: node_name.clone(),
                                type_name,
                                trigger,
                                call_trigger_name,
                                affects_call_trigger_name
                            };

                            if self.call_ident_regex.is_match(&node_name) {
                                let (input_type, modifiers, uses_wrapped_types) = return_some_err!(
                                    parse_function_options(&node_name, node_spec)
                                );
                                if uses_wrapped_types.is_some() {
                                    break Some(Err(SyntaxError::new(format!(
                                        "call nodes can't have a uses_wrapped_types option, which is reserved for function definitions (node {node_name})"
                                    ))));
                                }
                                if literal {
                                    break Some(Err(SyntaxError::new(format!(
                                        "call nodes can't be declaread as literals (node {node_name})"
                                    ))));
                                }
                                let func_name = SmolStr::new(node_name.split_once('_').unwrap().1);
                                break Some(Ok(CodeUnit::CallNode {
                                    node_common,
                                    func_name,
                                    inputs: input_type,
                                    modifiers,
                                }));
                            } else {
                                break Some(Ok(CodeUnit::RegularNode {
                                    node_common,
                                    literal,
                                }));
                            }
                        },
                        Some(DotToken::Arrow) => {
                            match self.lexer.next() {
                                Some(DotToken::Identifier(node_name_to)) => {
                                    break Some(Ok(CodeUnit::Edge {
                                        node_name_from: node_name, node_name_to
                                    }))
                                },
                                _ => break Some(Err(SyntaxError::new(
                                    format!("Expected an identifier after arrow: {node_name} -> ...")
                                )))
                            }
                        },
                        _ => break Some(Err(SyntaxError::new(
                            format!("Expected node definition ({node_name}[...]) or edge definition ({node_name} -> ...)")
                        )))
                    }
                },
                DotToken::QuotedIdentifiers(name) => break Some(Err(SyntaxError::new(
                    format!("Quoted identifiers are not supported: \"{name}\"")
                ))),
                DotToken::QuotedIdentifiersWithBrackets(name) => break Some(Err(SyntaxError::new(
                    format!("Quoted identifiers are not supported: \"{name}\"")
                ))),
                DotToken::CloseDeclaration => {
                    if self.current_function_definition.is_some() {
                        self.current_function_definition = None;
                        break Some(Ok(CodeUnit::CloseDeclaration))
                    }
                },
                any => break Some(Err(SyntaxError::new(
                    format!("Unexpected {:?} : {}", any, self.lexer.slice())
                ))),
            }
        }
    }
}

/// handles a new subgraph definition
fn handle_digraph(lex: &mut Lexer<'_, DotToken>) -> Result<(), SyntaxError> {
    match lex.next() {
        None => Ok(()), // digraph at the end of file
        Some(token) => match token {
            DotToken::Identifier(_) => match lex.next() {
                Some(DotToken::OpenDeclaration) => Ok(()),
                _ => Err(SyntaxError::new(format!("expected digraph body"))),
            },
            DotToken::OpenDeclaration => Ok(()),
            _ => Err(SyntaxError::new(format!(
                "expected digraph identifier or digraph body"
            ))),
        },
    }
}

/// returns None if subgraph is not a function
fn try_parse_function_def(lex: &mut Lexer<'_, DotToken>) -> Result<Option<FunctionDeclaration>, SyntaxError> {
    match lex.next() {
        Some(DotToken::Identifier(function_name)) => {
            if !function_name.starts_with("def_") {
                return Ok(None);
            }
            let function_name = match function_name.get(4..) {
                Some(decl_node_name) => decl_node_name,
                None => return Err(SyntaxError::new(format!("subgraph name def_ is forbidden"))),
            };
            match lex.next() {
                Some(DotToken::OpenDeclaration) => match lex.next() {
                    Some(DotToken::Identifier(node_name)) => {
                        if node_name != function_name {
                            Err(SyntaxError::new(
                                    format!(
                                        "Source node definition should be the first in the subgraph def_{} and should be named {}",
                                        function_name, function_name
                                    )
                                ))
                        } else {
                            let (input_type, modifiers, uses_wrapped_types) = parse_function_options(
                                &node_name,
                                read_node_specification(lex, &node_name)?,
                            )?;
                            let exit_node_name = parse_function_exit_node_name(lex, &node_name)?;
                            Ok(Some(FunctionDeclaration {
                                source_node_name: node_name,
                                exit_node_name,
                                input_type,
                                modifiers,
                                uses_wrapped_types: uses_wrapped_types.unwrap_or(false),
                            }))
                        }
                    }
                    _ => Err(SyntaxError::new(format!("expected source node definition"))),
                },
                _ => Err(SyntaxError::new(format!("expected subgraph body"))),
            }
        }
        Some(DotToken::OpenDeclaration) => Ok(None),
        _ => Err(SyntaxError::new(format!(
            "expected subgraph identifier or subgraph body"
        ))),
    }
}

/// parse function options, either in function declaration or in function call
fn parse_function_options(
    node_name: &SmolStr,
    mut node_spec: HashMap<SmolStr, DotToken>,
) -> Result<(FunctionInputsType, Option<Vec<SmolStr>>, Option<bool>), SyntaxError> {
    let mut input_type = FunctionInputsType::None;
    let mut modifiers = Option::<Vec<SmolStr>>::None;
    let mut uses_wrapped_types = None;
    if let Some(token) = node_spec.remove("TYPES") {
        if let DotToken::QuotedIdentifiersWithBrackets(idents) = token {
            if let Some(special_type) = FunctionInputsType::try_extract_special_type(&idents) {
                input_type = special_type;
            } else {
                input_type = FunctionInputsType::TypeNameList(
                    FunctionInputsType::parse_types(idents)?,
                );
            }
        } else {
            return Err(SyntaxError::new(format!(
                "{node_name}[TYPES=... should take bracketed form: TYPES=\"[.., ]\""
            )));
        }
    }
    if let Some(token) = node_spec.remove("MODS") {
        if let DotToken::QuotedIdentifiersWithBrackets(value) = token {
            let mods = get_identifier_names(value)
                .iter()
                .filter(|el| el.len() > 0)
                .map(|el| el.to_owned())
                .collect::<Vec<_>>();

            if mods.len() > 0 {
                modifiers = Some(mods);
            }
        } else {
            return Err(SyntaxError::new(format!(
                "{node_name}[MODS=... should take bracketed form: MODS=\"[.., ]\""
            )));
        }
    }
    if let Some(token) = node_spec.remove("USES_WRAPPED_TYPES") {
        match token {
            DotToken::QuotedIdentifiers(value) if ["true", "false"].contains(&value.as_str()) => {
                uses_wrapped_types = Some(value == "true");
            },
            _ => {
                return Err(SyntaxError::new(format!(
                    "{node_name}[uses_wrapped_types=... can only be either \"true\" or \"false\" (default)"
                )));
            }
        }
    }
    Ok((input_type, modifiers, uses_wrapped_types))
}

/// expects and parses an exit node name after function declaration
fn parse_function_exit_node_name(
    lex: &mut Lexer<'_, DotToken>,
    node_name: &SmolStr,
) -> Result<SmolStr, SyntaxError> {
    let gen_exit_node_format_err = || {
        SyntaxError::new(format!(
            "Exit node should be defined after the {node_name} definition: EXIT_{node_name}[...]"
        ))
    };
    match lex.next() {
        Some(DotToken::Identifier(name)) => {
            if !name.starts_with("EXIT_") {
                Err(gen_exit_node_format_err())
            } else {
                read_node_specification(lex, &name)?;
                Ok(name)
            }
        }
        _ => Err(gen_exit_node_format_err()),
    }
}

/// read an opening bracket and a node specification afterwards
fn read_node_specification(
    lex: &mut Lexer<'_, DotToken>,
    node_name: &SmolStr,
) -> Result<HashMap<SmolStr, DotToken>, SyntaxError> {
    match lex.next() {
        Some(DotToken::OpenSpecification) => read_opened_node_specification(lex, node_name),
        _ => Err(SyntaxError::new(format!(
            "expected source node properties specifier {}[...]",
            node_name
        ))),
    }
}

/// read node specification after an opening bracket was read
fn read_opened_node_specification(
    lex: &mut Lexer<'_, DotToken>,
    node_name: &SmolStr,
) -> Result<HashMap<SmolStr, DotToken>, SyntaxError> {
    let mut props = HashMap::<_, _>::new();
    let gen_props_error = || {
        SyntaxError::new(format!(
            "expected source node property value: {}[PROP=\"VAL\", ...] or empty brackets: {}[]",
            node_name, node_name
        ))
    };
    loop {
        match lex.next() {
            Some(DotToken::Identifier(prop_name)) => match lex.next() {
                Some(DotToken::Equals) => match lex.next() {
                    Some(token @ DotToken::QuotedIdentifiersWithBrackets(_)) => {
                        props.insert(SmolStr::new(prop_name.to_uppercase()), token);
                    }
                    Some(token @ DotToken::QuotedIdentifiers(_)) => {
                        props.insert(SmolStr::new(prop_name.to_uppercase()), token);
                    }
                    Some(DotToken::Identifier(_)) => continue,
                    _ => break Err(gen_props_error()),
                },
                _ => break Err(gen_props_error()),
            },
            Some(DotToken::CloseSpecification) => break Ok(props),
            Some(DotToken::Comma) => continue,
            _ => break Err(gen_props_error()),
        }
    }
}
