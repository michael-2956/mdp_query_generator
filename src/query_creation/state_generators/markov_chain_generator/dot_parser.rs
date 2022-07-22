use core::fmt;
use std::error::Error;

use logos::{Logos, Lexer, Filter};
use regex::Regex;
use smol_str::SmolStr;

#[derive(Logos, Debug, PartialEq)]
enum DotToken {
    #[regex(r"#[^\n]*", logos::skip)] // Skip # line comments
    #[regex(r"//[^\n]*", logos::skip)] // Skip // line comments
    #[regex(r"/\*", block_comment)] // Skip block comments
    #[regex(r"\[[\n\r\s]*color[\n\r\s]*=[\n\r\s]*none[\n\r\s]*\]", logos::skip)] // Skip [color=none]
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

    #[regex(r"\}")]
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

fn identifier(lex: &mut Lexer<'_, DotToken>) -> SmolStr {
    SmolStr::new(lex.slice())
}

fn quoted_identifier(lex: &mut Lexer<'_, DotToken>) -> SmolStr {
    let quoted_ident = lex.slice();
    SmolStr::new(quoted_ident.get(1..quoted_ident.len()-1).unwrap())
}

fn quoted_identifier_with_brackets(lex: &mut Lexer<'_, DotToken>) -> SmolStr {
    let quoted_ident = lex.slice();
    let opening_bracket_pos = quoted_ident.find('[').unwrap();
    let closing_bracket_pos = quoted_ident.find(']').unwrap();
    SmolStr::new(quoted_ident.get(
        opening_bracket_pos+1..closing_bracket_pos
    ).unwrap())
}

/// Callback to skip block comments
fn block_comment(lex: &mut Lexer<'_, DotToken>) -> Filter<()> {
    if let Some(end) = lex.remainder().find("*/") {
        lex.bump(end + 2);
        Filter::Skip
    } else {
        Filter::Emit(())
    }
}

#[derive(Debug, PartialEq)]
pub enum FunctionInputsType {
    Any,
    None,
    Known,
    Compatible,
    TypeName(SmolStr),
    TypeNameList(Vec<SmolStr>),
    TypeNameVariants(Vec<SmolStr>),
}

impl FunctionInputsType {
    fn try_extract_special_type(idents: &SmolStr) -> Option<FunctionInputsType> {
        match idents.as_str() {
            "any" => Some(FunctionInputsType::Any),
            "known" => Some(FunctionInputsType::Known),
            "compatible" => Some(FunctionInputsType::Compatible),
            _ => None
        }
    }

    fn get_identifier_names(name_list_str: SmolStr) -> Vec<SmolStr> {
        name_list_str.split(',').map(
            |string| SmolStr::new(string.trim())
        ).collect::<Vec<_>>()
    }
}

#[derive(Debug)]
pub enum CodeUnit {
    Function {
        source_node_name: SmolStr,
        exit_node_name: SmolStr,
        input_type: FunctionInputsType,
        modifiers: Option<Vec<SmolStr>>,
    },
    NodeDef {
        name: SmolStr,
    },
    Call {
        name: SmolStr,
        inputs: FunctionInputsType,
        modifiers: Option<Vec<SmolStr>>,
    },
    Edge {
        node_name_from: SmolStr,
        node_name_to: SmolStr,
    },
}

pub struct DotTokenizer<'a> {
    lexer: Lexer<'a, DotToken>,
    digraph_defined: bool,
    in_function_definition: bool,
    call_ident_regex: Regex,
}

impl<'a> DotTokenizer<'a> {
    pub fn from_str(source: &'a str) -> Self {
        DotTokenizer {
            lexer: DotToken::lexer(source),
            digraph_defined: false,
            in_function_definition: false,
            call_ident_regex: Regex::new(r"^call[0-9]+_[A-Za-z_][A-Za-z0-9_]*$").unwrap()
        }
    }
}

#[derive(Debug)]
pub struct SyntaxError {
    reason: String,
}

impl SyntaxError {
    fn new(reason: String) -> Self {
        SyntaxError { reason }
    }
}

impl Error for SyntaxError { }

impl fmt::Display for SyntaxError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Error: {}", self.reason)
    }
}

impl<'a> Iterator for DotTokenizer<'a> {
    type Item = Result<CodeUnit, SyntaxError>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut ignore_subgraph = false;
        loop {
            if ignore_subgraph {
                match self.lexer.next()? {
                    DotToken::OpenDeclaration => break Some(Err(SyntaxError::new(
                        format!("Nested braces {{}} in subgraphs are not supported")
                    ))),
                    DotToken::CloseDeclaration => ignore_subgraph = false,
                    _ => continue
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
                    if let Err(err) = handle_digraph(&mut self.lexer) {
                        break Some(Err(err))
                    }
                    self.digraph_defined = true;
                },
                DotToken::Subgraph => {
                    match try_parse_function_def(&mut self.lexer) {
                        Ok(Some(func @ CodeUnit::Function {..})) => {
                            if self.in_function_definition {
                                break Some(Err(SyntaxError::new(
                                    format!("nested functional node definitions are not supported")
                                )))
                            } else {
                                self.in_function_definition = true;
                                break Some(Ok(func))
                            }
                        },
                        Ok(_) => {  // there is no function
                            match self.lexer.next()? {
                                DotToken::OpenDeclaration => ignore_subgraph = true,
                                _ => break Some(Err(SyntaxError::new(
                                    format!("Expected subgraph body: {}", self.lexer.slice())
                                )))
                            };
                        },
                        Err(err) => break Some(Err(err)),
                    }
                },
                DotToken::Identifier(node_name) => {
                    match self.lexer.next() {
                        Some(DotToken::OpenSpecification) => {
                            match read_opened_node_specification(&mut self.lexer, &node_name) {
                                Ok(node_spec) => {
                                    if self.call_ident_regex.is_match(&node_name) {
                                        let (input_type, modifiers) = match parse_function_options(
                                            &node_name, node_spec, true
                                        ) {
                                            Ok(options) => options,
                                            Err(err) => break Some(Err(err))
                                        };
                                        break Some(Ok(CodeUnit::Call {
                                            name: SmolStr::new(node_name.split_once('_').unwrap().1),
                                            inputs: input_type,
                                            modifiers
                                        }));
                                    } else {
                                        break Some(Ok(CodeUnit::NodeDef { name: node_name }));
                                    }
                                },
                                Err(err) => break Some(Err(err))
                            }
                        },
                        Some(DotToken::Arrow) => {
                            match self.lexer.next() {
                                Some(DotToken::Identifier(node_name_to)) => break Some(Ok(CodeUnit::Edge {
                                    node_name_from: node_name, node_name_to
                                })),
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
                    self.in_function_definition = false;
                },
                any => break Some(Err(SyntaxError::new(
                    format!("Unexpected {:?} : {}", any, self.lexer.slice())
                ))),
            }
        }
    }
}

fn handle_digraph(lex: &mut Lexer<'_, DotToken>) -> Result<(), SyntaxError> {
    match lex.next() {
        None => Ok(()),  // digraph at the end of file
        Some(token) => {
            match token {
                DotToken::Identifier(_) => {
                    match lex.next() {
                        Some(DotToken::OpenDeclaration) => Ok(()),
                        _ => Err(SyntaxError::new(
                            format!("expected digraph body")
                        ))
                    }
                },
                DotToken::OpenDeclaration => Ok(()),
                _ => Err(SyntaxError::new(
                    format!("expected digraph identifier or digraph body")
                ))
            }
        }
    }
}

/// returns None if subgraph is not a function
fn try_parse_function_def(lex: &mut Lexer<'_, DotToken>) -> Result<Option<CodeUnit>, SyntaxError> {
    match lex.next() {
        Some(DotToken::Identifier(function_name)) => {
            if !function_name.starts_with("def_") {
                return Ok(None);
            }
            let function_name = match function_name.get(4..) {
                Some(decl_node_name) => decl_node_name,
                None => return Err(SyntaxError::new(
                    format!("subgraph name def_ is forbidden")
                )),
            };
            match lex.next() {
                Some(DotToken::OpenDeclaration) => {
                    match lex.next() {
                        Some(DotToken::Identifier(node_name)) => {
                            if node_name != function_name {
                                Err(SyntaxError::new(
                                    format!(
                                        "Source node definition should be the first in the subgraph def_{} and should be named {}",
                                        function_name, function_name
                                    )
                                ))
                            } else {
                                let (input_type, modifiers) = parse_function_options(
                                    &node_name, read_node_specification(lex, &node_name)?, false
                                )?;
                                let exit_node_name = parse_function_exit_node_name(lex, &node_name)?;
                                Ok(Some(CodeUnit::Function {
                                    source_node_name: node_name,
                                    exit_node_name,
                                    input_type,
                                    modifiers
                                }))
                            }
                        },
                        _ => Err(SyntaxError::new(
                            format!("expected source node definition")
                        )),
                    }
                },
                _ => Err(SyntaxError::new(
                    format!("expected subgraph body")
                ))
            }
        },
        Some(DotToken::OpenDeclaration) => Ok(None),
        _ => Err(SyntaxError::new(
            format!("expected subgraph identifier or subgraph body")
        ))
    }
}

fn parse_function_options(node_name: &SmolStr, node_spec: Vec<(SmolStr, DotToken)>, call: bool) -> Result<(FunctionInputsType, Option<Vec<SmolStr>>), SyntaxError> {
    let mut input_type = FunctionInputsType::None;
    let mut modifiers = Option::<Vec<SmolStr>>::None;
    for (key, token) in node_spec {
        if (key == "TYPE" || key == "TYPES") && input_type != FunctionInputsType::None {
            return Err(SyntaxError::new(
                format!("two {key} definitions in {node_name}[...]")
            ))
        }
        if key == "MOD" && modifiers != None {
            return Err(SyntaxError::new(
                format!("two MOD definitions in {node_name}[...]")
            ))
        }
        if key == "TYPE" {
            if let DotToken::QuotedIdentifiers(idents) = token {
                if let Some(special_type) = FunctionInputsType::try_extract_special_type(&idents) {
                    input_type = special_type;
                } else {
                    let idents = FunctionInputsType::get_identifier_names(idents);
                    if call {
                        match idents.len() {
                            1 => {
                                input_type = FunctionInputsType::TypeName(idents[0].clone())
                            },
                            _ => return Err(SyntaxError::new(
                                format!("call..._{node_name}[TYPE=... takes only 1 parameter")
                            ))
                        }
                    } else {
                        input_type = FunctionInputsType::TypeNameVariants(idents);
                    }
                }
            } else {
                return Err(SyntaxError::new(
                    format!("{node_name}[TYPE=... should take unbracketed form: TYPE=\".., \"")
                ))
            }
        } else if key == "TYPES" {
            if let DotToken::QuotedIdentifiersWithBrackets(idents) = token {
                if let Some(special_type) = FunctionInputsType::try_extract_special_type(&idents) {
                    input_type = special_type;
                } else {
                    input_type = FunctionInputsType::TypeNameList(FunctionInputsType::get_identifier_names(idents));
                }
            } else {
                return Err(SyntaxError::new(
                    format!("{node_name}[TYPES=... should take bracketed form: TYPES=\"[.., ]\"")
                ))
            }
        } else if key == "MOD" {
            if let DotToken::QuotedIdentifiersWithBrackets(value) = token {
                let mods = FunctionInputsType::get_identifier_names(value)
                                                .iter().filter(|el| el.len() > 0)
                                                .map(|el| el.to_owned()).collect::<Vec<_>>();

                if mods.len() > 0 {
                    modifiers = Some(mods);
                }
            } else {
                return Err(SyntaxError::new(
                    format!("{node_name}[MOD=... should take bracketed form: MOD=\"[.., ]\"")
                ))
            }
        }
    }
    Ok((input_type, modifiers))
}

fn parse_function_exit_node_name(lex: &mut Lexer<'_, DotToken>, node_name: &SmolStr) -> Result<SmolStr, SyntaxError> {
    let gen_exit_node_format_err = || { SyntaxError::new(
        format!("Exit node should be defined after the {node_name} definition: EXIT_{node_name}[...]")
    )};
    match lex.next() {
        Some(DotToken::Identifier(name)) => {
            if !name.starts_with("EXIT_") {
                Err(gen_exit_node_format_err())
            } else {
                read_node_specification(lex, &name)?;
                Ok(name)
            }
        },
        _ => Err(gen_exit_node_format_err())
    }
}

fn read_node_specification(lex: &mut Lexer<'_, DotToken>, node_name: &SmolStr) -> Result<Vec<(SmolStr, DotToken)>, SyntaxError> {
    match lex.next() {
        Some(DotToken::OpenSpecification) => read_opened_node_specification(lex, node_name),
        _ => Err(SyntaxError::new(
            format!("expected source node properties specifier {}[...]", node_name)
        )),
    }
}

fn read_opened_node_specification(lex: &mut Lexer<'_, DotToken>, node_name: &SmolStr) -> Result<Vec<(SmolStr, DotToken)>, SyntaxError> {
    let mut props = Vec::<_>::new();
    let gen_props_error = || {
        SyntaxError::new(
            format!(
                "expected source node property value: {}[PROP=\"VAL\", ...] or empty brackets: {}[]",
                node_name,
                node_name
            )
        )
    };
    loop {
        match lex.next() {
            Some(DotToken::Identifier(prop_name)) => {
                match lex.next() {
                    Some(DotToken::Equals) => {
                        match lex.next() {
                            Some(token @ DotToken::QuotedIdentifiersWithBrackets(_)) => {
                                props.push((prop_name, token));
                            },
                            Some(token @ DotToken::QuotedIdentifiers(_)) => {
                                props.push((prop_name, token));
                            },
                            Some(DotToken::Identifier(_)) => continue,
                            _ => break Err(gen_props_error())
                        }
                    },
                    _ => break Err(gen_props_error())
                }
            }
            Some(DotToken::CloseSpecification) => break Ok(props),
            Some(DotToken::Comma) => continue,
            _ => break Err(gen_props_error())
        }
    }
}