use std::{collections::HashMap, fs, path::PathBuf};

use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct TransitionPrompts {
    /// For example: """
    /// Select whether you want a column name or an expression.
    /// """
    task: String,
    /// For example: """
    /// 1) column name
    /// 2) expression
    /// """
    options: String,
    /// For example: {\
    ///   "1": "call0_WHERE",\
    ///   "2": "call0_GROUP_BY",\
    /// }
    option_nodes: HashMap<String, String>,
}

#[derive(Debug, Deserialize)]
pub struct LLMPrompts {
    /// the system prompts of the LLM agent
    pub system_prompt: String,
    /// the node transition prompts
    pub transitions: HashMap<String, TransitionPrompts>,
    /// the context that each call node provides the first decision
    /// of its subgraph with
    pub call_node_context: HashMap<String, String>,
}

impl LLMPrompts {
    pub fn read_from_file(file_path: &PathBuf) -> std::io::Result<Self> {
        let prompts_toml_str = fs::read_to_string(file_path)?;
        let _self = toml::from_str(prompts_toml_str.as_str()).unwrap();
        Ok(_self)
    }
}
