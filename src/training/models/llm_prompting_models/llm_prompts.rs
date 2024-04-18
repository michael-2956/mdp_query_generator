use std::{collections::HashMap, fmt, fs, path::PathBuf};

use itertools::Itertools;
use serde::Deserialize;
use smol_str::SmolStr;

use crate::query_creation::state_generator::markov_chain_generator::markov_chain::NodeParams;

#[derive(Debug, Deserialize)]
pub struct TransitionPrompts {
    /// For example: """
    /// Select whether you want a column name or an expression.
    /// """
    task: String,
    /// For example: {\
    ///   "1": "column name",\
    ///   "2": "expression",\
    /// }
    options: HashMap<String, String>,
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
    /// the taskds for each of the value choosers are stored here
    pub value_chooser_tasks: HashMap<String, String>,
}

impl LLMPrompts {
    pub fn read_from_file(file_path: &PathBuf) -> std::io::Result<Self> {
        let prompts_toml_str = fs::read_to_string(file_path)?;
        let mut _self: Self = toml::from_str(prompts_toml_str.as_str()).unwrap();
        Ok(_self)
    }

    /// returns the prompts and a mapping from the option number to the selected node
    pub fn generate_prompt(&self, current_query_str: &String, current_node: &SmolStr, outgoing_nodes: &Vec<NodeParams>, decision_context: Option<String>) -> Option<String> {
        let transition_prompts = self.transitions.get(current_node.as_str())?;
        let options_prompt: String = transition_prompts.options.iter().sorted_by(
            |(a_k, _), (b_k, _)| Ord::cmp(*a_k, *b_k)
        ).filter_map(|(opt_key, opt_prompt)| {
            let option_node = transition_prompts.option_nodes.get(opt_key).unwrap();
            if outgoing_nodes.iter().any(|node| node.node_common.name == option_node) {
                Some(format!("    {opt_key}) {opt_prompt}\n"))
            } else { None }
        }).collect();
        let context_str = if let Some(context) = decision_context {
            format!("Context: {context}\n\n")
        } else { "".to_string() };
        Some(format!(
            "Query: {current_query_str}\n\nTask: {}\n\n{context_str}Options:\n{options_prompt}",
            transition_prompts.task
        ))
    }

    pub fn get_option_nodes(&self, current_node: &SmolStr) -> Option<&HashMap<String, String>> {
        let transition_prompts = self.transitions.get(current_node.as_str())?;
        Some(&transition_prompts.option_nodes)
    }

    pub fn get_call_node_context(&self, node_name: &SmolStr) -> Option<String> {
        self.call_node_context.get(node_name.as_str()).cloned()
    }

    pub fn generate_value_chooser_options_prompt<OptT>(
        &self, current_query_str: String, task_key: &str, options: Vec<OptT>
    ) -> Option<(String, HashMap<String, OptT>)>
    where
        OptT: fmt::Display
    {
        let task_str = self.value_chooser_tasks.get(task_key)?;
        let mut options_prompt = "".to_string();
        let mut option_nodes = HashMap::new();
        for (opt_key, opt_prompt) in options.into_iter().enumerate() {
            options_prompt += format!("    {opt_key}) {opt_prompt}\n").as_str();
            option_nodes.insert(opt_key.to_string(), opt_prompt);
        }
        Some((format!(
            "Query: {current_query_str}\n\nTask: {task_str}\n\nOptions:\n{options_prompt}"
        ), option_nodes))
    }
}
