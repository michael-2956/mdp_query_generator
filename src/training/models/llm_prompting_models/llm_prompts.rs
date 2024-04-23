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
    transitions: HashMap<String, TransitionPrompts>,
    /// the context that each call node provides the first decision
    /// of its subgraph with
    call_node_context: HashMap<String, String>,
    /// the tasks for each of the value choosers are stored here
    value_chooser_tasks: HashMap<String, String>,
    /// whether to print the prompts and responses
    pub print_prompts_and_responses: bool,
    pub query_requests: Vec<String>,
    pub backtrack_prompt: String,
}

#[derive(Debug, Deserialize)]
struct MainPromptsFile {
    additional_prompts_folder: PathBuf,
    print_prompts_and_responses: bool,
    query_requests_file_path: PathBuf,
    backtrack_prompt: String,
    system_prompt: String,
}

#[derive(Debug, Deserialize)]
struct SupplementaryPromptsFile {
    transitions: HashMap<String, TransitionPrompts>,
    call_node_context: HashMap<String, String>,
    value_chooser_tasks: HashMap<String, String>,
}

#[derive(Debug, Deserialize)]
struct QueryRequests {
    requests: Vec<String>
}

impl LLMPrompts {
    pub fn read_from_file(file_path: &PathBuf) -> std::io::Result<Self> {
        let prompts_toml_str = fs::read_to_string(file_path)?;
        let contents: MainPromptsFile = toml::from_str(&prompts_toml_str).unwrap();
        let mut transitions = HashMap::new();
        let mut call_node_context = HashMap::new();
        let mut value_chooser_tasks = HashMap::new();
        for entry in fs::read_dir(contents.additional_prompts_folder).unwrap() {
            let supp_contents: SupplementaryPromptsFile = toml::from_str(&fs::read_to_string(&entry.unwrap().path()).unwrap()).unwrap();
            transitions.extend(supp_contents.transitions.into_iter());
            call_node_context.extend(supp_contents.call_node_context.into_iter());
            value_chooser_tasks.extend(supp_contents.value_chooser_tasks.into_iter());
        }
        let query_requests: QueryRequests = toml::from_str(&fs::read_to_string(&contents.query_requests_file_path).unwrap()).unwrap();
        Ok(Self {
            print_prompts_and_responses: contents.print_prompts_and_responses,
            backtrack_prompt: contents.backtrack_prompt,
            query_requests: query_requests.requests,
            system_prompt: contents.system_prompt,
            transitions,
            call_node_context,
            value_chooser_tasks,
        })
    }

    /// returns the prompts and a mapping from the option number to the selected node
    pub fn generate_prompt(&self, current_query_str: &String, current_node: &SmolStr, outgoing_nodes: &Vec<NodeParams>, decision_context: Option<String>) -> Option<(String, Vec<String>)> {
        let transition_prompts = self.transitions.get(current_node.as_str())?;
        let mut valid_options = vec![];
        let options_prompt: String = transition_prompts.options.iter().sorted_by(
            |(a_k, _), (b_k, _)| Ord::cmp(*a_k, *b_k)
        ).filter_map(|(opt_key, opt_prompt)| {
            let option_node = transition_prompts.option_nodes.get(opt_key).unwrap();
            if outgoing_nodes.iter().any(|node| node.node_common.name == option_node) {
                valid_options.push(opt_key.clone());
                Some(format!("    {opt_key}) {opt_prompt}\n"))
            } else { None }
        }).collect();
        let context_str = if let Some(context) = decision_context {
            format!("Context: {context}\n\n")
        } else { "".to_string() };
        Some((format!(
            "Current SQL Query: {current_query_str}\n\nDecision Required: {}\n\n{context_str}Options:\n{options_prompt}",
            transition_prompts.task
        ), valid_options))
    }

    pub fn get_option_nodes(&self, current_node: &SmolStr) -> Option<&HashMap<String, String>> {
        let transition_prompts = self.transitions.get(current_node.as_str())?;
        Some(&transition_prompts.option_nodes)
    }

    pub fn get_call_node_context(&self, node_name: &SmolStr) -> Option<String> {
        self.call_node_context.get(node_name.as_str()).cloned()
    }

    pub fn has_value_chooser_key(&self, task_key: &str) -> bool {
        self.value_chooser_tasks.contains_key(task_key)
    }

    pub fn generate_value_chooser_options_prompt_formatted<OptT, Fmt>(
        &self, current_query_str: String, task_key: &str, options: Vec<OptT>,
        formatter: Fmt
    ) -> Option<(String, HashMap<String, OptT>)>
    where
        OptT: fmt::Display,
        Fmt: Fn(String) -> String
    {
        let task_str = formatter(self.value_chooser_tasks.get(task_key)?.clone());
        let mut options_prompt = "".to_string();
        let mut option_nodes = HashMap::new();
        for (mut opt_key, opt_prompt) in options.into_iter().enumerate() {
            opt_key += 1;
            options_prompt += format!("    {opt_key}) {opt_prompt}\n").as_str();
            option_nodes.insert(opt_key.to_string(), opt_prompt);
        }
        Some((format!(
            "Current SQL Query: {current_query_str}\n\nDecision Required: {task_str}\n\nOptions:\n{options_prompt}"
        ), option_nodes))
    }

    pub fn generate_value_chooser_generate_prompt_formatted<Fmt>(
        &self, current_query_str: String, task_key: &str,
        formatter: Fmt
    ) -> Option<String> 
    where
        Fmt: Fn(String) -> String
    {
        let task_str = formatter(self.value_chooser_tasks.get(task_key)?.clone());
        Some(format!(
            "Query: {current_query_str}\n\nDecision Required: {task_str}"
        ))
    }
}
