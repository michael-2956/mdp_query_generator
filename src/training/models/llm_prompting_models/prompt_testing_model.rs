use std::path::PathBuf;

use crate::query_creation::state_generator::markov_chain_generator::{markov_chain::NodeParams, StackFrame};

use super::{llm_prompts::LLMPrompts, ModelPredictionResult, PathwayGraphModel};

use sqlparser::ast::Query;

pub struct PromptTestingModel {
    prompts: Option<LLMPrompts>,
}

impl PathwayGraphModel for PromptTestingModel {
    fn process_state(&mut self, _call_stack: &Vec<StackFrame>, _popped_stack_frame: Option<&StackFrame>) {
        panic!("The ChatGPT prompting model cannot be trained")
    }

    fn write_weights(&self, _file_path: &PathBuf) -> std::io::Result<()> {
        panic!("The ChatGPT prompting model does not have the ability to write weights")
    }

    /// This loads the prompts from the toml file
    fn load_weights(&mut self, file_path: &PathBuf) -> std::io::Result<()> {
        self.prompts = Some(LLMPrompts::read_from_file(file_path)?);
        for value_chooser_key in [
            "table_name", "column", "bigint",
            "select_alias_order_by", "aggregate_function_name",
            "integer", "numeric", "text", "date",
            "timestamp", "interval", "qualified_wildcard_relation",
            "select_alias", "from_alias",
            "from_column_renames_do_rename",
            "from_column_renames_do_rename_column",
            "from_column_renames_generate_rename",
        ] {
            if !self.prompts_ref().has_value_chooser_key(value_chooser_key) {
                panic!("No prompt set for value chooser: {value_chooser_key}")
            }
        }
        Ok(())
    }

    fn predict(&mut self, call_stack: &Vec<StackFrame>, node_outgoing: Vec<NodeParams>, current_query_ast_opt: Option<&Query>) -> ModelPredictionResult {
        if node_outgoing.len() == 1 {
            return ModelPredictionResult::Some(node_outgoing.into_iter().map(|p| (1f64, p)).collect());
        }

        // TODO: Remove this once the qualified_column_name/unqualified_column_name will be
        // decided AFTER column selection and not before. This will work for now.
        if node_outgoing.iter().any(|node| [
            "qualified_column_name", "unqualified_column_name"
        ].contains(&node.node_common.name.as_str())) {
            return ModelPredictionResult::Some(node_outgoing.into_iter().map(
                |node| (if node.node_common.name.as_str() == "qualified_column_name" {
                    1f64
                } else { 0f64 }, node)
            ).collect())
        }

        let current_query_str = format!("{}", current_query_ast_opt.unwrap());
        let current_node = &call_stack.last().unwrap().function_context.current_node.node_common.name;

        let mut pass = true;
        let num_templates = current_query_str.match_indices("[?]").count();
        if num_templates == 0 {
            pass = [
                "call0_FROM_item",
            ].contains(&current_node.as_str());
        }

        let prompt = self.prompts_ref().generate_prompt(
            &current_query_str, current_node, &node_outgoing, None
        );
        if prompt.is_none() {
            pass = false;
        }

        if !pass {
            let prompt_str = if let Some((prompt, _valid_options)) = prompt {
                let option_nodes = self.prompts_ref().get_option_nodes(current_node);
                format!("\n{prompt}\nOption Nodes: {:#?}", option_nodes)
            } else { format!("ABSCENT") };
            let outgoing_str = node_outgoing.iter().map(|node| format!("{} ", node.node_common.name)).collect::<String>();
            panic!("Query: {current_query_str}\n# templates: {num_templates}\nCurrent node: {current_node}\nOutgoing nodes: {outgoing_str}\nPrompts: {prompt_str}")
        }
        
        ModelPredictionResult::None(node_outgoing)
    }
}

impl PromptTestingModel {
    pub fn new() -> Self {
        Self {
            prompts: None
        }
    }

    fn prompts_ref(&self) -> &LLMPrompts {
        self.prompts.as_ref().unwrap()
    }
}
