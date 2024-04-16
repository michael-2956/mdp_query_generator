use std::{env, path::PathBuf};

use crate::query_creation::{query_generator::{call_modifiers::WildcardRelationsValue, query_info::{CheckAccessibility, ClauseContext, ColumnRetrievalOptions, IdentName, Relation}, value_choosers::QueryValueChooser}, state_generator::{markov_chain_generator::{markov_chain::NodeParams, StackFrame}, subgraph_type::SubgraphType}};

use super::{llm_prompts::LLMPrompts, ModelPredictionResult, PathwayGraphModel};

use chatgpt::{client::ChatGPT, config::{ChatGPTEngine, ModelConfiguration}, converse::Conversation, types::CompletionResponse};
use rand::distributions::WeightedIndex;
use sqlparser::ast::{DateTimeField, Ident, ObjectName, Query};
use tokio::runtime::{self, Runtime};

pub struct ChatGPTPromptingModel {
    decision_context_by_depth: Vec<Option<String>>,
    current_conversation: Option<Conversation>,
    prompts: Option<LLMPrompts>,
    async_runtime: Runtime,
    client: ChatGPT,
}

impl PathwayGraphModel for ChatGPTPromptingModel {
    fn process_state(&mut self, _call_stack: &Vec<StackFrame>, _popped_stack_frame: Option<&StackFrame>) {
        panic!("The ChatGPT prompting model cannot be trained")
    }

    fn write_weights(&self, _file_path: &PathBuf) -> std::io::Result<()> {
        panic!("The ChatGPT prompting model does not have the ability to write weights")
    }

    /// This loads the prompts from the toml file
    fn load_weights(&mut self, file_path: &PathBuf) -> std::io::Result<()> {
        self.prompts = Some(LLMPrompts::read_from_file(file_path)?);
        Ok(())
    }

    fn predict(&mut self, call_stack: &Vec<StackFrame>, node_outgoing: Vec<NodeParams>, current_query_ast_opt: Option<&Query>) -> ModelPredictionResult {
        // Keep the decision_context_by_depth the same length as the call stack
        assert!(call_stack.len() <= self.decision_context_by_depth.len());
        self.decision_context_by_depth.truncate(call_stack.len());
        let decision_context = self.decision_context_by_depth.last().unwrap().clone();
    
        // If we are at the start of the generation proces, initiate the convesation with chatgpt
        if let &[ref main_func] = call_stack.as_slice() {
            if main_func.function_context.current_node.node_common.name == "Query" {
                self.initiate_conversation();
            }
        }
        
        let current_node = &call_stack.last().unwrap().function_context.current_node;
        let is_call_node = current_node.call_params.is_some();
        let current_node_name = &current_node.node_common.name;

        // Add context for the inserted subgraphs first decision
        if is_call_node {
            self.decision_context_by_depth.push(self.prompts_ref().get_call_node_context(current_node_name));
        }

        // No need to predict if only one outgoing node is available
        if node_outgoing.len() == 1 {
            return ModelPredictionResult::Some(node_outgoing.into_iter().map(|p| (1f64, p)).collect());
        }

        // Automatically select a qualified column name.
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

        // Form the prompt
        let current_query_str = format!("{}", current_query_ast_opt.unwrap());
        let prompt = self.prompts_ref().generate_prompt(
            &current_query_str, current_node_name, &node_outgoing, decision_context
        ).unwrap();

        // Query the model and interpret the response
        let response = self.query_model(prompt).unwrap();
        let answer = response.message().content.lines().last().unwrap();
        let option_nodes = self.prompts_ref().get_option_nodes(current_node_name).unwrap();
        let node_name = option_nodes.get(answer).unwrap();

        // Return the prediction result
        ModelPredictionResult::Some(node_outgoing.into_iter().map(
            |node| (if node.node_common.name.as_str() == node_name {
                1f64
            } else { 0f64 }, node)
        ).collect())
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

impl ChatGPTPromptingModel {
    pub fn new() -> Self {
        let mut model_config = ModelConfiguration::default();
        model_config.engine = ChatGPTEngine::Custom("gpt-3.5-turbo-0125");
        let mut _self = Self {
            decision_context_by_depth: vec![],
            prompts: None,
            async_runtime: runtime::Builder::new_current_thread().build().unwrap(),
            client: ChatGPT::new_with_config(
                env::var("OPENAI_API_KEY").unwrap(), model_config
            ).unwrap(),
            current_conversation: None,
        };
        _self
    }

    fn initiate_conversation(&mut self) {
        self.current_conversation = Some(self.client.new_conversation_directed(
            self.prompts.as_ref().unwrap().system_prompt.clone()
        ));
    }

    fn query_model(&mut self, prompt: String) -> chatgpt::Result<CompletionResponse> {
        self.async_runtime.block_on(async {
            self.current_conversation.as_mut().unwrap().send_message(prompt).await
        })
    }

    fn prompts_ref(&self) -> &LLMPrompts {
        self.prompts.as_ref().unwrap()
    }
}

impl QueryValueChooser for ChatGPTPromptingModel {
    fn choose_table_name(&mut self, _available_table_names: &Vec<ObjectName>) -> ObjectName {
        let _task = self.prompts_ref().get_value_chooser_task("table_name");
        todo!()
    }

    fn choose_column(&mut self, _clause_context: &ClauseContext, _column_types: Vec<SubgraphType>, _check_accessibility: CheckAccessibility, _column_retrieval_options: ColumnRetrievalOptions) -> (SubgraphType, [IdentName; 2]) {
        let _task = self.prompts_ref().get_value_chooser_task("column");
        todo!()
    }

    fn choose_select_alias_order_by(&mut self, _aliases: &Vec<&IdentName>) -> Ident {
        let _task = self.prompts_ref().get_value_chooser_task("select_alias_order_by");
        todo!()
    }

    fn choose_aggregate_function_name(&mut self, _func_names_iter: Vec<&String>, _dist: WeightedIndex<f64>) -> ObjectName {
        let _task = self.prompts_ref().get_value_chooser_task("aggregate_function_name");
        todo!()
    }

    fn choose_bigint(&mut self) -> String {
        let _task = self.prompts_ref().get_value_chooser_task("bigint");
        todo!()
    }

    fn choose_integer(&mut self) -> String {
        let _task = self.prompts_ref().get_value_chooser_task("integer");
        todo!()
    }

    fn choose_numeric(&mut self) -> String {
        let _task = self.prompts_ref().get_value_chooser_task("numeric");
        todo!()
    }

    fn choose_text(&mut self) -> String {
        let _task = self.prompts_ref().get_value_chooser_task("text");
        todo!()
    }

    fn choose_date(&mut self) -> String {
        let _task = self.prompts_ref().get_value_chooser_task("date");
        todo!()
    }

    fn choose_timestamp(&mut self) -> String {
        let _task = self.prompts_ref().get_value_chooser_task("timestamp");
        todo!()
    }

    fn choose_interval(&mut self, _with_field: bool) -> (String, Option<DateTimeField>) {
        let _task = self.prompts_ref().get_value_chooser_task("interval");
        todo!()
    }

    fn choose_qualified_wildcard_relation<'a>(&mut self, _clause_context: &'a ClauseContext, _wildcard_relations: &WildcardRelationsValue) -> (Ident, &'a Relation) {
        let _task = self.prompts_ref().get_value_chooser_task("qualified_wildcard_relation");
        todo!()
    }

    fn choose_select_alias(&mut self) -> Ident {
        let _task = self.prompts_ref().get_value_chooser_task("select_alias");
        todo!()
    }

    fn choose_from_alias(&mut self) -> Ident {
        let _task = self.prompts_ref().get_value_chooser_task("from_alias");
        todo!()
    }

    fn choose_from_column_renames(&mut self, _n_columns: usize) -> Vec<Ident> {
        let _task = self.prompts_ref().get_value_chooser_task("from_column_renames");
        todo!()
    }

    fn reset(&mut self) { }
}
