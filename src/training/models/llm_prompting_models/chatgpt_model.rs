use std::{collections::HashMap, env, fmt, path::PathBuf};

use crate::query_creation::{query_generator::{call_modifiers::WildcardRelationsValue, query_info::{CheckAccessibility, ClauseContext, ColumnRetrievalOptions, IdentName, Relation}, value_choosers::QueryValueChooser}, state_generator::{markov_chain_generator::{markov_chain::NodeParams, StackFrame}, subgraph_type::SubgraphType}};

use super::{llm_prompts::LLMPrompts, ModelPredictionResult, PathwayGraphModel};

use chatgpt::{client::ChatGPT, config::{ChatGPTEngine, ModelConfiguration}, converse::Conversation, types::CompletionResponse};
use rand::distributions::WeightedIndex;
use sqlparser::ast::{DateTimeField, Ident, ObjectName, Query};
use tokio::runtime::{self, Runtime};

pub struct ChatGPTPromptingModel {
    /// stores unused decision context by call_stack depth\
    /// the decision context applies to the first non-choiuce\
    /// decision in the inserted subgraph
    decision_context_by_depth: Vec<Option<String>>,
    current_conversation: Option<Conversation>,
    /// the query string for the next value choice, set by the\
    /// set_choice_query_ast method of the QueryValueChooser trait
    value_choice_query_str: Option<String>,
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

    fn start_inference(&mut self) {
        self.decision_context_by_depth = vec![None];
        self.initiate_conversation();
    }

    fn end_inference(&mut self) {
        self.current_conversation = None;
    }

    fn predict(&mut self, call_stack: &Vec<StackFrame>, node_outgoing: Vec<NodeParams>, current_query_ast_opt: Option<&Query>) -> ModelPredictionResult {
        // Keep the decision_context_by_depth the same length as the call stack
        assert!(call_stack.len() <= self.decision_context_by_depth.len());
        self.decision_context_by_depth.truncate(call_stack.len());
        let decision_context_ind = self.decision_context_by_depth.len() - 1;
    
        let current_node = &call_stack.last().unwrap().function_context.current_node;
        let is_call_node = current_node.call_params.is_some();
        let current_node_name = &current_node.node_common.name;

        if is_call_node {
            // Add context for the subgraph that will be inserted next
            self.decision_context_by_depth.push(self.prompts_ref().get_call_node_context(current_node_name));
        }

        // No need to predict if only one outgoing node is available
        if node_outgoing.len() == 1 {
            return ModelPredictionResult::Some(node_outgoing.into_iter().map(|p| (1f64, p)).collect());
        }

        // Automatically select "qualified_column_name" and "interval_literal_format_string"
        // TODO 1: Remove this once the qualified_column_name/unqualified_column_name will be
        // decided AFTER column selection and not before. This will work for now.
        // TODO 2: I was just too lazy to let ChatGPT select the interval field so.
        for auto_select_state in [
            "qualified_column_name", "interval_literal_format_string"
        ] {
            if node_outgoing.iter().any(|node| node.node_common.name == auto_select_state) {
                return ModelPredictionResult::Some(node_outgoing.into_iter().map(
                    |node| (if node.node_common.name.as_str() == auto_select_state { 1f64 } else { 0f64 }, node)
                ).collect())
            }
        }

        // extract the decision context since it will now be spent on a decision
        let decision_context = (&mut self.decision_context_by_depth[decision_context_ind]).take();

        // Form the prompt
        let current_query_str = format!("{}", current_query_ast_opt.unwrap());
        let prompt = self.prompts_ref().generate_prompt(
            &current_query_str, current_node_name, &node_outgoing, decision_context
        ).unwrap();

        // Query the model and interpret the response
        let answer = self.get_model_answer(prompt);
        let option_nodes = self.prompts_ref().get_option_nodes(current_node_name).unwrap();
        let node_name = option_nodes.get(&answer).unwrap();

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
            value_choice_query_str: None,
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

    fn get_model_answer(&mut self, prompt: String) -> String {
        let response = self.query_model(prompt).unwrap();
        response.message().content.lines().last().unwrap().to_string()
    }

    fn prompts_ref(&self) -> &LLMPrompts {
        self.prompts.as_ref().unwrap()
    }

    /// returns selected node name
    // fn query_model_and_parse_response(&mut self, prompt: String, current_node_name: &SmolStr) -> &String {
    //     let response = self.query_model(prompt).unwrap();
    //     let answer = response.message().content.lines().last().unwrap();
    //     let option_nodes = self.prompts_ref().get_option_nodes(current_node_name).unwrap();
    //     option_nodes.get(answer).unwrap()
    // }

    fn generate_value_chooser_options_prompt<OptT>(
        &mut self, _task_key: &str, _options: Vec<OptT>
    ) -> (String, HashMap<String, OptT>)
    where
        OptT: fmt::Display
    {
        unimplemented!()
    }

    fn get_chosen_value<OptT>(&mut self, task_key: &str, options: Vec<OptT>) -> OptT 
    where
        OptT: fmt::Display + Clone
    {
        self.get_chosen_value_formatted(task_key, options, |s| s)
    }

    fn get_chosen_value_formatted<OptT, Fmt>(&mut self, task_key: &str, options: Vec<OptT>, formatter: Fmt) -> OptT 
    where
        OptT: fmt::Display + Clone,
        Fmt: Fn(String) -> String
    {
        let current_query_str = self.value_choice_query_str.take().unwrap();
        let (prompt, options_map) = self.prompts_ref().generate_value_chooser_options_prompt_formatted(
            current_query_str, task_key, options, formatter
        ).unwrap();
        options_map.get(&self.get_model_answer(prompt)).unwrap().clone()
    }

    fn get_generated_value(&mut self, task_key: &str) -> String {
        self.get_generated_value_formatted(task_key, |s| s)
    }

    fn get_generated_value_formatted<Fmt>(&mut self, task_key: &str, formatter: Fmt) -> String 
    where
        Fmt: Fn(String) -> String
    {
        let current_query_str = self.value_choice_query_str.take().unwrap();
        let prompt = self.prompts_ref().generate_value_chooser_generate_prompt_formatted(current_query_str, task_key, formatter).unwrap();
        self.get_model_answer(prompt)
    }
}

macro_rules! gen_format_fn {
    ({$($key:expr => $value:expr),* $(,)?}) => {{|mut s| {
        $(
            s = s.replace(&format!("{{{}}}", $key), &$value.to_string());
        )*
        s
    }}};
}

impl QueryValueChooser for ChatGPTPromptingModel {
    fn set_choice_query_ast(&mut self, current_query_ref: &Query) {
       self.value_choice_query_str = Some(format!("{current_query_ref}"));
    }

    fn choose_from_alias(&mut self) -> Ident {
        Ident::new(self.get_generated_value("from_alias"))
    }

    fn choose_table_name(&mut self, available_table_names: &Vec<ObjectName>) -> ObjectName {
        self.get_chosen_value("table_name", available_table_names.clone())
    }

    fn choose_from_column_renames(&mut self, n_columns: usize) -> Vec<Ident> {
        let do_rename = self.get_chosen_value("from_column_renames_do_rename", vec!["RENAME COLUMNS", "SKIP THIS STEP"]);
        let mut output = vec![];
        if do_rename == "RENAME COLUMNS" {
            for i in 1..=n_columns {
                let do_rename_column = self.get_chosen_value_formatted(
                    "from_column_renames", vec!["RENAME COLUMN", "STOP THE RENAMING"],
                    gen_format_fn!({"i" => i, "n" => n_columns})
                );
                if do_rename_column == "STOP THE RENAMING" {
                    break
                }
                output.push(Ident::new(self.get_generated_value_formatted(
                    "from_column_renames_generate_rename",
                    gen_format_fn!({"i" => i, "n" => n_columns})
                )));
            }
        }
        output
    }

    fn choose_select_alias(&mut self) -> Ident {
        Ident::new(self.get_generated_value("select_alias"))
    }

    fn choose_qualified_wildcard_relation<'a>(&mut self, clause_context: &'a ClauseContext, wildcard_relations: &WildcardRelationsValue) -> (Ident, &'a Relation) {
        let available_relations = wildcard_relations.relation_levels_selectable_by_qualified_wildcard
            .iter().filter(|x| !x.is_empty())
            .flat_map(|v| v.iter().cloned()).collect::<Vec<_>>();

        let rel_name = self.get_chosen_value("qualified_wildcard_relation", available_relations);

        let relation = clause_context.get_relation_by_name(&rel_name);

        (rel_name.into(), relation)
    }

    fn choose_aggregate_function_name(&mut self, func_names: Vec<&String>, _dist: WeightedIndex<f64>) -> ObjectName {
        let func_name = self.get_chosen_value("aggregate_function_name", func_names);
        ObjectName(vec![Ident::new(func_name)])
    }

    fn choose_select_alias_order_by(&mut self, aliases: &Vec<&IdentName>) -> Ident {
        self.get_chosen_value("select_alias_order_by", aliases.clone()).clone().into()
    }

    fn choose_column(&mut self, _clause_context: &ClauseContext, _column_types: Vec<SubgraphType>, _check_accessibility: CheckAccessibility, _column_retrieval_options: ColumnRetrievalOptions) -> (SubgraphType, [IdentName; 2]) {
        let (_prompt, _options_map) = self.generate_value_chooser_options_prompt("column", vec![""]);
        todo!()
    }

    fn choose_bigint(&mut self) -> String {
        let (_prompt, _options_map) = self.generate_value_chooser_options_prompt("bigint", vec![""]);
        todo!()
    }

    fn choose_integer(&mut self) -> String {
        let (_prompt, _options_map) = self.generate_value_chooser_options_prompt("integer", vec![""]);
        todo!()
    }

    fn choose_numeric(&mut self) -> String {
        let (_prompt, _options_map) = self.generate_value_chooser_options_prompt("numeric", vec![""]);
        todo!()
    }

    fn choose_text(&mut self) -> String {
        let (_prompt, _options_map) = self.generate_value_chooser_options_prompt("text", vec![""]);
        todo!()
    }

    fn choose_date(&mut self) -> String {
        let (_prompt, _options_map) = self.generate_value_chooser_options_prompt("date", vec![""]);
        todo!()
    }

    fn choose_timestamp(&mut self) -> String {
        let (_prompt, _options_map) = self.generate_value_chooser_options_prompt("timestamp", vec![""]);
        todo!()
    }

    fn choose_interval(&mut self, _with_field: bool) -> (String, Option<DateTimeField>) {
        let (_prompt, _options_map) = self.generate_value_chooser_options_prompt("interval", vec![""]);
        todo!()
    }

    fn reset(&mut self) { }
}
