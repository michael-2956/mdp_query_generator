use std::{collections::HashMap, env, fmt, hash::RandomState, path::PathBuf};

use crate::query_creation::{query_generator::{call_modifiers::WildcardRelationsValue, query_info::{CheckAccessibility, ClauseContext, ColumnRetrievalOptions, IdentName, Relation}, value_choosers::QueryValueChooser}, state_generator::{markov_chain_generator::{markov_chain::NodeParams, StackFrame}, subgraph_type::SubgraphType}};

use super::{llm_prompts::LLMPrompts, ModelPredictionResult, PathwayGraphModel};

use chatgpt::{client::ChatGPT, config::{ChatGPTEngine, ModelConfiguration}, converse::Conversation, types::CompletionResponse};
use rand::distributions::WeightedIndex;
use sqlparser::ast::{DateTimeField, Ident, ObjectName, Query};
use tokio::runtime::{self, Runtime};

macro_rules! gen_format_fn {
    ({$($key:expr => $value:expr),* $(,)?}) => {{|mut s: String| {
        $(
            s = s.replace(&format!("{{{}}}", $key), &$value.to_string());
        )*
        s
    }}};
}

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

    fn start_inference(&mut self, schema_string: String) {
        self.decision_context_by_depth = vec![None];
        self.initiate_conversation(schema_string);
    }

    fn end_inference(&mut self) {
        self.current_conversation = None;
    }

    fn predict(&mut self, call_stack: &Vec<StackFrame>, node_outgoing: Vec<NodeParams>, current_query_ast_opt: Option<&Query>) -> ModelPredictionResult {
        // Keep the decision_context_by_depth the same length as the call stack
        assert!(call_stack.len() <= self.decision_context_by_depth.len());
        self.decision_context_by_depth.truncate(call_stack.len());
        let decision_context_ind = self.decision_context_by_depth.len() - 1;

        // No need to predict if only one outgoing node is available
        if let [ref single_node] = node_outgoing.as_slice() {
            let single_node_name = single_node.node_common.name.clone();
            return self.produce_prediction(node_outgoing, &single_node_name)
        }

        // Automatically select "qualified_column_name" and "interval_literal_format_string"
        // TODO 1: Remove this once the qualified_column_name/unqualified_column_name will be
        // decided AFTER column selection and not before. This will work for now.
        // TODO 2: I was just too lazy to let ChatGPT select the interval field so.
        for auto_select_state in [
            "qualified_column_name", "interval_literal_format_string"
        ] {
            if node_outgoing.iter().any(|node| node.node_common.name == auto_select_state) {
                return self.produce_prediction(node_outgoing, auto_select_state)
            }
        }

        // extract the decision context since it will now be spent on a decision
        let decision_context = (&mut self.decision_context_by_depth[decision_context_ind]).take();

        // Form the prompt
        let current_node_name = &call_stack.last().unwrap().function_context.current_node.node_common.name;
        let current_query_str = format!("{}", current_query_ast_opt.unwrap());
        let prompt = self.prompts_ref().generate_prompt(
            &current_query_str, current_node_name, &node_outgoing, decision_context
        ).unwrap();

        // Query the model and interpret the response
        let answer = self.get_model_answer(prompt);
        let option_nodes = self.prompts_ref().get_option_nodes(current_node_name).unwrap();
        let node_name = option_nodes.get(&answer).unwrap().clone();

        // Return the prediction result
        self.produce_prediction(node_outgoing, &node_name)
    }

    fn as_value_chooser(&mut self) -> Option<&mut dyn QueryValueChooser> { Some(self) }
}

impl ChatGPTPromptingModel {
    pub fn new() -> Self {
        let mut model_config = ModelConfiguration::default();
        model_config.engine = ChatGPTEngine::Custom("gpt-3.5-turbo-0125");
        let mut _self = Self {
            value_choice_query_str: None,
            decision_context_by_depth: vec![],
            prompts: None,
            async_runtime: runtime::Builder::new_current_thread().enable_all().build().unwrap(),
            client: ChatGPT::new_with_config(
                env::var("OPENAI_API_KEY").unwrap(), model_config
            ).unwrap(),
            current_conversation: None,
        };
        _self
    }

    fn produce_prediction(&mut self, node_outgoing: Vec<NodeParams>, selected_node: &str) -> ModelPredictionResult {
        if let Some(call_node) = node_outgoing.iter().find(
            // if we selected a call node
            |node| node.call_params.is_some() && node.node_common.name.as_str() == selected_node
        ) {
            // add call node context to stack
            self.decision_context_by_depth.push(self.prompts_ref().get_call_node_context(&call_node.node_common.name));
        }
        ModelPredictionResult::Some(node_outgoing.into_iter().map(
            |node| (if node.node_common.name.as_str() == selected_node { 1f64 } else { 0f64 }, node)
        ).collect())
    }

    fn initiate_conversation(&mut self, schema_string: String) {
        let direction_message = gen_format_fn!({
            "request" => "Count the total number of orders",
            "schema" => schema_string,
        })(self.prompts.as_ref().unwrap().system_prompt.clone());
        eprintln!("Direction message: {direction_message}\n======================\n======================\n");
        self.current_conversation = Some(self.client.new_conversation_directed(direction_message));
    }

    fn query_model(&mut self, prompt: String) -> chatgpt::Result<CompletionResponse> {
        self.async_runtime.block_on(async {
            self.current_conversation.as_mut().unwrap().send_message(prompt).await
        })
    }

    fn get_model_answer(&mut self, prompt: String) -> String {
        eprintln!("Prompt: {prompt}");
        let response = self.query_model(prompt).unwrap();
        eprintln!("Response: {}", response.message().content);
        let last_line = response.message().content.trim_end_matches('\n').lines().last().unwrap().to_string();
        eprintln!("Last line: {}\n======================\n======================\n", last_line);
        last_line
    }

    fn prompts_ref(&self) -> &LLMPrompts {
        self.prompts.as_ref().unwrap()
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

    fn choose_bigint(&mut self) -> String {
        self.get_generated_value("bigint")
    }

    fn choose_integer(&mut self) -> String {
        self.get_generated_value("integer")
    }

    fn choose_numeric(&mut self) -> String {
        self.get_generated_value("numeric")
    }

    fn choose_text(&mut self) -> String {
        self.get_generated_value("text")
    }

    fn choose_date(&mut self) -> String {
        self.get_generated_value("date")
    }

    fn choose_timestamp(&mut self) -> String {
        self.get_generated_value("timestamp")
    }

    fn choose_interval(&mut self, _with_field: bool) -> (String, Option<DateTimeField>) {
        (self.get_generated_value("interval"), None)
    }

    fn choose_column(&mut self, clause_context: &ClauseContext, column_types: Vec<SubgraphType>, check_accessibility: CheckAccessibility, column_retrieval_options: ColumnRetrievalOptions) -> (SubgraphType, [IdentName; 2]) {
        let columns: Vec<_> = clause_context.get_non_empty_column_levels_by_types(
            column_types.clone(), check_accessibility, column_retrieval_options.clone()
        ).into_iter().flat_map(|v| v.into_iter()).collect();

        let column_options_map = HashMap::<_, _, RandomState>::from_iter(columns.iter().map(
            |(tp, [rel_name, col_name])| (
                format!("Column {} of type {} from relation {}", col_name, tp.to_llm_str(), rel_name),
                (tp, [rel_name, col_name])
            )
        ));

        let map_key = self.get_chosen_value("column", column_options_map.keys().into_iter().collect());

        let (tp, [rel_name, col_name]) = column_options_map.get(map_key).unwrap();

        ((**tp).clone(), [(**rel_name).clone(), (**col_name).clone()])
    }

    fn reset(&mut self) { }
}
