use std::{env, path::PathBuf};

use crate::query_creation::state_generator::markov_chain_generator::{markov_chain::NodeParams, StackFrame};

use self::llm_prompts::LLMPrompts;

use super::{ModelPredictionResult, PathwayGraphModel};

use chatgpt::{client::ChatGPT, config::{ChatGPTEngine, ModelConfiguration}, converse::Conversation};
use sqlparser::ast::Query;
use tokio::runtime::{self, Runtime};

mod llm_prompts;

pub struct ChatGPTPromptingModel {
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

    /// This loads the prompts from the json file
    fn load_weights(&mut self, file_path: &PathBuf) -> std::io::Result<()> {
        self.prompts = Some(LLMPrompts::read_from_file(file_path)?);
        Ok(())
    }

    fn predict(&mut self, call_stack: &Vec<StackFrame>, node_outgoing: Vec<NodeParams>, current_query_ast_opt: Option<&Query>) -> ModelPredictionResult {
        if let &[ref main_func] = call_stack.as_slice() {
            if main_func.function_context.current_node.node_common.name == "Query" {
                self.initiate_conversation();
            }
        }

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
        // if current_query_str.contains("[?]") {
        //     eprintln!("{current_query_str}");
        //     let outgoing_str = node_outgoing.iter().map(|node| format!("{} ", node.node_common.name)).collect::<String>();
        //     eprintln!("{outgoing_str}\n");
        // }
        eprintln!("{current_query_str}");
        let outgoing_str = node_outgoing.iter().map(|node| format!("{} ", node.node_common.name)).collect::<String>();
        eprintln!("{outgoing_str}\n");
        

        /// TODO impl QueryValueChooser for ChatGPTPromptingModel
        // every method prompts chatgpt instead
        // every chooser has its own prompt in config
        // to use the model as a chooser
        //
        // then instead of calling generator.value_chooser.choose_...()
        // we call generator.value_chooser().choose_...()
        //
        // value_chooser() is a method that has something like:
        // if let Some(vc) = self.predictor_model.as_any().downcast_mut::<QueryValueChooser>() {
        //     vc
        // } else { self.value_chooser.as_mut().unwrap() }

        ModelPredictionResult::None(node_outgoing)
    }
}

impl ChatGPTPromptingModel {
    pub fn new() -> Self {
        let mut model_config = ModelConfiguration::default();
        model_config.engine = ChatGPTEngine::Custom("gpt-3.5-turbo-0125");
        let mut _self = Self {
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
        self.async_runtime.block_on(async {
            self.current_conversation = Some(self.client.new_conversation_directed(
                self.prompts.as_ref().unwrap().system_prompt.clone()
            ))
        })
    }
}
