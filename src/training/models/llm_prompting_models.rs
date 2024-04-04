use std::{env, path::PathBuf};

use crate::query_creation::state_generator::markov_chain_generator::{markov_chain::NodeParams, StackFrame};

use self::llm_prompts::LLMPrompts;

use super::{ModelPredictionResult, PathwayGraphModel};

use chatgpt::{client::ChatGPT, config::{ChatGPTEngine, ModelConfiguration}, converse::Conversation};
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

    fn predict(&mut self, call_stack: &Vec<StackFrame>, node_outgoing: Vec<NodeParams>) -> ModelPredictionResult {
        if let &[ref main_func] = call_stack.as_slice() {
            if main_func.function_context.current_node.node_common.name == "Query" {
                self.initiate_conversation();
            }
        }

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
