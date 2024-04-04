use super::PathwayGraphModel;

pub struct ChatGPTPromptingModel {

}

impl PathwayGraphModel for ChatGPTPromptingModel {
    fn process_state(&mut self, call_stack: &Vec<crate::query_creation::state_generator::markov_chain_generator::StackFrame>, popped_stack_frame: Option<&crate::query_creation::state_generator::markov_chain_generator::StackFrame>) {
        todo!()
    }

    fn write_weights(&self, file_path: &std::path::PathBuf) -> std::io::Result<()> {
        panic!("ChatGPT prompting model does not have the ability to write weights")
    }

    /// This loads the prompts from the json file
    fn load_weights(&mut self, file_path: &std::path::PathBuf) -> std::io::Result<()> {
        todo!()
    }

    fn predict(&mut self, call_stack: &Vec<crate::query_creation::state_generator::markov_chain_generator::StackFrame>, node_outgoing: Vec<crate::query_creation::state_generator::markov_chain_generator::markov_chain::NodeParams>) -> super::ModelPredictionResult {
        todo!()
    }
}

impl ChatGPTPromptingModel {
    pub fn new() -> Self {
        Self {

        }
    }
}
