mod dot_parser;
use std::{path::PathBuf, fs::File, io::Read};

use self::dot_parser::{DotTokenizer, SyntaxError};

struct MarkovChain {

}

impl MarkovChain {
    fn parse_dot(source_path: PathBuf) -> Result<Self, SyntaxError> {
        let mut file = File::open(source_path).unwrap();
        let mut source = String::new();
        file.read_to_string(&mut source).unwrap();
        for token in DotTokenizer::from_str(&source) {
            println!("{:#?}", token?);
        }
        Ok(MarkovChain {
        })
    }
}

pub struct MarkovChainGenerator {
    markov_chain: MarkovChain
}

impl MarkovChainGenerator {
    pub fn parse_graph_from_file(source_path: PathBuf) -> Result<Self, SyntaxError> {
        Ok(MarkovChainGenerator {
            markov_chain: MarkovChain::parse_dot(source_path)?
        })
    }
}
