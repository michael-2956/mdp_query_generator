use std::fs::File;
use std::collections::HashMap;
use std::io::{self, Write, Read};

use smol_str::SmolStr;
use serde::{Deserialize, Serialize};

use crate::query_creation::state_generator::markov_chain_generator::markov_chain::Function;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarkovWeights {
    /// function name -> from -> [(to, weight), ...]
    pub weights: HashMap<SmolStr, HashMap<SmolStr, HashMap<SmolStr, f64>>>,
}

impl MarkovWeights {
    pub fn new(chain_functions: &HashMap<SmolStr, Function>) -> Self {
        let mut _self = Self {
            weights: HashMap::new(),
        };
        _self.initialize_keys(chain_functions);
        _self
    }

    /// initialize all of tha map keys using the
    /// parsed MarkovChain class
    fn initialize_keys(&mut self, chain_functions: &HashMap<SmolStr, Function>) {
        for (func_name, function) in chain_functions.iter() {
            let func_weights = self.weights
                .entry(func_name.clone())
                .or_insert(HashMap::new());
            for (from_node_name, to_vec) in function.chain.iter() {
                let w_outgoing = func_weights
                    .entry(from_node_name.clone())
                    .or_insert(HashMap::new());
                for (_, to_node) in to_vec.iter() {
                    w_outgoing.insert(to_node.node_common.name.clone(), 0f64);
                }
            }
        }
    }

    /// Fills all the weights with zeroes (useful before accumilation)
    pub fn fill_probs_zero(&mut self) {
        for (_, chain) in self.weights.iter_mut() {
            for (_, out) in chain {
                for (_, weight) in out {
                    *weight = 0f64;
                }
            }
        }
    }

    /// Fills all the weights with uniform probabilities
    pub fn fill_probs_uniform(&mut self) {
        for (_, chain) in self.weights.iter_mut() {
            for (_, out) in chain {
                let fill_with = 1f64 / (out.len() as f64);
                for (_, weight) in out {
                    *weight = fill_with;
                }
            }
        }
    }

    /// Makes all the weight sums equal to one,
    /// which is nessesary for them to be a probability distribution
    pub fn normalize(&mut self) {
        for (_, chain) in self.weights.iter_mut() {
            for (_, out) in chain {
                let prob_sum: f64 = out.into_iter().map(|(_, p)| *p).sum();
                for (_, weight) in out {
                    *weight = *weight / prob_sum;
                }
            }
        }
    }

    /// adds 1 to the specified edge
    pub fn add_edge(&mut self, func_name: &SmolStr, from: &SmolStr, to: &SmolStr) {
        let assign_to = self.weights
            .get_mut(func_name).unwrap_or_else(|| panic!("Error obtaining weights: subgraph {func_name} not found."))
            .get_mut(from).unwrap_or_else(|| panic!("Error obtaining weights: node {from} in subgraph {func_name} not found."))
            .get_mut(to).unwrap_or_else(|| panic!("Error obtaining weights: node {to} is not outgoing from node {from} in subgraph {func_name}."));
        *assign_to += 1f64;
    }

    pub fn write_to_file(&self, file_path: &str) -> io::Result<()> {
        let encoded: Vec<u8> = bincode::serialize(&self).unwrap();
        let mut file = File::create(file_path)?;
        file.write_all(&encoded)?;
        Ok(())
    }

    pub fn read_from_file(file_path: &str) -> io::Result<Self> {
        let mut file = File::open(file_path)?;
        let mut encoded = Vec::new();
        file.read_to_end(&mut encoded)?;
        let decoded: MarkovWeights = bincode::deserialize(&encoded[..]).unwrap();
        Ok(decoded)
    }

    pub fn print_outgoing_weights(&self, func_name: &SmolStr, from: &SmolStr) {
        for (to, weight) in self.weights
            .get(func_name).unwrap()
            .get(from).unwrap()
            .iter()
        {
            println!("{to} -> {weight}");
        }
    }

    pub fn print_function_weights(&self, func_name: &SmolStr) {
        for (from, out) in self.weights
            .get(func_name).unwrap()
        {
            println!("{from}: ");
            for (to, weight) in out {
                println!("    {weight} -> {to}");
            }
        }
    }

    pub fn print(&self) {
        for (func_name, chain) in self.weights.iter() {
            println!("\n=====================================================\n{func_name}: ");
            for (from, out) in chain {
                println!("    {from}: ");
                for (to, weight) in out {
                    println!("        {weight} -> {to}");
                }
            }
        }
    }
}
