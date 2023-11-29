use std::collections::HashMap;

use smol_str::SmolStr;

use crate::query_creation::state_generator::markov_chain_generator::markov_chain::MarkovChain;

use super::ast_to_path::PathNode;

pub struct MarkovWeights {
    /// function name -> from -> [(to, weight), ...]
    pub weights: HashMap<SmolStr, HashMap<SmolStr, HashMap<SmolStr, f64>>>,
}

impl MarkovWeights {
    pub fn new(markov_chain: &MarkovChain) -> Self {
        let mut _self = Self {
            weights: HashMap::new(),
        };
        _self.initialize_keys(markov_chain);
        _self
    }

    /// initialize all of tha map keys using the
    /// parsed MarkovChain class
    fn initialize_keys(&mut self, markov_chain: &MarkovChain) {
        for (func_name, function) in markov_chain.functions.iter() {
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
            for (_, out) in chain.iter_mut() {
                for (_, weight) in out {
                    *weight = 0f64;
                }
            }
        }
    }

    /// Fills all the weights with uniform probabilities
    pub fn fill_probs_uniform(&mut self) {
        for (_, chain) in self.weights.iter_mut() {
            for (_, out) in chain.iter_mut() {
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
            for (_, out) in chain.iter_mut() {
                let prob_sum: f64 = out.into_iter().map(|(_, p)| *p).sum();
                for (_, weight) in out {
                    *weight = *weight / prob_sum;
                }
            }
        }
    }

    // pub fn add_edge(&mut self, ) {

    // }

    /// adds 1 to every edge which the path has
    /// used, for every visit
    pub fn add_path(&mut self, path: &Vec<PathNode>) {
        let (mut f, mut t) = (None, None);
        let mut current_subgraph = None;
        for node in path {
            match node {
                // new state. Add 1 to the edge
                /// TODO: check for exit node, track current_subgraph stack
                /// NOTE:
                /// - actually, use a markov chain generator?
                /// - Obtain it by reference from trainer?
                PathNode::State(state_name) => {
                    f = t.take();
                    t = Some(state_name.clone());
                },
                // new function
                // the call node and funciton node are not connected
                PathNode::NewFunction(first_state) => {
                    current_subgraph = Some(first_state.clone());
                    f = None;
                    t = Some(first_state.clone());
                },
                // ignore these
                PathNode::SelectedTableName(_) |
                PathNode::SelectedColumnNameFROM(_) |
                PathNode::SelectedColumnNameGROUPBY(_) |
                PathNode::NumericValue(_) |
                PathNode::IntegerValue(_) |
                PathNode::QualifiedWildcardSelectedRelation(_) => {},
            }
        }
        if let Some(ref subgraph_name) = current_subgraph {
            if let Some(ref from_st) = f {
                if let Some(ref to_st) = t {
                    if let Some(subgraph_weights) = self.weights.get_mut(subgraph_name) {
                        if let Some(outgoing) = subgraph_weights.get_mut(from_st) {
    
                        }
                    } else {
                        panic!("Error obtaining weights: subgraph {subgraph_name} not found.");
                    }
                }
            }
        } else if f.is_some() || t.is_some() {
            panic!(
                "Error obtaining weights: current_subgraph = {:?}, but f = {:?} and t = {:?}",
                current_subgraph, f, t
            );
        }
    }
}
