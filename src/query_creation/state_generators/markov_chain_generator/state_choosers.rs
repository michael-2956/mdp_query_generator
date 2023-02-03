use core::fmt::Debug;
use rand::{Rng, SeedableRng};

use smol_str::SmolStr;
use sqlparser::ast::Query;
use rand_chacha::ChaCha8Rng;

use super::markov_chain::NodeParams;

pub trait StateChooser: Debug + Clone {
    fn new() -> Self where Self: Sized;
    fn choose_destination(&mut self, cur_node_outgoing: Vec<(bool, f64, NodeParams)>) -> Option<NodeParams>;
}

#[derive(Debug, Clone)]
pub struct ProbabilisticStateChooser {
    rng: ChaCha8Rng,
}

impl StateChooser for ProbabilisticStateChooser {
    fn new() -> Self {
        Self { rng: ChaCha8Rng::seed_from_u64(1), }
    }

    fn choose_destination(&mut self, cur_node_outgoing: Vec<(bool, f64, NodeParams)>) -> Option<NodeParams> {
        let cur_node_outgoing: Vec<(f64, NodeParams)> = {
            let cur_node_outgoing = cur_node_outgoing.iter().map(|el| {
                (if el.0 { 0f64 } else { el.1 }, el.2.clone())
            }).collect::<Vec<_>>();
            let max_level: f64 = cur_node_outgoing.iter().map(|el| { el.0 }).sum();
            cur_node_outgoing.into_iter().map(|el| { (el.0 / max_level, el.1) }).collect()
        };
        let level: f64 = self.rng.gen::<f64>();
        let mut cumulative_prob = 0f64;
        let mut destination = Option::<NodeParams>::None;
        for (prob, dest) in cur_node_outgoing {
            cumulative_prob += prob;
            if level < cumulative_prob {
                destination = Some(dest);
                break;
            }
        }
        destination
    }
}

#[derive(Debug, Clone)]
pub struct DeterministicStateChooser {
    state_list: Vec<SmolStr>,
    state_index: usize,
}

impl DeterministicStateChooser {
    fn _from_query(_query: Query) -> Self where Self: Sized {
        // TODO: Initialise state list from query
        Self {
            state_list: vec![],
            state_index: 0
        }
    }
}

impl StateChooser for DeterministicStateChooser {
    fn new() -> Self where Self: Sized {
        Self {
            state_list: vec![],
            state_index: 0
        }
    }

    fn choose_destination(&mut self, cur_node_outgoing: Vec<(bool, f64, NodeParams)>) -> Option<NodeParams> {
        if cur_node_outgoing.len() == 0 {
            println!("List of outgoing nodes is empty!");
            return None
        }
        if cur_node_outgoing.len() == 1 { return Some(cur_node_outgoing[0].2.clone()) }
        let node_name = self.state_list[self.state_index].clone();
        self.state_index += 1;
        match cur_node_outgoing.iter().find(|node| node.2.name == node_name) {
            Some(node) => Some(node.2.clone()),
            None => {
                println!("None of {:?} matches \"{node_name}\" inferred from query", cur_node_outgoing);
                None
            },
        }
    }
}