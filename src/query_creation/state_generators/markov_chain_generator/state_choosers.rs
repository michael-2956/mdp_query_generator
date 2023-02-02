use rand::{Rng, SeedableRng};

use core::fmt::Debug;
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

// pub struct DeterministicStateChooser