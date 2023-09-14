use core::fmt::Debug;
use logaddexp::LogSumExp;
use rand::Rng;

use rand_chacha::ChaCha8Rng;

// use super::super::super::random_query_generator::TypesSelectedType;
use super::markov_chain::NodeParams;

pub trait StateChooser: Debug + Clone {
    fn new() -> Self where Self: Sized;
    fn choose_destination(&mut self, rng: &mut ChaCha8Rng, outgoing_states: Vec<(bool, f64, NodeParams)>) -> Option<NodeParams>;
}

#[derive(Debug, Clone)]
pub struct ProbabilisticStateChooser { }

impl StateChooser for ProbabilisticStateChooser {
    fn new() -> Self {
        Self { }
    }

    fn choose_destination(&mut self, rng: &mut ChaCha8Rng, outgoing_states: Vec<(bool, f64, NodeParams)>) -> Option<NodeParams> {
        let cur_node_outgoing: Vec<(f64, NodeParams)> = {
            let cur_node_outgoing = outgoing_states.iter().map(|el| {
                (if el.0 { f64::NEG_INFINITY } else { el.1 }, el.2.clone())
            }).collect::<Vec<_>>();
            // Rebalancing on the current level only as a temporary work-around
            let max_level: f64 = cur_node_outgoing.iter().map(|el| { el.0 }).ln_sum_exp();
            cur_node_outgoing.into_iter().map(|el| { (f64::exp(el.0 - max_level), el.1) }).collect()
        };
        let level: f64 = rng.gen::<f64>();
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
pub struct MaxProbStateChooser { }

impl StateChooser for MaxProbStateChooser {
    fn new() -> Self where Self: Sized {
        Self { }
    }

    fn choose_destination(&mut self, _rng: &mut ChaCha8Rng, outgoing_states: Vec<(bool, f64, NodeParams)>) -> Option<NodeParams> {
        outgoing_states.iter()
            .max_by(|x, y|
                x.1.partial_cmp(&y.1).unwrap()
            ).map(|(.., node)| node.clone())
    }
}