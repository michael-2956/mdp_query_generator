use core::fmt::Debug;
use logaddexp::LogSumExp;
use rand::Rng;

use rand_chacha::ChaCha8Rng;

// use super::super::super::random_query_generator::TypesSelectedType;
use super::markov_chain::NodeParams;

pub trait StateChooser: Debug + Clone {
    fn new() -> Self where Self: Sized;
    /// choose destination adhering to the unnormalized log-probabilities
    fn choose_destination(&mut self, rng: &mut ChaCha8Rng, outgoing_states: Vec<(f64, NodeParams)>) -> Option<NodeParams>;
}

#[derive(Debug, Clone)]
pub struct ProbabilisticStateChooser { }

impl StateChooser for ProbabilisticStateChooser {
    fn new() -> Self {
        Self { }
    }

    fn choose_destination(&mut self, rng: &mut ChaCha8Rng, outgoing_states: Vec<(f64, NodeParams)>) -> Option<NodeParams> {
        let cur_node_outgoing: Vec<(f64, NodeParams)> = {
            // Balance the weights so they make up a probability distribution
            let w_sum: f64 = outgoing_states.iter().map(|el| { el.0 }).ln_sum_exp();
            outgoing_states.into_iter().map(|el| { (f64::exp(el.0 - w_sum), el.1) }).collect()
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

    fn choose_destination(&mut self, _rng: &mut ChaCha8Rng, outgoing_states: Vec<(f64, NodeParams)>) -> Option<NodeParams> {
        outgoing_states.iter()
            .max_by(|(x_w, _), (y_w, _)|
                x_w.partial_cmp(y_w).unwrap()
            ).map(|(_, node)| node.clone())
    }
}