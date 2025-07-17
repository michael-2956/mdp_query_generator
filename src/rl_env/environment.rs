use std::path::PathBuf;

use crate::{config::Config, query_creation::{query_generator::QueryGenerator, state_generator::{state_choosers::MaxProbStateChooser, substitute_models::DeterministicModel, MarkovChainGenerator}}};

#[derive(Debug, Clone)]
pub enum ConstraintType {
    Point(f64),
    Range {
        from: f64,
        to: f64
    }
}

#[derive(Debug, Clone)]
pub enum ConstraintMetric {
    Cost,
    Cardinality
}

pub struct QueryConstraint {
    c_type: ConstraintType,
    metric: ConstraintMetric,
}

impl QueryConstraint {
    pub fn new(c_type: ConstraintType, metric: ConstraintMetric) -> Self {
        Self {
            c_type,
            metric,
        }
    }
}

pub struct ConstraintMeetingEnvironment {
    config: Config,
    constraint: QueryConstraint,
    query_generator: QueryGenerator<MaxProbStateChooser>,
}

impl ConstraintMeetingEnvironment {
    pub fn new(constraint: QueryConstraint, config_path: PathBuf) -> Self {
        let config = Config::read_config(&config_path).unwrap();
        Self {
            constraint,
            query_generator: QueryGenerator::from_state_generator_and_config(
                MarkovChainGenerator::with_config(&config.chain_config).unwrap(),
                config.generator_config.clone(),
                Box::new(DeterministicModel::empty())
            ),
            config,
        }
    }

    pub fn reset(&mut self) -> Vec<i32> {

        // here, if
        // self.observation_space = spaces.Box(low=-999, high=999, shape=(1,), dtype=np.int32)
        // then
        // vec![self.state]

        vec![0]
    }

    /// Step the environment. Returns `(observation, reward, done, info)`.
    pub fn step(&mut self, action: i32) -> (Vec<i32>, f32, bool, Option<String>) {

        // here, if
        // self.action_space = spaces.Discrete(10)  # e.g. 0..9
        // then
        // (vec![self.state], reward, done, info)

        // TODO:
        // - basically we need to have the action be an integer index out
        //   of the available actions
        // - now, the available actions, when we talk about selecting values,
        //   they should be sampled as in learnedsqlgen. a valueselector is
        //   initiated with the sampled values. thus the number of states is
        //   fixed.
        // - the set of states is formed in new(). the states are the graph states
        //   plus the sampled set. a dict associated strings with indexes. 
        // - every state is a list of indexes of states available. NN selects
        //   states via masking and passes an appropriate action.
        // - even if there is only a single decision it is still added to the decision
        //   sequence.

        (vec![action.clamp(0, 9)], 0f32, false, None)
    }
}
