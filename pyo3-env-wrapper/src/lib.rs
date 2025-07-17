use std::{path::PathBuf, str::FromStr};

use equivalence_testing::rl_env::environment::ConstraintMeetingEnvironment;
use pyo3::prelude::*;
use query_constraint_wrapper::{ConstraintMetricWrapper, ConstraintTypeWrapper, QueryConstraintWrapper};

pub mod query_constraint_wrapper;

/// A simple environment struct, storing some internal 'state'.
#[pyclass]
pub struct ConstraintMeetingEnvironmentWrapper {
    env: ConstraintMeetingEnvironment,
}

#[pymethods]
impl ConstraintMeetingEnvironmentWrapper {
    /// Constructor visible to Python.  e.g. `env = my_rust_env.ConstraintMeetingEnvironmentWrapper(0)`
    #[new]
    pub fn new(query_constraint: QueryConstraintWrapper, config_path: &str) -> Self {
        let inner_constraint = query_constraint.into_inner();
        ConstraintMeetingEnvironmentWrapper {
            env: ConstraintMeetingEnvironment::new(
                inner_constraint,
                PathBuf::from_str(config_path).unwrap()
            ),
        }
    }

    /// Reset the environment (Gym-style). Returns the initial observation.
    pub fn reset(&mut self) -> PyResult<Vec<i32>> {
        Ok(self.env.reset())
    }

    /// Step the environment. Returns `(observation, reward, done, info)`.
    /// In a real environment, `action` would affect `state` more interestingly.
    pub fn step(&mut self, action: i32) -> PyResult<(Vec<i32>, f32, bool, Option<String>)> {
        Ok(self.env.step(action))
    }
}

/// A Python module implemented in Rust.
#[pymodule]
fn pyo3_env_wrapper(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<ConstraintMeetingEnvironmentWrapper>()?;
    m.add_class::<ConstraintTypeWrapper>()?;
    m.add_class::<ConstraintMetricWrapper>()?;
    m.add_class::<QueryConstraintWrapper>()?;
    Ok(())
}
