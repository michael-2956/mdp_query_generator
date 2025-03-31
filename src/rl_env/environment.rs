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
    constraint: QueryConstraint,
}

impl ConstraintMeetingEnvironment {
    pub fn new(constraint: QueryConstraint) -> Self {
        Self {
            constraint
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

        (vec![action.clamp(0, 9)], 0f32, false, None)
    }
}
