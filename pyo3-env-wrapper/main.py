import gym
import numpy as np
from gym import spaces
from pyo3_env_wrapper.pyo3_env_wrapper import (
    ConstraintMeetingEnvironmentWrapper,
    QueryConstraintWrapper,
    ConstraintTypeWrapper,
    ConstraintMetricWrapper
)

class ConstraintMeetingEnvironment(gym.Env):
    def __init__(self, constraint: QueryConstraintWrapper):
        super().__init__()
        # Single integer observation
        self.observation_space = spaces.Box(low=-999, high=999, shape=(1,), dtype=np.int32)
        # 0..9
        self.action_space = spaces.Discrete(10)
        self.env = ConstraintMeetingEnvironmentWrapper(constraint)

    def reset(self):
        obs = self.env.reset()  # returns a Python list
        return np.array(obs, dtype=np.int32)

    def step(self, action):
        obs, reward, done, info = self.env.step(action)
        obs = np.array(obs, dtype=np.int32)
        return obs, reward, done, info if info else {}

def main():
    env = ConstraintMeetingEnvironment(QueryConstraintWrapper(
        ConstraintTypeWrapper.point(3.14),
        ConstraintMetricWrapper.cost()
    ))
    print(f"{env.reset() = }")
    for action in range(-10, 11):
        print(f"env.step({action}) = {env.step(action)}")

if __name__ == "__main__":
    main()
