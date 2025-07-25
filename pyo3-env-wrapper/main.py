import gymnasium as gym
from gymnasium import spaces
import numpy as np
from pyo3_env_wrapper.pyo3_env_wrapper import (
    ConstraintMeetingEnvironmentWrapper,
)

def to_bool_numpy(l):
    return np.array(l, dtype=np.bool_)

class ConstraintMeetingEnvironment(gym.Env):
    def __init__(
            self,
            config_path: str,
            max_steps: int = 10000,
        ):
        super().__init__()

        self.max_steps = max_steps
        self.env = ConstraintMeetingEnvironmentWrapper(config_path)

        self.previous_mask, _query_str_opt = self.env.reset()
        self.observation_space = spaces.MultiBinary(len(self.previous_mask))
        self.action_space = spaces.Discrete(len(self.previous_mask))

    def reset(self):
        previous_mask_or_none, query_str_opt = self.env.reset()
        if previous_mask_or_none is not None:
            self.previous_mask = previous_mask_or_none
        info = {}
        if query_str_opt is not None:
            info['query_str'] = query_str_opt
        self.current_step = 0
        return to_bool_numpy(self.previous_mask), info

    def step(self, action):
        terminated = False
        info = {}

        if not self.previous_mask[action]:
            reward = -1.0
            terminated, truncated = True, False
            info['error'] = 'Invalid action: selected index is False in mask'
            return to_bool_numpy(self.previous_mask), reward, terminated, truncated, info

        self.previous_mask, anticall_reward, terminated = self.env.step(action)
        reward = 1.0 + anticall_reward
        self.current_step += 1
        truncated = self.current_step >= self.max_steps
        if truncated:  # terminate generation
            self.env.step(None)

        return to_bool_numpy(self.previous_mask), reward, terminated, truncated, info

    def pop_query(self):
        query_str_opt = self.env.pop_query()
        return query_str_opt


def main():
    env = ConstraintMeetingEnvironment("/Users/mila/research/mdp_query_gen/mdp_query_generator/configs/rl_envoronment.toml")
    while True:
        obs, info = env.reset()
        done = False
        while not done:
            valid_actions = np.flatnonzero(obs)
            action = np.random.choice(valid_actions)
            obs, reward, terminated, truncated, info = env.step(action)
            done = terminated or truncated
            print(f"Step: {env.current_step}, Action: {action}, Reward: {reward}, Done: {done}, Info: {info}")
        if terminated:
            break  # terminate if a query was generated successfully
    query_str_opt = env.pop_query()
    print(f"Resulting query: {query_str_opt}")
    

if __name__ == "__main__":
    main()
