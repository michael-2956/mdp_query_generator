import json
import torch
import psycopg2
import numpy as np
import torch.nn as nn
import gymnasium as gym
from psycopg2 import sql
import torch.optim as optim
from gymnasium import spaces
import torch.nn.functional as F
from torch.distributions import Categorical
from pyo3_env_wrapper.pyo3_env_wrapper import (
    QueryCreationEnvironmentWrapper,
)


def to_bool_numpy(l):
    return np.array(l, dtype=np.bool_)


def get_query_cardinality(query: str) -> int:
    conn = psycopg2.connect(
        host="localhost",
        user="mykhailo",
        dbname="tpch"
    )
    try:
        with conn.cursor() as cur:
            count_q = sql.SQL(
                "SELECT COUNT(*) FROM ({inner}) AS sub"
            ).format(inner=sql.SQL(query))
            cur.execute(count_q)
            (cardinality,) = cur.fetchone()
            return cardinality
    except:
        return None  # query has a mistake
    finally:
        conn.close()


def get_estimated_cardinality(query: str) -> float:
    conn = psycopg2.connect(
        host="localhost",
        user="mykhailo",
        dbname="tpch"
    )
    try:
        with conn.cursor() as cur:
            cur.execute(f"EXPLAIN (FORMAT JSON) {query}")
            plan = cur.fetchone()[0][0]
            return plan["Plan"]["Plan Rows"]
    finally:
        conn.close()


def relative_error(e_value, t_value):
    return abs(e_value-t_value) / t_value


def get_card_range_reward(
        query_str, low_b=20, up_b=30
    ):
    """
    Calcualtes the cardinality range error inbetween given bounds. \
    Error is in the [-5, 5] range. It is 5 when the constarints are satisfied, \
    a negative value otherwise, -5 if query has a runtime error. \
    """
    e_card = get_query_cardinality(query_str)
    if e_card is None:
        return -5  # run-time error.
    # e_card = get_estimated_cardinality(query_str)
    if low_b <= e_card <= up_b:
        return 5
    else:
        r_e = max(
            relative_error(e_card, up_b),
            relative_error(e_card, low_b)
        )
        reward = -r_e
        reward = max(reward, -5)
        return reward


class ConstraintMeetingEnvironment(gym.Env):
    def __init__(
            self,
            config_path: str,
            max_steps: int = 300,
        ):
        super().__init__()

        self.max_steps = max_steps
        self.env = QueryCreationEnvironmentWrapper(config_path)

        self.previous_mask, _query_str_opt = self.env.reset()
        self.action_space_size = len(self.previous_mask)
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

        truncated = self.current_step >= self.max_steps
        if truncated:  # truncate generation
            reward = -1.0
            self.previous_mask, _anticall_reward, terminated = self.env.step(None)
            info['error'] = 'Generation was truncated'
            return None, reward, terminated, truncated, info

        if not self.previous_mask[action]:
            reward = -1.0
            terminated, truncated = False, True
            info['error'] = 'Invalid action: selected index is False in mask'
            return None, reward, terminated, truncated, info

        self.previous_mask, _anticall_reward, terminated = self.env.step(action)
        reward = 0
        self.current_step += 1
        if terminated:
            query_str = self.pop_query()
            reward = 6 + get_card_range_reward(query_str)
            info['query_str'] = query_str

        return to_bool_numpy(self.previous_mask), reward, terminated, truncated, info

    def pop_query(self):
        query_str_opt = self.env.pop_query()
        return query_str_opt


class LSTMModel(nn.Module):
    def __init__(self, input_dim, output_dim, embedding_dim, hidden_dim):
        super().__init__()
        self.embedding = nn.Embedding(input_dim, embedding_dim)
        self.lstm_cell_1 = nn.LSTMCell(embedding_dim, hidden_dim)
        self.lstm_cell_2 = nn.LSTMCell(hidden_dim, hidden_dim)
        self.output_layer = nn.Linear(hidden_dim, output_dim)

        self.hidden_dim = hidden_dim
        self.output_dim = output_dim
        self.reset_states(batch_size=1)  # Default single batch

    def reset_states(self, batch_size):
        device = next(self.parameters()).device
        h0 = torch.zeros(batch_size, self.hidden_dim, device=device)
        c0 = torch.zeros(batch_size, self.hidden_dim, device=device)
        self.state1 = (h0.clone(), c0.clone())
        self.state2 = (h0.clone(), c0.clone())
        self.time_step = 0

    def forward(self, input_seq):
        """
        input_seq: (batch_size, seq_len)
          - sequence of token ids for every batch element
        """
        batch_size = input_seq.size(0)
        self.reset_states(batch_size)

        # shape: (batch_size, seq_len, E)
        embedded = self.embedding(input_seq)
        outputs = []

        for t in range(embedded.size(1)):
            x = embedded[:, t]  # (batch_size, E)
            h1, c1 = self.lstm_cell_1(x, self.state1)
            h2, c2 = self.lstm_cell_2(h1, self.state2)
            self.state1, self.state2 = (h1, c1), (h2, c2)

            logits = self.output_layer(h2)
            outputs.append(logits)
            self.time_step += 1

        return torch.stack(outputs, dim=1)  # shape: (batch_size, seq_len, output_dim)

    def step(self, input_t):
        """
        input_t: (batch_size,)
          - index of tokens at some time step for every batch element
        """
        embedded = self.embedding(input_t)  # shape: (batch_size, embedding_dim)
        h1, c1 = self.lstm_cell_1(embedded, self.state1)
        h2, c2 = self.lstm_cell_2(h1, self.state2)
        self.state1, self.state2 = (h1, c1), (h2, c2)
        logits = self.output_layer(h2)
        self.time_step += 1
        return logits


def train_actor_critic(env, num_episodes=int(1e6), gamma=0.99, entropy_coef=1, lr=1e-3):
    actor = LSTMModel(
        env.action_space_size + 1,
        env.action_space_size,
        embedding_dim=200,
        hidden_dim=30
    )
    critic = LSTMModel(
        env.action_space_size+1,
        output_dim=1,
        embedding_dim=200,
        hidden_dim=30
    )
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    actor, critic = actor.to(device), critic.to(device)

    optimizer = optim.Adam(
        list(actor.parameters()) + list(critic.parameters()), lr=lr
    )  # TODO: lr 0.001 for actor, 0.003 for critic
    
    n_completed = 0
    n_in_constraint = 0
    reward_history = []

    for ep in range(num_episodes):
        actor.reset_states(batch_size=1)
        critic.reset_states(batch_size=1)
        obs, info = env.reset()
        if 'query_str' in info:
            print(f"Query: {info['query_str']}")
        prev_action = torch.tensor([env.action_space_size], device=device)

        log_probs, values, rewards, entropies = [], [], [], []
        done = False
        while not done:
            logits = actor.step(prev_action)
            mask = torch.tensor(obs, dtype=torch.bool, device=device)
            logits = logits.masked_fill(~mask.unsqueeze(0), -1e9)
            dist = Categorical(F.softmax(logits, -1))
            action = dist.sample()

            log_probs.append(dist.log_prob(action))
            entropies.append(dist.entropy())

            # take step
            obs, reward, terminated, truncated, info = env.step(action.item())
            done = terminated or truncated
            prev_action = action

            # critic forward
            with torch.no_grad():
                v = critic.step(prev_action).squeeze(-1)
            values.append(v)
            rewards.append(torch.tensor([reward], device=device))

        # compute discounted returns
        returns = []
        discounted_r = torch.zeros(1, device=device)
        for reward in reversed(rewards):
            discounted_r = reward + gamma * discounted_r
            returns.append(discounted_r)
        returns = list(reversed(returns))
        returns = torch.cat(returns).detach()

        values = torch.stack(values)
        log_probs = torch.stack(log_probs)
        entropies = torch.stack(entropies)

        adv = returns - values

        # losses
        policy_loss = -(log_probs * adv.detach()).mean()
        value_loss = adv.pow(2).mean()
        e_coef = 0.0 if ep < 0 else entropy_coef
        loss = policy_loss + 0.5 * value_loss - e_coef * entropies.mean()

        optimizer.zero_grad()
        loss.backward()
        optimizer.step()

        total_r = sum(r.item() for r in rewards)
        if total_r > 0:
            n_completed += 1
        in_constraint = np.isclose(total_r, 11.)
        if in_constraint:
            n_in_constraint += 1
        reward_history.append(total_r)
        print(
            f"Ep {ep} | F {n_completed} | C {n_in_constraint} "
            f"| R={total_r:.4f} "
            f"| R_mean100={np.mean(reward_history[-100:]):.4f} "
            f"| S={env.current_step} "
            f"| L={loss.item():.4f} "
            f"| Ent={ (e_coef*entropies.mean()).item():.4f}",
            end='          \r'
        )
        
        if 'query_str' in info and in_constraint:
            print(f"\nQuery: {info['query_str']}")
    
    query_str_opt = env.pop_query()
    print(f"Last query: {query_str_opt}")

    print("Training finished.")


def main():
    env = ConstraintMeetingEnvironment("/Users/mila/research/mdp_query_gen/mdp_query_generator/configs/rl_envoronment.toml")
    train_actor_critic(env)
    # while True:
    #     obs, info = env.reset()
    #     if 'query_str' in info:
    #         print()
    #         print(f"Query: {info['query_str']}")
    #     done = False
    #     while not done:
    #         valid_actions = np.flatnonzero(obs)
    #         action = np.random.choice(valid_actions)
    #         obs, reward, terminated, truncated, info = env.step(action)
    #         done = terminated or truncated
    #         print(f"Step: {env.current_step}, Action: {action}, Reward: {reward}, Done: {done}, Info: {info}", end='        \r')
    #     if terminated:
    #         break  # terminate if a query was generated successfully
    # query_str_opt = env.pop_query()
    # print(f"Last query: {query_str_opt}")
    

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
