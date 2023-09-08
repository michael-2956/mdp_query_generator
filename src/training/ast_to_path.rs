use std::{path::PathBuf, str::FromStr, error::Error, fmt};

use rand::SeedableRng;
use rand_chacha::ChaCha8Rng;
use smol_str::SmolStr;
use sqlparser::{parser::Parser, dialect::PostgreSqlDialect, ast::{Statement, Query, ObjectName, Expr}};

use crate::{query_creation::{random_query_generator::query_info::{DatabaseSchema, ClauseContext}, state_generators::{subgraph_type::SubgraphType, state_choosers::MaxProbStateChooser, MarkovChainGenerator, markov_chain_generator::{StateGeneratorConfig, error::SyntaxError, markov_chain::CallModifiers}, dynamic_models::{DeterministicModel, DynamicModel}}}, config::TomlReadable};

pub struct SQLTrainer {
    query_asts: Vec<Box<Query>>,
    path_generator: PathGenerator,
}

pub struct TrainingConfig {
    pub training_db_path: PathBuf,
    pub training_schema: PathBuf,
}

impl TomlReadable for TrainingConfig {
    fn from_toml(toml_config: &toml::Value) -> Self {
        let section = &toml_config["training"];
        Self {
            training_db_path: PathBuf::from_str(section["training_db_path"].as_str().unwrap()).unwrap(),
            training_schema: PathBuf::from_str(section["training_schema"].as_str().unwrap()).unwrap(),
        }
    }
}

impl SQLTrainer {
    pub fn with_config(config: TrainingConfig, chain_config: &StateGeneratorConfig) -> Result<Self, SyntaxError> {
        let db = std::fs::read_to_string(config.training_db_path).unwrap();
        Ok(SQLTrainer {
            query_asts: Parser::parse_sql(&PostgreSqlDialect {}, &db).unwrap().into_iter()
                .filter_map(|statement| if let Statement::Query(query) = statement {
                    Some(query)
                } else { None })
                .collect(),
            path_generator: PathGenerator::new(
                DatabaseSchema::parse_schema(&config.training_schema),
                chain_config,
            )?,
        })
    }

    pub fn train(&mut self) -> Result<Vec<Vec<SmolStr>>, ConvertionError> {
        let mut paths = Vec::<_>::new();
        for query in self.query_asts.iter() {
            println!("Converting query {}", query);
            paths.push(self.path_generator.get_query_path(query)?);
        }
        Ok(paths)
    }
}

#[derive(Debug)]
pub struct ConvertionError {
    reason: String,
}

impl Error for ConvertionError { }

impl fmt::Display for ConvertionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Error: {}", self.reason)
    }
}

impl ConvertionError {
    fn new(reason: String) -> Self {
        Self { reason }
    }
}

struct PathGenerator {
    current_path: Vec<SmolStr>,
    database_schema: DatabaseSchema,
    state_generator: MarkovChainGenerator<MaxProbStateChooser>,
    state_selector: DeterministicModel,
    clause_context: ClauseContext,
    rng: ChaCha8Rng,
}

impl PathGenerator {
    fn new(database_schema: DatabaseSchema, chain_config: &StateGeneratorConfig) -> Result<Self, SyntaxError> {
        Ok(Self {
            current_path: vec![],
            database_schema,
            state_generator: MarkovChainGenerator::<MaxProbStateChooser>::with_config(chain_config)?,
            state_selector: DeterministicModel::new(),
            clause_context: ClauseContext::new(),
            rng: ChaCha8Rng::seed_from_u64(1),
        })
    }

    fn get_query_path(&mut self, query: &Box<Query>) -> Result<Vec<SmolStr>, ConvertionError> {
        self.handle_query(query)?;
        Ok(std::mem::replace(&mut self.current_path, vec![]))
    }
}

impl PathGenerator {
    fn expect_compat(&self, target: &SubgraphType, compat_with: &SubgraphType) {
        if !target.is_compat_with(compat_with) {
            self.state_generator.print_stack();
            panic!("Incompatible types: expected compatible with {:?}, got {:?}", compat_with, target);
        }
    }

    fn push_state(&mut self, state: &str) {
        let state = SmolStr::new(state);
        self.state_selector.push_state(state.clone());
        self.state_generator.next(&mut self.rng, &self.clause_context, &mut self.state_selector);
        self.current_path.push(state);
    }

    fn push_states(&mut self, state_names: &[&str]) {
        for state_name in state_names {
            self.push_state(state_name);
        }
    }

    /// subgraph def_Query
    fn handle_query(&mut self, query: &Box<Query>) -> Result<Vec<(Option<ObjectName>, SubgraphType)>, ConvertionError> {
        self.clause_context.on_query_begin();
        self.push_state("Query");

        match self.state_generator.get_fn_modifiers() {
            CallModifiers::StaticList(list) if list.contains(&SmolStr::new("single row")) => {
                self.push_state("single_value_true");
            }
            _ => {
                self.push_state("single_value_false");
                if let Some(ref limit) = query.limit {
                    self.push_states(&["limit", "call52_types"]);
                    self.handle_types(limit, Some(&[SubgraphType::Numeric]), None)?;
                }
            }
        }
        self.push_state("FROM");

        return Err(ConvertionError::new(format!("subgraph def_query not yet implemented")))
    }

    /// subgraph def_types
    fn handle_types(
        &mut self,
        expr: &Expr,
        check_generated_by_one_of: Option<&[SubgraphType]>,
        check_compatible_with: Option<SubgraphType>,
    ) -> Result<SubgraphType, ConvertionError> {
        let selected_type = SubgraphType::Undetermined;

        // TODO: this code is repeated here and in random query generator.
        // NOTE: Will be solved after out_types are implemented, so that all of this is managed
        // in the state generator
        if let Some(generators) = check_generated_by_one_of {
            if !generators.iter().any(|as_what| selected_type.is_same_or_more_determined_or_undetermined(&as_what)) {
                self.state_generator.print_stack();
                panic!("Unexpected type: expected one of {:?}, got {:?}", generators, selected_type);
            }
        }
        if let Some(with) = check_compatible_with {
            self.expect_compat(&selected_type, &with);
        }

        return Err(ConvertionError::new(format!("subgraph def_types not yet implemented")))
    }
}
