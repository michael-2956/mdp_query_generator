use std::{path::PathBuf, str::FromStr};

use sqlparser::{parser::Parser, dialect::PostgreSqlDialect, ast::{Statement, Query}};

use crate::{config::TomlReadable, query_creation::{state_generators::markov_chain_generator::{StateGeneratorConfig, error::SyntaxError}, query_generator::query_info::DatabaseSchema}};

use super::ast_to_path::{PathNode, ConvertionError, PathGenerator};

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

    pub fn train(&mut self) -> Result<Vec<Vec<PathNode>>, ConvertionError> {
        let mut paths = Vec::<_>::new();
        for query in self.query_asts.iter() {
            // println!("Converting query {}", query);
            paths.push(self.path_generator.get_query_path(query)?);
            // println!("Path: {:?}\n", paths.last().unwrap());
        }
        Ok(paths)
    }
}
