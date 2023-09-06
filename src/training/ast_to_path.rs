use std::{path::PathBuf, str::FromStr};

use smol_str::SmolStr;
use sqlparser::{parser::Parser, dialect::PostgreSqlDialect, ast::{Statement, Query}};

use crate::{query_creation::random_query_generator::query_info::DatabaseSchema, config::TomlReadable};

pub struct AstToPathConverter {
    query_asts: Vec<Box<Query>>,
    database_schema: DatabaseSchema,
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

impl AstToPathConverter {
    pub fn with_config(config: TrainingConfig) -> Self {
        let db = std::fs::read_to_string(config.training_db_path).unwrap();
        AstToPathConverter {
            query_asts: Parser::parse_sql(&PostgreSqlDialect {}, &db).unwrap().into_iter()
                .filter_map(|statement| if let Statement::Query(query) = statement {
                    Some(query)
                } else { None })
                .collect(),
            database_schema: DatabaseSchema::parse_schema(&config.training_schema),
        }
    }

    pub fn get_paths(&self) -> Vec<Vec<SmolStr>> {
        println!("{}", self.database_schema);
        println!("{}", self.query_asts[0]);
        vec![]
    }
}
