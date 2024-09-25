use std::{fs, path::PathBuf};

use postgres::{Client, NoTls};

use crate::{config::{Config, TomlReadable}, query_creation::{query_generator::{query_info::DatabaseSchema, QueryGenerator}, state_generator::{state_choosers::ProbabilisticStateChooser, substitute_models::AntiCallModel, MarkovChainGenerator}}};

use super::syntax_coverage::{create_database, drop_database_if_exists};

pub struct SemanticCorrectnessConfig {
    n_tests: usize,
    schema_path: PathBuf,
}

impl TomlReadable for SemanticCorrectnessConfig {
    fn from_toml(toml_config: &toml::Value) -> Self {
        let section = &toml_config["semantic_correctness"];
        Self {
            n_tests: section["n_tests"].as_integer().unwrap() as usize,
            schema_path: PathBuf::from(section["schema_path"].as_str().unwrap()),
        }
    }
}

pub fn test_semantic_correctness(config: Config) {
    let schema_path = config.semantic_correctness_config.schema_path;
    let schema_str = fs::read_to_string(schema_path).unwrap();
    let db = DatabaseSchema::parse_schema_string(schema_str);

    let mut path_query_generator = QueryGenerator::<ProbabilisticStateChooser>::from_state_generator_and_config_with_schema(
        MarkovChainGenerator::with_config(&config.chain_config).unwrap(),
        db.clone(),
        config.generator_config,
        Box::new(AntiCallModel::new(config.anticall_model_config.clone()))
    );

    let db_name = format!("query_test_user_semantic").to_lowercase();
    drop_database_if_exists(&db_name).unwrap();
    create_database(&db_name).unwrap();
    let conn_str = format!("host=localhost user=query_test_user dbname={}", db_name);
    let mut client = Client::connect(&conn_str, NoTls).unwrap();
    if let Err(err) = client.batch_execute(db.get_schema_string().as_str()) {
        eprintln!("Semantic testing failed!\nSchema creation error: {err}");
        client.close().unwrap();
        drop_database_if_exists(&db_name).unwrap();
        return
    }

    let mut n_ok = 0usize;
    let n_tests = config.semantic_correctness_config.n_tests;
    for i in 0..n_tests {
        if i % 10 == 0 {
            eprint!("Testing: {} / {} ({n_ok} OK)   \r", i+1, n_tests);
        }

        let query = path_query_generator.generate();

        match client.batch_execute(format!("{query};").as_str()) {
            Ok(_) => n_ok += 1,
            // Err(err) => {
            //     eprintln!("Query: {query}");
            //     eprintln!("Error: {err}");
            //     break;
            // },
            Err(_) => { }
        }
    }

    println!("\n\nTotal semantically correct: {n_ok}/{n_tests} ({:.2}%)", 100f64 * n_ok as f64 / n_tests as f64);
}
