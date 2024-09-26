use std::{collections::HashMap, fs, path::PathBuf};

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

    let ignored_errs = [
        "db error: ERROR: division by zero",
        "db error: ERROR: a negative number raised to a non-integer power yields a complex result",
        "db error: ERROR: negative substring length not allowed",
        "db error: ERROR: LIMIT must not be negative",
    ];

    let mut n_ignored = 0usize;
    let mut n_ok: usize = 0usize;
    let mut errs = vec![];
    let n_tests = config.semantic_correctness_config.n_tests;
    for i in 0..n_tests {
        if i % 100 == 0 {
            eprint!("Testing: {} / {} ({} OK)   \r", i, n_tests, n_ok + n_ignored);
        }

        let query = path_query_generator.generate();

        match client.batch_execute(format!("{query};").as_str()) {
            Ok(_) => n_ok += 1,
            Err(err) => {
                let err_str = format!("{err}");
                if ignored_errs.contains(&(err_str.as_str())) {
                    n_ignored += 1;
                } else {
                    errs.push(err_str);
                    // eprintln!("Query: {query}");
                    // eprintln!("Error: {err}");
                    // break;
                }
            }
        }
    }

    println!("\n\nTotal semantically correct: {n_ok}/{n_tests} ({:.2}%)", 100f64 * n_ok as f64 / n_tests as f64);
    println!("\n\nWithout ignored errors: {n_ok}/{} ({:.2}%)", n_tests - n_ignored, 100f64 * n_ok as f64 / (n_tests - n_ignored) as f64);

    let errs = errs.into_iter().fold(HashMap::new(), |mut map, s| {
        *map.entry(s).or_insert(0) += 1;
        map
    });
    println!("\n\n{:#?}", errs);
}
