use std::{collections::BTreeMap, fs, path::PathBuf};

use postgres::{Client, NoTls};

use crate::{config::{Config, TomlReadable}, query_creation::{query_generator::{query_info::DatabaseSchema, QueryGenerator}, state_generator::{state_choosers::ProbabilisticStateChooser, substitute_models::AntiCallModel, MarkovChainGenerator}}};

use super::syntax_coverage::{create_database, drop_database_if_exists};

pub struct SemanticCorrectnessConfig {
    pub n_tests: usize,
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
        MarkovChainGenerator::with_config(&config.chain_config).unwrap_or_else(|err| { panic!("{err}") }),
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
        "db error: ERROR: a negative number raised to a non-integer power yields a complex result",
        "db error: ERROR: too many grouping sets present (maximum 4096)",
        "db error: ERROR: zero raised to a negative power is undefined",
        "db error: ERROR: negative substring length not allowed",
        "db error: ERROR: LIMIT must not be negative",
        "db error: ERROR: division by zero",
    ];

    let mut n_ignored = 0usize;
    let mut n_ok: usize = 0usize;
    let mut errs = BTreeMap::<String, Vec<String>>::new();
    let mut ignored_err_count = BTreeMap::<String, usize>::new();
    let n_tests = config.semantic_correctness_config.n_tests;
    let print_interval = f64::min((n_tests as f64)/100f64, 100f64) as usize;
    for i in 0..n_tests {
        if i % print_interval == 0usize {
            eprint!("Testing: {} / {} ({} OK)   \r", i, n_tests, n_ok + n_ignored);
        }

        let query = path_query_generator.generate();

        match client.batch_execute(format!("{query};").as_str()) {
            Ok(_) => n_ok += 1,
            Err(err) => {
                let err_str = format!("{err}");
                if ignored_errs.contains(&(err_str.as_str())) {
                    *ignored_err_count.entry(err_str).or_insert(0) += 1;
                    n_ignored += 1;
                } else {
                    errs.entry(err_str).or_insert(vec![]).push(format!("{query}"));
                    // eprintln!("Query: {query}");
                    // eprintln!("Error: {err}");
                    // break;
                }
            }
        }
    }

    println!("\n\nTotal semantically correct: {n_ok}/{n_tests} ({:.3}%)", 100f64 * n_ok as f64 / n_tests as f64);
    println!("\nWithout ignored errors: {n_ok}/{} ({:.3}%)", n_tests - n_ignored, 100f64 * n_ok as f64 / (n_tests - n_ignored) as f64);

    println!("\nError counts (ignored): \n{:#?}", ignored_err_count);
    println!("\nErrors (not ignored): \n{:#?}", errs);
}
