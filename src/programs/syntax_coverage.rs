use std::{fs, path::PathBuf};

use itertools::Itertools;
use serde::Deserialize;
use sqlparser::ast::{SetExpr, Statement};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

use crate::config::{Config, TomlReadable};
use crate::query_creation::query_generator::query_info::{CreateTableSt, DatabaseSchema};
use crate::training::ast_to_path::PathGenerator;
use crate::unwrap_variant;

pub struct SyntaxCoverageConfig {
    spider_tables_json_path: PathBuf,
    spider_queries_json_path: PathBuf,
}

impl TomlReadable for SyntaxCoverageConfig {
    fn from_toml(toml_config: &toml::Value) -> Self {
        let section = &toml_config["syntax_coverage"];
        Self {
            spider_tables_json_path: PathBuf::from(section["spider_tables_json_path"].as_str().unwrap()),
            spider_queries_json_path: PathBuf::from(section["spider_queries_json_path"].as_str().unwrap()),
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct SpiderDatabase {
    // column_names: Vec<(i64, String)>,
    column_names_original: Vec<(i64, String)>,
    column_types: Vec<String>,
    db_id: String,
    // foreign_keys: Vec<Vec<i64>>,
    // primary_keys: Vec<i64>,
    // table_names: Vec<String>,
    table_names_original: Vec<String>,
}

pub struct ProccessedSpiderTable {
    pub table_name: String,
    pub column_types_str: Vec<(String, String)>,
}

impl ProccessedSpiderTable {
    fn from_json(json_table: SpiderDatabase) -> (String, Vec<Self>) {
        (json_table.db_id, json_table.table_names_original.into_iter().enumerate()
            .map(|(i, table_name)| {
                Self {
                    table_name,
                    column_types_str: json_table.column_names_original.iter().zip(json_table.column_types.iter()).filter_map(
                        |((table_i, name), tp)| {
                            if *table_i as usize == i {
                                Some((name.clone(), tp.clone()))
                            } else { None }
                        }
                    ).collect_vec(),
                }
            }).collect())
    }
}

#[derive(Debug, Deserialize)]
struct SpiderQuery {
    db_id: String,
    query: String,
    // query_toks: Vec<String>,
    // query_toks_no_value: Vec<String>,
    // question: String,
    // question_toks: Vec<String>,
    // sql field is ignored
}


pub fn test_syntax_coverage(config: Config) {
    let dbs: Vec<SpiderDatabase> = serde_json::from_str(fs::read_to_string(
        config.syntax_coverage_config.spider_tables_json_path
    ).unwrap().as_str()).unwrap();
    let queries: Vec<SpiderQuery> = serde_json::from_str(fs::read_to_string(
        config.syntax_coverage_config.spider_queries_json_path
    ).unwrap().as_str()).unwrap();
    let dbs = dbs.into_iter().map(
        ProccessedSpiderTable::from_json
    ).map(|(db_id, tables)| {
        (db_id, DatabaseSchema::with_tables(
            tables.into_iter().map(CreateTableSt::from_spider).collect_vec()
        ))
    }).collect_vec();
    let mut n_ok = 0usize;
    let mut n_total = 0usize;
    for (db_id, db) in dbs.into_iter() {
        eprint!("Testing: {n_total} / {} ({n_ok} OK)   \r", queries.len());
        let db_queries = queries.iter().filter_map(
            |sq| if sq.db_id == db_id {
                Some(sq.query.clone())
            } else { None }
        ).collect_vec();
        let mut path_generator = PathGenerator::new(
            db, &config.chain_config,
            config.generator_config.aggregate_functions_distribution.clone(),
        ).unwrap();
        n_total += db_queries.len();
        for query_str in db_queries {
            // println!("Query: {query_str}");
            let Ok(statements) = Parser::parse_sql(
                &PostgreSqlDialect {}, query_str.as_str()
            ) else { continue; };
            let query = unwrap_variant!(statements.into_iter().next().unwrap(), Statement::Query);
            if !matches!(*query.body, SetExpr::Select(..)) {
                continue;
            }
            n_ok += path_generator.get_query_path(&query).is_ok() as usize;
            // todo: test that path results in the same query
        }
    }
    println!("{n_ok} / {n_total} converted succesfully ({:.2}%)", 100f64 * n_ok as f64 / n_total as f64);
}