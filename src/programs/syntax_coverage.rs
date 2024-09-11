use std::collections::BTreeSet;
use std::{fs, path::PathBuf};

use itertools::Itertools;
use serde::Deserialize;
use sqlparser::ast::{SetExpr, Statement};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

use crate::config::{Config, TomlReadable};
use crate::query_creation::query_generator::query_info::DatabaseSchema;
use crate::training::ast_to_path::PathGenerator;
use crate::unwrap_variant;

pub struct SyntaxCoverageConfig {
    spider_schemas_folder: PathBuf,
    spider_tables_json_path: PathBuf,
    spider_queries_json_path: PathBuf,
}

impl TomlReadable for SyntaxCoverageConfig {
    fn from_toml(toml_config: &toml::Value) -> Self {
        let section = &toml_config["syntax_coverage"];
        Self {
            spider_schemas_folder: PathBuf::from(section["spider_schemas_folder"].as_str().unwrap()),
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
    pub fn from_json(json_table: SpiderDatabase) -> (String, Vec<Self>) {
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

    // let schema_path = config.syntax_coverage_config.spider_schemas_folder.join(format!("{}.sql", "perpetrator"));
    // let schema_str = fs::read_to_string(schema_path).unwrap().replace("PRAGMA", "-- PRAGMA");
    // let database = DatabaseSchema::parse_schema_string(schema_str);

    // println!("{:#?}", database.table_defs.iter().map(
    //     |x| x.name.to_string().to_uppercase()
    // ).collect_vec());

    // return;

    // let dbs = dbs.into_iter().map(
    //     ProccessedSpiderTable::from_json
    // ).map(|(db_id, tables)| {
    //     (db_id, DatabaseSchema::with_tables(
    //         tables.into_iter().map(CreateTableSt::from_spider).collect_vec()
    //     ))
    // }).collect_vec();

    let dbs = dbs.into_iter().map(|db| {
        let schema_path = config.syntax_coverage_config.spider_schemas_folder.join(format!("{}.sql", db.db_id));
        let schema_str = fs::read_to_string(schema_path).unwrap().replace("PRAGMA", "-- PRAGMA");
        let database = DatabaseSchema::parse_schema_string(schema_str);
        (db.db_id, database)
    }).collect_vec();

    let queries: Vec<SpiderQuery> = serde_json::from_str(fs::read_to_string(
        config.syntax_coverage_config.spider_queries_json_path
    ).unwrap().as_str()).unwrap();

    let mut n_duplicates = 0usize;
    let mut n_ok = 0usize;
    let mut n_total = 0usize;
    let mut n_parse_err = 0usize;
    let mut n_other_set_op = 0usize;
    let mut n_absent_table = 0usize;
    let mut n_no_subquery_alias = 0usize;
    for (db_id, db) in dbs.into_iter() {
        // eprintln!("\n\nDB: {db_id}\n");
        eprint!("Testing: {} / {} ({n_ok} OK)   \r", n_duplicates + n_total, queries.len());
        
        let db_queries = queries.iter().filter_map(
            |sq| if sq.db_id == db_id {
                Some(sq.query.clone())
            } else { None }
        ).collect::<BTreeSet::<_>>();
        n_duplicates += queries.iter().filter(|sq| sq.db_id == db_id).count() - db_queries.len();
        n_total += db_queries.len();
        
        let mut path_generator = PathGenerator::new(
            db, &config.chain_config,
            config.generator_config.aggregate_functions_distribution.clone(),
        ).unwrap();
        
        for query_str in db_queries {
            let Ok(statements) = Parser::parse_sql(
                &PostgreSqlDialect {}, query_str.as_str()
            ) else {
                n_parse_err += 1;
                // println!("Parse error in {db_id}.\nQuery: {query_str}");
                continue;
            };

            let query = unwrap_variant!(statements.into_iter().next().unwrap(), Statement::Query);
            if !matches!(*query.body, SetExpr::Select(..)) {
                n_other_set_op += 1;
                continue;
            }

            // eprintln!("Query: {query_str}");

            match path_generator.get_query_path(&query) {
                Ok(_) => n_ok += 1, // todo: test that path results in the same query
                Err(err) => {
                    if format!("{err}").contains("Table retrieval error: Couldn't find table") {
                        n_absent_table += 1;
                    }
                    if format!("{err}").contains("Expected SetExpr::Select, got") {
                        n_other_set_op += 1;
                    }
                    if format!("{err}").contains("No alias for subquery") {
                        n_no_subquery_alias += 1;
                    }
                },
            }
        }
    }
    println!("\nError summary:");
    
    println!("{n_ok} / {n_total} converted succesfully ({:.2}%)", 100f64 * n_ok as f64 / n_total as f64);
    println!("{n_parse_err} / {n_total} parse errors ({:.2}%)", 100f64 * n_parse_err as f64 / n_total as f64);
    println!("{n_other_set_op} / {n_total} unimplemented set expressions ({:.2}%)", 100f64 * n_other_set_op as f64 / n_total as f64);
    println!("{n_absent_table} / {n_total} absent tables ({:.2}%)", 100f64 * n_absent_table as f64 / n_total as f64);
    println!("{n_no_subquery_alias} / {n_total} no subquery alias ({:.2}%)", 100f64 * n_no_subquery_alias as f64 / n_total as f64);
    
    let n_rest = n_total - n_ok - n_parse_err - n_other_set_op - n_absent_table - n_no_subquery_alias;
    println!("{n_rest} / {n_total} other convertion errors ({:.2}%)", 100f64 * n_rest as f64 / n_total as f64);
    
    println!("\nIn total, {n_duplicates} / {} ({:.2}%) of queries were duplicates", queries.len(), 100f64 * n_duplicates as f64 / queries.len() as f64);
}