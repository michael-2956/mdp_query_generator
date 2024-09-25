use core::fmt;
use std::collections::{BTreeSet, HashMap, VecDeque};
use std::{fs, path::PathBuf};

use itertools::Itertools;
use postgres::{Client, NoTls};
use serde::Deserialize;
use sqlparser::ast::{ColumnDef, DataType, ExactNumberInfo, Ident, ObjectName, SetExpr, Statement, TableConstraint, TimezoneInfo};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

use crate::config::{Config, TomlReadable};
use crate::query_creation::query_generator::query_info::{CreateTableSt, DatabaseSchema};
use crate::query_creation::query_generator::value_choosers::DeterministicValueChooser;
use crate::query_creation::query_generator::QueryGenerator;
use crate::query_creation::state_generator::state_choosers::MaxProbStateChooser;
use crate::query_creation::state_generator::substitute_models::PathModel;
use crate::query_creation::state_generator::MarkovChainGenerator;
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
    // column_names_original: Vec<(i64, String)>,
    // column_types: Vec<String>,
    db_id: String,
    // foreign_keys: Vec<Vec<i64>>,
    // primary_keys: Vec<i64>,
    // table_names: Vec<String>,
    // table_names_original: Vec<String>,
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

fn transform_sqlite_ident(mut id: Ident) -> Ident {
    if id.quote_style == Some('\'') {
        id.quote_style = None; // ' is a string in postgres, so simply remove
    }
    if id.value == "End" && id.quote_style == None {
        id.quote_style = Some('"'); // End is a keyword, so add "
    }
    if id.quote_style == Some('`') {
        id.quote_style = Some('"'); // postgres doe snot support `, replace with "
    }
    if id.quote_style == Some('"') {
        id.value = id.value.to_lowercase(); // lowercase the quoted idents to make sqlite queries work
    }
    id
}

fn transform_sqlite_ident_opt(id_opt: Option<Ident>) -> Option<Ident> {
    id_opt.map(|id| transform_sqlite_ident(id))
}

fn transform_sqlite_ident_vec(id_vec: Vec<Ident>) -> Vec<Ident> {
    id_vec.into_iter().map(|id| transform_sqlite_ident(id)).collect_vec()
}

fn transform_sqlite_objectname(mut ob: ObjectName) -> ObjectName {
    ob.0 = transform_sqlite_ident_vec(ob.0);
    ob
}

impl DatabaseSchema {
    fn parse_schema_string_and_adapt_sqlite(source: String) -> Self {
        let mut _self = Self::parse_schema_string(source);
        _self.table_defs = _self.table_defs.into_iter()
            // convert all quoted table and column definitions to lowercase
            .map(|mut tb: CreateTableSt| {
                tb.name = transform_sqlite_objectname(tb.name);
                tb.columns = tb.columns.into_iter().map(|mut col: ColumnDef| {
                    col.name = transform_sqlite_ident(col.name);
                    // type substitution
                    col.data_type = match col.data_type {
                        DataType::Datetime(precision) => DataType::Timestamp(precision, TimezoneInfo::None),
                        DataType::Custom(id, _) if format!("{}", id).to_lowercase().as_str() == "varchar2"
                            => DataType::Varchar(None),  // ignore precision
                        DataType::Custom(id, _) if format!("{}", id).to_lowercase().as_str() == "number"
                            => DataType::Numeric(ExactNumberInfo::None),  // ignore precision
                        DataType::Double => DataType::Numeric(ExactNumberInfo::None),
                        DataType::Int(_) => DataType::Int(None), // psql does not support precision of Int
                        DataType::BigInt(_) => DataType::BigInt(None), // psql does not support precision of BigInt
                        DataType::UnsignedMediumInt(p) => DataType::Integer(p), // closest alternative
                        DataType::UnsignedSmallInt(p) => DataType::SmallInt(p), // closest alternative
                        DataType::UnsignedTinyInt(p) => DataType::SmallInt(p), // closest alternative
                        DataType::Custom(id, _) if format!("{}", id).to_lowercase().as_str() == "year"
                            => DataType::Integer(None),
                        DataType::Blob(_) => DataType::Bytea, // closest alternative
                        any => any
                    };
                    col
                }).collect();
                tb.constraints = tb.constraints.into_iter().map(|constraint| {
                    match constraint {
                        TableConstraint::Unique { name, columns, is_primary } => TableConstraint::Unique {
                            name: transform_sqlite_ident_opt(name),
                            columns: transform_sqlite_ident_vec(columns),
                            is_primary
                        },
                        TableConstraint::ForeignKey { name, columns, foreign_table, referred_columns, on_delete, on_update } => TableConstraint::ForeignKey {
                            name: transform_sqlite_ident_opt(name),
                            columns: transform_sqlite_ident_vec(columns),
                            foreign_table: transform_sqlite_objectname(foreign_table),
                            referred_columns: transform_sqlite_ident_vec(referred_columns),
                            on_delete,
                            on_update
                        },
                        TableConstraint::Check { name, expr } => TableConstraint::Check {
                            name: transform_sqlite_ident_opt(name),
                            expr
                        },
                        TableConstraint::Index { display_as_key, name, index_type, columns } => TableConstraint::Index {
                            display_as_key,
                            name: transform_sqlite_ident_opt(name),
                            index_type,
                            columns: transform_sqlite_ident_vec(columns)
                        },
                        TableConstraint::FulltextOrSpatial { fulltext, index_type_display, opt_index_name, columns } => TableConstraint::FulltextOrSpatial {
                            fulltext,
                            index_type_display,
                            opt_index_name: transform_sqlite_ident_opt(opt_index_name),
                            columns: transform_sqlite_ident_vec(columns)
                        },
                    }
                }).collect();
                tb
            }).collect();
        _self
    }
}

pub fn test_single_query_path(config: Config) {
    let schema_str = "CREATE TABLE \"Roles2\" (
        \"RoleCode\" VARCHAR(15) PRIMARY KEY,
        \"role_description\" VARCHAR(80)
    )".to_string();

    let db = DatabaseSchema::parse_schema_string(schema_str);

    let mut path_generator = PathGenerator::new(
        db.clone(), &config.chain_config,
        config.generator_config.aggregate_functions_distribution.clone(),
    ).unwrap();

    let query_str = "select \"role_description\" from \"Roles2\";";
    // let query_str = "select \"RoleCode\" from Roles2;";

    let statements = Parser::parse_sql(&PostgreSqlDialect {}, query_str).unwrap();

    let query = unwrap_variant!(statements.into_iter().next().unwrap(), Statement::Query);

    println!("Query: {query_str}");
    let path = match path_generator.get_query_path(&query) {
        Ok(path) => {
            println!("Convertion successful!");
            path
        },
        Err(err) => {
            println!("Error: {err}");
            return;
        },
    };

    let mut path_query_generator = QueryGenerator::<MaxProbStateChooser>::from_state_generator_and_config_with_schema(
        MarkovChainGenerator::with_config(&config.chain_config).unwrap(),
        db,
        config.generator_config,
        Box::new(PathModel::empty())
    );

    let generated_query = path_query_generator.generate_with_substitute_model_and_value_chooser(
        Box::new(PathModel::from_path_nodes(&path)),
        Box::new(DeterministicValueChooser::from_path_nodes(&path))
    );

    if generated_query == *query {
        println!("Query match successful!");
        println!("Matched query: {generated_query}");
    } else {
        println!("Query mismatch!");
        println!("Matched query: {generated_query}");
    }

    return;
}

fn create_or_ensure_user_exists() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = Client::connect("host=localhost user=mykhailo", NoTls)?;
    let create_user_command = r#"
        DO $$
        BEGIN
           IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'query_test_user') THEN
              CREATE ROLE query_test_user WITH LOGIN CREATEDB;
           END IF;
        END
        $$;
    "#;
    client.batch_execute(create_user_command)?;
    Ok(())
}

pub fn create_database(db_id: &str) -> Result<(), Box<dyn std::error::Error>> {
    let create_db_command = format!("CREATE DATABASE {};", db_id);
    let mut client = Client::connect("host=localhost user=query_test_user", NoTls)?;
    client.batch_execute(&create_db_command)?;
    Ok(())
}

pub fn drop_database_if_exists(db_name: &str) -> Result<(), Box<dyn std::error::Error>> {
    let drop_db_command = format!("DROP DATABASE IF EXISTS {};", db_name);
    let mut client = Client::connect("host=localhost user=query_test_user", NoTls)?;
    client.batch_execute(&drop_db_command)?;
    Ok(())
}

enum PostgreSQLErrorFilter {
    StartEnd {
        starts_with: String,
        ends_with: String,
    },
    Contains {
        what: String,
    }
}

impl PostgreSQLErrorFilter {
    fn contains(what: &str) -> Self {
        Self::Contains { what: what.to_string() }
    }

    fn starts_ends(starts_with: &str, ends_with: &str) -> Self {
        Self::StartEnd { starts_with: starts_with.to_string(), ends_with: ends_with.to_string() }
    }

    fn check(&self, err_str: &str) -> bool {
        let cut_ind = err_str.find("\nHINT: ").unwrap_or(err_str.len());
        match self {
            PostgreSQLErrorFilter::StartEnd { starts_with, ends_with } => err_str.starts_with(starts_with) && err_str[0..cut_ind].ends_with(ends_with),
            PostgreSQLErrorFilter::Contains { what } => err_str.contains(what),
        }
    }
}

impl fmt::Display for PostgreSQLErrorFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PostgreSQLErrorFilter::StartEnd { starts_with: startswith, ends_with: endswith } => write!(f, "{startswith} [...] {endswith}"),
            PostgreSQLErrorFilter::Contains { what } => write!(f, "{what}"),
        }
    }
}

enum PostgreSQLErrorAction {
    Skip,
    Stop
}

struct PostgreSQLErrors {
    filters: Vec<PostgreSQLErrorFilter>,
    n_errors: HashMap<String, usize>,
}

impl PostgreSQLErrors {
    fn with_filters(filters: Vec<PostgreSQLErrorFilter>) -> Self {
        Self {
            filters,
            n_errors: HashMap::new(),
        }
    }

    fn record_error_and_get_action(&mut self, err_str: String) -> PostgreSQLErrorAction {
        match self.filters.iter().find(|filter| filter.check(&err_str)) {
            Some(filter) => {
                *self.n_errors.entry(filter.to_string()).or_insert(0) += 1;
                PostgreSQLErrorAction::Skip
            },
            None => PostgreSQLErrorAction::Stop,
        }
    }

    fn print_stats(&self, n_total: usize) {
        for (k, v) in self.n_errors.iter().sorted_by_key(|(_, num)| -(**num as i128)) {
            println!("#{v} ({:.2}%): {k}", 100f64 * *v as f64 / n_total as f64);
        }
    }

    fn total_errors(&self) -> usize {
        self.n_errors.values().sum()
    }
}

fn try_schema_reorder(client: &mut Client, table_defs: &Vec<CreateTableSt>, err_str: &mut String) -> bool {
    let relation_doesnt_exist_filter = PostgreSQLErrorFilter::starts_ends("db error: ERROR: relation ", " does not exist");
    let mut reorder_success = false;
    if relation_doesnt_exist_filter.check(err_str.as_str()) {
        let mut table_def_queue = VecDeque::<String>::new();
        for table_def in table_defs.iter() {
            table_def_queue.push_back(format!("{table_def};"));
        }
        reorder_success = true;
        let mut error_counter = 0usize;
        while let Some(table_def_str) = table_def_queue.pop_front() {
            if let Err(err_table) = client.batch_execute(table_def_str.as_str()) {
                let err_table_str = format!("{err_table}");
                if relation_doesnt_exist_filter.check(err_table_str.as_str()) {
                    error_counter += 1;
                    table_def_queue.push_back(table_def_str);
                } else {
                    *err_str = err_table_str;
                    reorder_success = false;
                    break;
                }
            } else {
                error_counter = 0usize;
            }
            if error_counter > table_def_queue.len() {
                reorder_success = false;
                break;
            }
        }
    }
    reorder_success
}

pub fn test_syntax_coverage(config: Config) {
    let dbs: Vec<SpiderDatabase> = serde_json::from_str(fs::read_to_string(
        config.syntax_coverage_config.spider_tables_json_path
    ).unwrap().as_str()).unwrap();

    let dbs = dbs.into_iter().map(|db| {
        let schema_path = config.syntax_coverage_config.spider_schemas_folder.join(format!("{}.sql", db.db_id));
        let schema_str = fs::read_to_string(schema_path).unwrap().replace("PRAGMA", "-- PRAGMA");
        let database = DatabaseSchema::parse_schema_string_and_adapt_sqlite(schema_str);
        (db.db_id, database)
    }).collect_vec();

    let queries: Vec<SpiderQuery> = serde_json::from_str(fs::read_to_string(
        config.syntax_coverage_config.spider_queries_json_path
    ).unwrap().as_str()).unwrap();

    create_or_ensure_user_exists().unwrap();

    let mut do_break = false;
    let mut n_duplicates = 0usize;
    let mut n_incorrect_schema = 0usize;
    let mut n_ok = 0usize;
    let mut n_total = 0usize;
    let mut n_parse_err = 0usize;
    let mut n_other_set_op = 0usize;
    let mut psql_query_errors = PostgreSQLErrors::with_filters(vec![
        PostgreSQLErrorFilter::starts_ends("db error: ERROR: column ", " does not exist"),
        PostgreSQLErrorFilter::starts_ends("db error: ERROR: column ", " must appear in the GROUP BY clause or be used in an aggregate function"),
        PostgreSQLErrorFilter::starts_ends("db error: ERROR: relation ", " does not exist"),
        PostgreSQLErrorFilter::contains("db error: ERROR: for SELECT DISTINCT, ORDER BY expressions must appear in select list"),
        PostgreSQLErrorFilter::contains("db error: ERROR: function avg(character varying) does not exist"),
        PostgreSQLErrorFilter::contains("db error: ERROR: function avg(character) does not exist"),
        PostgreSQLErrorFilter::contains("db error: ERROR: function avg(text) does not exist"),
        PostgreSQLErrorFilter::contains("db error: ERROR: function sum(character varying) does not exist"),
        PostgreSQLErrorFilter::contains("db error: ERROR: function sum(character) does not exist"),
        PostgreSQLErrorFilter::contains("db error: ERROR: function sum(text) does not exist"),
        PostgreSQLErrorFilter::contains("db error: ERROR: operator does not exist: text > integer"),
        PostgreSQLErrorFilter::contains("db error: ERROR: operator does not exist: text < integer"),
        PostgreSQLErrorFilter::contains("db error: ERROR: operator does not exist: character varying > integer"),
        PostgreSQLErrorFilter::contains("db error: ERROR: operator does not exist: character varying >= integer"),
        PostgreSQLErrorFilter::contains("db error: ERROR: operator does not exist: character varying = integer"),
        PostgreSQLErrorFilter::contains("db error: ERROR: operator does not exist: character varying < integer"),
        PostgreSQLErrorFilter::contains("db error: ERROR: operator does not exist: boolean = integer"),
        PostgreSQLErrorFilter::contains("db error: ERROR: operator does not exist: bit = integer"),
        PostgreSQLErrorFilter::contains("db error: ERROR: syntax error at or near \"INTERSECT\""),
        PostgreSQLErrorFilter::contains("db error: ERROR: syntax error at or near \"GROUP\""),
        PostgreSQLErrorFilter::contains("db error: ERROR: syntax error at or near \"WHERE\""),
        PostgreSQLErrorFilter::contains("db error: ERROR: syntax error at or near \";\""),
        PostgreSQLErrorFilter::contains("db error: ERROR: subquery in FROM must have an alias"),
        PostgreSQLErrorFilter::contains("db error: ERROR: missing FROM-clause entry for table "),
    ]);
    
    let incorrect_schemas = [
        "race_track", "academic", "phone_market", "student_assessment",
        "city_record", "imdb", "dorm_1", "shop_membership", "concert_singer",
        "museum_visit", "baseball_1", "architecture", "party_people",
        "restaurants", "performance_attendance", "culture_company",
        "employee_hire_evaluation", "aircraft", "soccer_1", "school_finance",
        "voter_1", "yelp", "loan_1", "cre_Drama_Workshop_Groups", "car_1",
        "phone_1", "wrestler"
    ];
    let mut n_reordered_schemas = 0usize;
    let n_dbs = dbs.len();

    for (db_id, db) in dbs.into_iter() {
        eprint!("Testing: {} / {} ({n_ok} / {n_total} OK)   \r", n_duplicates + n_total, queries.len());
        
        let db_queries = queries.iter().filter_map(
            |sq| if sq.db_id == db_id {
                Some(sq.query.clone())
            } else { None }
        ).collect::<BTreeSet::<_>>();
        n_duplicates += queries.iter().filter(|sq| sq.db_id == db_id).count() - db_queries.len();
        n_total += db_queries.len();

        let db_name = format!("query_test_user_{db_id}").to_lowercase();
        
        drop_database_if_exists(&db_name).unwrap();
        create_database(&db_name).unwrap();

        let conn_str = format!("host=localhost user=query_test_user dbname={}", db_name);
        let mut client = Client::connect(&conn_str, NoTls).unwrap();
        if let Err(err) = client.batch_execute(db.get_schema_string().as_str()) {
            if incorrect_schemas.contains(&db_id.as_str()) {
                n_incorrect_schema += db_queries.len();
                client.close().unwrap();
                drop_database_if_exists(&db_name).unwrap();
                continue;
            }

            let mut err_str = format!("{err}");

            if (err_str.contains("Key columns") && err_str.contains("are of incompatible types: ")) ||
                err_str.starts_with("db error: ERROR: there is no unique constraint matching given keys for referenced table ")
            {
                eprintln!("\n\nFor database: {db_id}.sql\nError: {err_str}\n\n");
                n_incorrect_schema += db_queries.len();
                client.close().unwrap();
                drop_database_if_exists(&db_name).unwrap();
                continue;
            }
            
            if try_schema_reorder(&mut client, &db.table_defs, &mut err_str) {
                n_reordered_schemas += 1;
            } else {
                eprintln!("\n\nDB: {db_id}");
                eprintln!("Schema:\n{}", db.get_schema_string());
                eprintln!("PostgreSQL error: {err_str}");
                // eprintln!("AST:\n{}", db.table_defs.iter().map(|td| format!("{:#?}", td)).join("\n\n"));
                client.close().unwrap();
                drop_database_if_exists(&db_name).unwrap();
                break;
            }
        }

        let mut path_generator = PathGenerator::new(
            db.clone(), &config.chain_config,
            config.generator_config.aggregate_functions_distribution.clone(),
        ).unwrap();
        // path_generator.set_try_with_quotes(true);

        for query_str in db_queries {

            if let Err(err) = client.batch_execute(format!("{query_str};").as_str()) {
                let err_str = format!("{err}");
                match psql_query_errors.record_error_and_get_action(err_str) {
                    PostgreSQLErrorAction::Skip => continue,
                    PostgreSQLErrorAction::Stop => {
                        eprintln!("\n\nDB: {db_id}");
                        eprintln!("Query: {query_str}");
                        eprintln!("PostgreSQL error: {err}");
                        do_break = true;
                        break;
                    },
                }
            }

            let Ok(statements) = Parser::parse_sql(
                &PostgreSqlDialect {}, query_str.as_str()
            ) else {
                n_parse_err += 1;
                continue;
            };

            let query = unwrap_variant!(statements.into_iter().next().unwrap(), Statement::Query);
            if !matches!(*query.body, SetExpr::Select(..)) {
                n_other_set_op += 1;
                continue;
            }

            match path_generator.get_query_path(&query) {
                Ok(_) => n_ok += 1, // todo: test that path results in the same query
                Err(err) => {
                    let err_str = format!("{err}");
                    if err_str.contains("Expected SetExpr::Select, got") {
                        n_other_set_op += 1;
                    } else {
                        // eprintln!("\n\nDB: {db_id}");
                        // eprintln!("Query: {query_str}");
                        // eprintln!("Error: {err_str}");
                        // eprintln!("Schema:\n{}", db.get_schema_string());
                        // // eprintln!("Schema AST:\n{}", db.table_defs.iter().map(|td| format!("{:#?}", td)).join("\n\n"));
                        // do_break = true;
                        // break;
                    }
                },
            }
        }

        client.close().unwrap();
        drop_database_if_exists(&db_name).unwrap();

        if do_break {
            break;
        }
    }
    println!("\n\nQuery error summary:");
    
    println!("{n_ok} / {n_total} converted succesfully ({:.2}%)", 100f64 * n_ok as f64 / n_total as f64);
    println!("{n_parse_err} / {n_total} parse errors ({:.2}%)", 100f64 * n_parse_err as f64 / n_total as f64);
    println!("{n_other_set_op} / {n_total} unimplemented set expressions ({:.2}%)", 100f64 * n_other_set_op as f64 / n_total as f64);
    println!("{n_incorrect_schema} / {n_total} incorrect schemas ({:.2}%)", 100f64 * n_incorrect_schema as f64 / n_total as f64);

    println!("\nOther Postgres Errors:");
    psql_query_errors.print_stats(n_total);
    
    let n_rest: usize = n_total
        - n_ok
        - n_parse_err
        - n_other_set_op
        - n_incorrect_schema
        - psql_query_errors.total_errors();
    println!("\nOther convertion errors: {n_rest} / {n_total} ({:.2}%)", 100f64 * n_rest as f64 / n_total as f64);

    let n_valid = n_total - psql_query_errors.total_errors() - n_incorrect_schema;
    println!("\nIn total, {n_ok}/{n_valid} ({:.2}%) of valid queries were converted", 100f64 * n_ok as f64 / n_valid as f64);

    println!("\nOut of schemas, {n_reordered_schemas}/{n_dbs} ({:.2}%) were reordered", 100f64 * n_reordered_schemas as f64 / n_dbs as f64);
    
    println!("\nIn total, {n_duplicates} / {} ({:.2}%) of queries were duplicates", n_duplicates + n_total, 100f64 * n_duplicates as f64 / (n_duplicates + n_total) as f64);
}