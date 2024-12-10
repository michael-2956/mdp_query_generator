use core::fmt;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::{fs, path::PathBuf};

use itertools::Itertools;
use postgres::{Client, NoTls};
use serde::Deserialize;
use sqlparser::ast::{ColumnDef, DataType, ExactNumberInfo, Expr, GroupByExpr, Ident, ObjectName, SetExpr, Statement, TableConstraint, TimezoneInfo};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

use crate::config::{Config, TomlReadable};
use crate::equivalence_testing_function::string_to_query;
use crate::query_creation::query_generator::query_info::{CreateTableSt, DatabaseSchema, IdentName};
use crate::query_creation::query_generator::value_choosers::DeterministicValueChooser;
use crate::query_creation::query_generator::QueryGenerator;
use crate::query_creation::state_generator::state_choosers::MaxProbStateChooser;
use crate::query_creation::state_generator::substitute_models::PathModel;
use crate::query_creation::state_generator::MarkovChainGenerator;
use crate::training::ast_to_path::PathGenerator;
use crate::{unwrap_pat, unwrap_variant};

#[derive(Debug, Clone)]
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

fn connect_to_db(db_name: &str) -> Result<Client, Box<dyn std::error::Error>> {
    let conn_str = format!("host=localhost user=query_test_user dbname={}", db_name);
    Ok(Client::connect(&conn_str, NoTls)?)
}

pub fn create_database_and_connect(db_name: &str) -> Result<Client, Box<dyn std::error::Error>> {
    let create_db_command = format!("CREATE DATABASE {};", db_name);
    {
        let mut client = Client::connect("host=localhost user=query_test_user", NoTls)?;
        client.batch_execute(&create_db_command)?;
    }
    connect_to_db(db_name)
}

pub fn drop_database_if_exists(db_name: &str) -> Result<(), Box<dyn std::error::Error>> {
    let drop_db_command = format!("DROP DATABASE IF EXISTS {};", db_name);
    let mut client = Client::connect("host=localhost user=query_test_user", NoTls)?;
    client.batch_execute(&drop_db_command)?;
    Ok(())
}

#[derive(Debug)]
enum PostgreSQLErrorFilterKind {
    StartEnd {
        starts_with: String,
        ends_with: String,
    },
    Contains {
        what: String,
    },
    ContainsSequence {
        sequence: Vec<String>,
    }
}

struct PostgreSQLErrorFilter {
    filter_kind: PostgreSQLErrorFilterKind,
    fix: Option<Box<dyn Fn(&PostgreSQLErrorFilter, String, String) -> Vec<String>>>,
}

impl PostgreSQLErrorFilter {
    fn contains(what: &str) -> Self {
        Self {
            filter_kind: PostgreSQLErrorFilterKind::Contains { what: what.to_string() },
            fix: None
        }
    }

    fn contains_sequence(members: &[&str]) -> Self {
        Self {
            filter_kind: PostgreSQLErrorFilterKind::ContainsSequence { sequence: members.into_iter().map(|m| m.to_string()).collect() },
            fix: None
        }
    }

    fn starts_ends(starts_with: &str, ends_with: &str) -> Self {
        Self {
            filter_kind: PostgreSQLErrorFilterKind::StartEnd { starts_with: starts_with.to_string(), ends_with: ends_with.to_string() },
            fix: None
        }
    }

    fn add_fix(mut self, fix: Box<dyn Fn(&PostgreSQLErrorFilter, String, String) -> Vec<String>>) -> Self {
        self.fix = Some(fix);
        self
    }

    fn try_run_fix(&self, query_str: String, err_str: String) -> Option<Vec<String>> {
        if let Some(fix) = self.fix.as_ref() {
            Some(fix(self, query_str, err_str))
        } else {
            None
        }
    }

    fn check(&self, err_str: &str) -> bool {
        let cut_ind = err_str.find("\nHINT: ").unwrap_or(err_str.len());
        match &self.filter_kind {
            PostgreSQLErrorFilterKind::StartEnd { starts_with, ends_with } => err_str.starts_with(starts_with) && err_str[0..cut_ind].ends_with(ends_with),
            PostgreSQLErrorFilterKind::Contains { what } => err_str.contains(what),
            PostgreSQLErrorFilterKind::ContainsSequence { sequence } => {
                let mut err_str = err_str;
                for s in sequence.iter() {
                    if let Some(p) = err_str.find(s) {
                        err_str = &err_str[p+s.len()..err_str.len()];
                    } else {
                        return false
                    }
                }
                return true
            },
        }
    }

    fn get_inbetween(&self, err_str: &str) -> String {
        let cut_ind = err_str.find("\nHINT: ").unwrap_or(err_str.len());
        match &self.filter_kind {
            PostgreSQLErrorFilterKind::StartEnd { starts_with, ends_with } => {
                err_str[0..cut_ind]
                    .strip_prefix(starts_with).unwrap()
                    .strip_suffix(ends_with).unwrap()
                    .to_string()
            },
            any => panic!("No in-between for the {:?} filter", any),
        }
    }

    fn get_inbetween_sequence(&self, err_str: &str) -> Vec<String> {
        match &self.filter_kind {
            PostgreSQLErrorFilterKind::ContainsSequence { sequence } => {
                let mut inbetweens = vec![];
                let mut err_str = err_str;
                for s in sequence.iter() {
                    if let Some(p) = err_str.find(s) {
                        inbetweens.push(err_str[0..p].to_string());
                        err_str = &err_str[p+s.len()..err_str.len()];
                    } else {
                        panic!("Pattern {s} not found in {err_str}")
                    }
                }
                inbetweens.push(err_str[0..err_str.len()].to_string());
                inbetweens
            },
            any => panic!("No in-between sequence for the {:?} filter", any),
        }
    }

    fn check_get_inbetween(&self, err_str: &str) -> (bool, Option<String>) {
        let cut_ind = err_str.find("\nHINT: ").unwrap_or(err_str.len());
        match &self.filter_kind {
            PostgreSQLErrorFilterKind::StartEnd { starts_with, ends_with } => {
                if err_str.starts_with(starts_with) && err_str[0..cut_ind].ends_with(ends_with) {
                    (true, Some(
                        err_str[0..cut_ind]
                            .strip_prefix(starts_with).unwrap()
                            .strip_suffix(ends_with).unwrap()
                            .to_string()
                    ))
                } else {
                    (false, None)
                }
            },
            _ => (self.check(err_str), None),
        }
    }
}

impl fmt::Display for PostgreSQLErrorFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.filter_kind {
            PostgreSQLErrorFilterKind::StartEnd { starts_with: startswith, ends_with: endswith } => write!(f, "{startswith} [...] {endswith}"),
            PostgreSQLErrorFilterKind::Contains { what } => write!(f, "{what}"),
            PostgreSQLErrorFilterKind::ContainsSequence { sequence: members } => write!(f, "{}", members.iter().join(" [...] ")),
        }
    }
}

#[derive(PartialEq, Clone)]
enum PostgreSQLErrorAction {
    /// error string
    RecognisedError(String),
    /// error string
    UnrecognisedError(String),
    /// query string
    Ok(String)
}

struct PostgreSQLErrors {
    filters: Vec<PostgreSQLErrorFilter>,
    n_errors: BTreeMap<String, usize>,
}

impl PostgreSQLErrors {
    fn with_filters(filters: Vec<PostgreSQLErrorFilter>) -> Self {
        Self {
            filters,
            n_errors: BTreeMap::new(),
        }
    }

    fn record_err_action(&mut self, action: &PostgreSQLErrorAction) {
        match action {
            PostgreSQLErrorAction::RecognisedError(err_str) => {
                let filter_str = self.filters.iter().find(|filter| filter.check(&err_str)).unwrap().to_string();
                *self.n_errors.entry(filter_str).or_insert(0) += 1;
            },
            PostgreSQLErrorAction::UnrecognisedError(err_str) => {
                *self.n_errors.entry(format!("unrecognised error: {err_str}")).or_insert(0) += 1;
            },
            PostgreSQLErrorAction::Ok(..) => panic!("record_err_action does not accept Ok(..)"),
        }
    }

    /// - If query is ok or it was fixed, returns the fixed query
    /// - In case it fails to fix, returns original error
    fn try_query(&mut self, client: &mut Client, query_str: &String, record_error: bool) -> PostgreSQLErrorAction {
        let mut query_changed_from_original = false;
        let mut original_error = None;
        let mut original_error_key = None;
        let mut past_errors = BTreeSet::new();
        let mut fixed_query = None;
        let mut query_queue = VecDeque::new();
        query_queue.push_back(query_str.clone());
        while let Some(query_str) = query_queue.pop_front() {
            client.batch_execute("BEGIN;").unwrap();
            let rs = client.batch_execute(format!("BEGIN;\n{query_str};\nROLLBACK;").as_str());
            client.batch_execute("ROLLBACK;").unwrap();
            if let Err(err) = rs {
                let err_str = format!("{err}");
                if past_errors.contains(&err_str) {
                    continue;
                }
                past_errors.insert(err_str.clone());
                match self.filters.iter().find(|filter| filter.check(&err_str)) {
                    Some(filter) => {
                        if !query_changed_from_original {
                            original_error_key = Some(filter.to_string());
                            original_error = Some(PostgreSQLErrorAction::RecognisedError(err_str.clone()));
                        }
                        if let Some(new_query_strs) = filter.try_run_fix(query_str.clone(), err_str) {
                            query_queue.extend(new_query_strs.into_iter());
                            query_changed_from_original = true;
                        }
                    },
                    None => {
                        if !query_changed_from_original {
                            original_error_key = Some(format!("unrecognised error: {err_str}"));
                            original_error = Some(PostgreSQLErrorAction::UnrecognisedError(err_str));
                        }
                    },
                }
            } else {
                fixed_query = Some(query_str);
                break;
            }
        }
        if let Some(query_str) = fixed_query {
            PostgreSQLErrorAction::Ok(query_str)
        } else {
            if record_error {
                *self.n_errors.entry(original_error_key.unwrap()).or_insert(0) += 1;
            }
            original_error.unwrap()
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

fn create_schema_try_reorder_try_fix(db_name: &String, table_defs: &Vec<CreateTableSt>, psql_schema_errors: &mut PostgreSQLErrors) -> PostgreSQLErrorAction {
    let mut client = create_database_and_connect(&db_name).unwrap();

    let relation_doesnt_exist_filter = PostgreSQLErrorFilter::starts_ends("db error: ERROR: relation ", " does not exist");

    let mut success = true;

    let mut table_def_queue = VecDeque::<String>::new();
    for table_def in table_defs.iter() {
        table_def_queue.push_back(format!("{table_def};"));
    }

    let mut err_action = None;
    let mut schema_str = "".to_string();

    let mut error_counter = 0usize;

    let mut did_reorder = false;

    while let Some(table_def_str) = table_def_queue.pop_front() {
        let new_schema_str_candidate = schema_str.clone() + format!("{table_def_str};").as_str();
        match psql_schema_errors.try_query(&mut client, &new_schema_str_candidate, false) {
            ref e @ (
                PostgreSQLErrorAction::RecognisedError(ref err_table_str) |
                PostgreSQLErrorAction::UnrecognisedError(ref err_table_str)
            ) => {
                if relation_doesnt_exist_filter.check(err_table_str.as_str()) {
                    did_reorder = true;
                    error_counter += 1;
                    table_def_queue.push_back(table_def_str);
                } else {
                    err_action = Some(e.clone());
                    success = false;
                    break;
                }
            },
            PostgreSQLErrorAction::Ok(new_schema_str) => {
                schema_str = new_schema_str;
                error_counter = 0usize;
            },
        }
        // this means there are circular relations and reorder didn't help
        if error_counter > table_def_queue.len() {
            err_action = Some(PostgreSQLErrorAction::RecognisedError(relation_doesnt_exist_filter.to_string()));
            success = false;
            break;
        }
    }

    client.close().unwrap();

    if success {
        if did_reorder {
            PostgreSQLErrorAction::Ok(format!("-- NOTE: schema was reordered\n\n{schema_str}"))
        } else {
            PostgreSQLErrorAction::Ok(schema_str)
        }
    } else {
        drop_database_if_exists(&db_name).unwrap();
        let err_action = err_action.unwrap();
        psql_schema_errors.record_err_action(&err_action);
        err_action
    }
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
    // does not include duplicates
    let mut n_total = 0usize;
    let mut n_parse_err = 0usize;
    let mut psql_query_errors = PostgreSQLErrors::with_filters(vec![
        PostgreSQLErrorFilter::starts_ends("db error: ERROR: column ", " does not exist").add_fix(Box::new(|_self, query_str, err_str| {
            let column_name = _self.get_inbetween(&err_str);
            if let Some(unquoted) = column_name.strip_prefix("\"").and_then(|column_name| column_name.strip_suffix("\"")) {
                vec![
                    // for when it was actually a column name in ""
                    query_str.replace(column_name.as_str(), unquoted),
                    // for when it was actually a string in ""
                    query_str.replace(column_name.as_str(), format!("'{unquoted}'").as_str()),
                ]
            } else {
                vec![]
            }
        })),
        PostgreSQLErrorFilter::starts_ends("db error: ERROR: column ", " must appear in the GROUP BY clause or be used in an aggregate function").add_fix(
            Box::new(|_self, query_str, err_str| {
                let column_name = _self.get_inbetween(&err_str);
                let mut altered = false;
                let mut query_ast = string_to_query(query_str.as_str()).unwrap();
                if let Some(unquoted) = column_name.strip_prefix("\"").and_then(|column_name| column_name.strip_suffix("\"")) {
                    if let SetExpr::Select(select) = &mut *query_ast.body {
                        if let GroupByExpr::Expressions(group_by_exprs) = &mut select.group_by {
                            group_by_exprs.push(Expr::CompoundIdentifier(unquoted.split('.').into_iter().map(
                                |x| Ident::new(x)
                            ).collect()));
                            altered = true;
                        }
                    }
                }
                if altered {
                    vec![format!("{query_ast}")]
                } else {
                    vec![]
                }
            }
        )),
        PostgreSQLErrorFilter::starts_ends("db error: ERROR: relation ", " does not exist"),
        PostgreSQLErrorFilter::contains("db error: ERROR: for SELECT DISTINCT, ORDER BY expressions must appear in select list"),
        PostgreSQLErrorFilter::contains("db error: ERROR: function avg(character varying) does not exist"),
        PostgreSQLErrorFilter::contains("db error: ERROR: function avg(character) does not exist"),
        PostgreSQLErrorFilter::contains("db error: ERROR: function avg(text) does not exist"),
        PostgreSQLErrorFilter::contains("db error: ERROR: function sum(character varying) does not exist"),
        PostgreSQLErrorFilter::contains("db error: ERROR: function sum(character) does not exist"),
        PostgreSQLErrorFilter::contains("db error: ERROR: function sum(text) does not exist"),
        PostgreSQLErrorFilter::contains_sequence(&["db error: ERROR: operator does not exist: ", " > ", ""]),
        PostgreSQLErrorFilter::contains_sequence(&["db error: ERROR: operator does not exist: ", " >= ", ""]),
        PostgreSQLErrorFilter::contains_sequence(&["db error: ERROR: operator does not exist: ", " = ", ""]),
        PostgreSQLErrorFilter::contains_sequence(&["db error: ERROR: operator does not exist: ", " <= ", ""]),
        PostgreSQLErrorFilter::contains_sequence(&["db error: ERROR: operator does not exist: ", " < ", ""]),
        PostgreSQLErrorFilter::contains("db error: ERROR: syntax error at or near \"INTERSECT\""),
        PostgreSQLErrorFilter::contains("db error: ERROR: syntax error at or near \"GROUP\""),
        PostgreSQLErrorFilter::contains("db error: ERROR: syntax error at or near \"WHERE\""),
        PostgreSQLErrorFilter::contains("db error: ERROR: syntax error at or near \";\""),
        PostgreSQLErrorFilter::contains("db error: ERROR: subquery in FROM must have an alias"),
        PostgreSQLErrorFilter::contains("db error: ERROR: missing FROM-clause entry for table "),
        PostgreSQLErrorFilter::contains_sequence(&["db error: ERROR: invalid input syntax for type ", ": \"", "\""]),
    ]);
    
    let debug_filter = PostgreSQLErrorFilter::starts_ends("db error: ERROR: column ", " must appear in the GROUP BY clause or be used in an aggregate function");
    let mut info_set = BTreeSet::new();

    let mut psql_schema_errors = PostgreSQLErrors::with_filters(vec![
        PostgreSQLErrorFilter::contains_sequence(&[
            "db error: ERROR: foreign key constraint", "cannot be implemented",
            "Key columns", "and", "are of incompatible types:", "and", "."
        ]).add_fix(Box::new(|_self, statements_str, error_str| {
            
            let inbetween_sequence = _self.get_inbetween_sequence(&error_str);
            let (column_name_1, column_name_2, type_1, type_2) = inbetween_sequence.into_iter().skip(3).take(4).collect_tuple().unwrap();
            // these will be in lowercase
            let column_name_1 = column_name_1.trim_matches(|c| c == '"' || c == ' ').to_string();
            let column_name_2 = column_name_2.trim_matches(|c| c == '"' || c == ' ').to_string();
            let type_1 = type_1.trim_matches(|c| c == '"' || c == ' ').to_string();
            let type_2 = type_2.trim_matches(|c| c == '"' || c == ' ').to_string();

            // rule says whether last definition has the issue and the new data type
            let rule_opt = [
                (("text", "integer"), (true, DataType::Integer(None))),
                (("integer", "text"), (false, DataType::Integer(None))),
                (("character varying", "integer"), (true, DataType::Integer(None))),
                (("integer", "character varying"), (false, DataType::Integer(None))),
                (("text", "real"), (true, DataType::Real)),
            ].into_iter().find_map(|((rtp1, rtp2), rule)| {
                if type_1 == rtp1 && type_2 == rtp2 { Some(rule) } else { None }
            });

            let mut create_table_stmts = Parser::parse_sql(&PostgreSqlDialect {}, &statements_str).unwrap();
            let last_create_table_stmt = create_table_stmts.iter_mut().last().unwrap();

            if let Some((change_last, new_data_type)) = rule_opt {
                let modded_column_def = if change_last {
                    // if we want to change last definition
                    let columns = unwrap_pat!(last_create_table_stmt, Statement::CreateTable { columns, .. }, columns);
                    columns.iter_mut().find(|col| col.name.value.to_lowercase() == column_name_1).unwrap()
                } else {
                    // if we want to change some previous definition
                    let constraints = unwrap_pat!(last_create_table_stmt, Statement::CreateTable { constraints, .. }, constraints);
                    let problem_constraint = constraints.iter_mut().find_map(|constraint| {
                        if let TableConstraint::ForeignKey { columns, referred_columns, .. } = constraint {
                            let constraint_column = columns.iter_mut().next().unwrap();
                            if constraint_column.value.to_lowercase() == column_name_1 {
                                let referred_column = referred_columns.iter_mut().next().unwrap();
                                if referred_column.value.to_lowercase() == column_name_2 {
                                    Some(constraint)
                                } else { None }
                            } else { None }
                        } else { None }
                    }).unwrap();
                    let TableConstraint::ForeignKey { foreign_table, .. } = problem_constraint else { unreachable!() };
                    let foreign_table_name = foreign_table.clone();
                    let foreign_table_column = create_table_stmts.iter_mut().find_map(|stmt| {
                        if let Statement::CreateTable { name, columns, .. } = stmt {
                            if *name == foreign_table_name {
                                Some(columns.iter_mut().find(|col| col.name.value.to_lowercase() == column_name_2).unwrap())
                            } else { None }
                        } else { None }
                    }).unwrap();
                    foreign_table_column
                };
                modded_column_def.data_type = new_data_type;
                vec![format!("{}", create_table_stmts.into_iter().map(|stmt| format!("{stmt};\n")).join(""))]
            } else {
                eprintln!("\n\n\nLast statement:\n{last_create_table_stmt}\n");
                eprintln!("{column_name_1} ({type_1}) ||| {column_name_2} ({type_2})\n\n");
                vec![]
            }
        })),
        PostgreSQLErrorFilter::contains_sequence(
            &["db error: ERROR: there is no unique constraint matching given keys for referenced table \"", "\""]
        ).add_fix(Box::new(|_self, statements_str, error_str| {
            let inbetween_sequence = _self.get_inbetween_sequence(&error_str);
            let foreign_table_name = IdentName::from(Ident::new(inbetween_sequence.into_iter().skip(1).next().unwrap()));

            let mut create_table_stmts = Parser::parse_sql(&PostgreSqlDialect {}, &statements_str).unwrap();
            let last_create_table_stmt = create_table_stmts.iter_mut().last().unwrap();

            let constraints = unwrap_pat!(last_create_table_stmt, Statement::CreateTable { constraints, .. }, constraints);
            let unique_column_list = constraints.iter_mut().filter_map(|constraint| {
                if let TableConstraint::ForeignKey { foreign_table, referred_columns, .. } = constraint {
                    if IdentName::from(foreign_table.0[0].clone()) == foreign_table_name {
                        Some(referred_columns.iter().next().unwrap().clone())
                    } else { None }
                } else { None }
            }).collect_vec();

            let foreign_table_constraints = create_table_stmts.iter_mut().find_map(|stmt| {
                if let Statement::CreateTable { name, constraints, .. } = stmt {
                    if IdentName::from(name.0[0].clone()) == foreign_table_name {
                        Some(constraints)
                    } else { None }
                } else { None }
            }).unwrap();

            let mut n_now_unique = 0usize;
            for unique_column_name in unique_column_list {
                // ensure all referenced columns are unique
                if foreign_table_constraints.iter().find(
                    |constraint| matches!(
                        constraint, TableConstraint::Unique { columns, .. } if columns.contains(&unique_column_name)
                    )
                ).is_none() {
                    foreign_table_constraints.push(TableConstraint::Unique {
                        name: None,
                        columns: vec![unique_column_name],
                        is_primary: false
                    });
                    n_now_unique += 1;
                }
            }

            if n_now_unique > 0 {
                vec![format!("{}", create_table_stmts.into_iter().map(|stmt| format!("{stmt};\n")).join(""))]
            } else {
                vec![]
            }
        })),
        PostgreSQLErrorFilter::contains_sequence(&[
            "db error: ERROR: column \"", "\" referenced in foreign key constraint does not exist"
        ]).add_fix(Box::new(|_self, statements_str, error_str| {
            let inbetween_sequence = _self.get_inbetween_sequence(&error_str);
            let err_foreign_column_name = IdentName::from(Ident::new(inbetween_sequence.into_iter().skip(1).next().unwrap()));

            // 3 fixes for 3 separate situations in 3 DBs
            let fixes = [
                (("keyword", "kid"), (None, Some("id"))),  // DB: imdb
                (("player", "team_id"), (Some("team"), None)),  // DB: baseball_1
                (("restaurant", "restaurant_id"), (None, Some("id"))),  // DB: restaurants
            ];

            let mut create_table_stmts = Parser::parse_sql(&PostgreSqlDialect {}, &statements_str).unwrap();
            let last_create_table_stmt = create_table_stmts.iter_mut().last().unwrap();
            let constraints = unwrap_pat!(last_create_table_stmt, Statement::CreateTable { constraints, .. }, constraints);
            let mut fixed = false;

            for ((foreign_table_name, foreign_column_name), (table_name_fix, column_name_fix)) in fixes {
                let (foreign_table_name, foreign_column_name) = (
                    IdentName::from(Ident::new(foreign_table_name)),
                    IdentName::from(Ident::new(foreign_column_name))
                );
                if err_foreign_column_name != foreign_column_name { continue; }
                let constraints = constraints.iter_mut().filter_map(|constraint| {
                    if let TableConstraint::ForeignKey { foreign_table, referred_columns, .. } = constraint {
                        if IdentName::from(foreign_table.0[0].clone()) == foreign_table_name {
                            if IdentName::from(referred_columns.iter().next().unwrap().clone()) == foreign_column_name {
                                Some(constraint)
                            } else { None }
                        } else { None }
                    } else { None }
                }).collect_vec();
                for constraint in constraints {
                    let TableConstraint::ForeignKey { foreign_table, referred_columns, .. } = constraint else { unreachable!() };
                    if let Some(table_name_fix) = table_name_fix {
                        *foreign_table = ObjectName(vec![Ident::new(table_name_fix)]);
                        fixed = true;
                    }
                    if let Some(column_name_fix) = column_name_fix {
                        *referred_columns = vec![Ident::new(column_name_fix)];
                        fixed = true;
                    }
                }
            }

            if fixed {
                vec![format!("{}", create_table_stmts.into_iter().map(|stmt| format!("{stmt};\n")).join(""))]
            } else {
                vec![]
            }
        })),
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

    let mut failed_queries = vec![];

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

        let fixed_schema = match create_schema_try_reorder_try_fix(&db_name, &db.table_defs, &mut psql_schema_errors) {
            PostgreSQLErrorAction::RecognisedError(..) => {
                assert!(incorrect_schemas.contains(&db_id.as_str()));
                n_incorrect_schema += db_queries.len();
                continue;
            },
            PostgreSQLErrorAction::UnrecognisedError(err_str) => {
                eprintln!("\nDB ID: {db_id}");
                eprintln!("Error: {err_str}");
                if !incorrect_schemas.contains(&db_id.as_str()) {
                    eprintln!("\n\nDB: {db_id}");
                    eprintln!("Schema:\n{}", db.get_schema_string());
                    eprintln!("PostgreSQL error: {err_str}");
                    // eprintln!("AST:\n{}", db.table_defs.iter().map(|td| format!("{:#?}", td)).join("\n\n"));
                    break;
                } else {
                    continue;
                }
            },
            PostgreSQLErrorAction::Ok(fixed_schema) => {
                if fixed_schema.starts_with("-- NOTE: schema was reordered") {
                    n_reordered_schemas += 1;
                }
                fixed_schema
            },
        };

        let mut client = connect_to_db(&db_name).unwrap();

        client.batch_execute(fixed_schema.as_str()).unwrap();

        let db = DatabaseSchema::parse_schema_string_and_adapt_sqlite(fixed_schema);

        let mut path_generator = PathGenerator::new(
            db.clone(), &config.chain_config,
            config.generator_config.aggregate_functions_distribution.clone(),
        ).unwrap();

        let mut path_query_generator = QueryGenerator::<MaxProbStateChooser>::from_state_generator_and_config_with_schema(
            MarkovChainGenerator::with_config(&config.chain_config).unwrap(),
            db.clone(),
            config.generator_config.clone(),
            Box::new(PathModel::empty())
        );

        for query_str in db_queries {

            let query_str = match psql_query_errors.try_query(&mut client, &query_str, true) {
                PostgreSQLErrorAction::RecognisedError(err_str) => {
                    if let (true, Some(col_name)) = debug_filter.check_get_inbetween(&err_str) {
                        info_set.insert(col_name[1..col_name.len()-1].to_string());
                    }
                    continue;
                },
                PostgreSQLErrorAction::UnrecognisedError(err_str) => {
                    eprintln!("\n\nDB: {db_id}");
                    eprintln!("Query: {query_str}");
                    eprintln!("PostgreSQL error: {err_str}");
                    do_break = true;
                    break;
                },
                PostgreSQLErrorAction::Ok(fixed_query_str) => fixed_query_str,
            };

            let Ok(statements) = Parser::parse_sql(
                &PostgreSqlDialect {}, query_str.as_str()
            ) else {
                n_parse_err += 1;
                continue;
            };

            let query = unwrap_variant!(statements.into_iter().next().unwrap(), Statement::Query);

            match path_generator.get_query_path(&query) {
                Ok(path) => {
                    n_ok += 1;
                    let generated_query = path_query_generator.generate_with_substitute_model_and_value_chooser(
                        Box::new(PathModel::from_path_nodes(&path)),
                        Box::new(DeterministicValueChooser::from_path_nodes(&path))
                    );
                    // might be differences in quotes and use of lowercase
                    let generated_normalised = format!("{generated_query}").replace('"', "").to_lowercase();
                    let source_normalised = format!("{query}").replace('"', "").to_lowercase();
                    if format!("{generated_query}").replace('"', "").to_lowercase() != format!("{query}").replace('"', "").to_lowercase() {
                        eprintln!("\nQuery mismatch!\nDB: {db_id}\nQuery (normalized): {source_normalised}\nReconstructed (normalized): {generated_normalised}\n");
                    }
                },
                Err(_) => {
                    failed_queries.push(query_str);
                    // let err_str = format!("{err}");
                    // eprintln!("\n\nDB: {db_id}");
                    // eprintln!("Query: {query_str}");
                    // eprintln!("Error: {err_str}");
                    // eprintln!("Schema:\n{}", db.get_schema_string());
                    // // eprintln!("Schema AST:\n{}", db.table_defs.iter().map(|td| format!("{:#?}", td)).join("\n\n"));
                    // do_break = true;
                    // break;
                },
            }
        }

        client.close().unwrap();
        drop_database_if_exists(&db_name).unwrap();

        if do_break {
            break;
        }
    }

    // println!("Info set:\n    {}", info_set.into_iter().join("\n    "));
    println!("\n\nQuery error summary:");
    
    println!("{n_ok} / {n_total} converted succesfully ({:.2}%)", 100f64 * n_ok as f64 / n_total as f64);
    println!("{n_parse_err} / {n_total} parse errors ({:.2}%)", 100f64 * n_parse_err as f64 / n_total as f64);

    println!("\n{n_incorrect_schema} / {n_total} incorrect schemas ({:.2}%)", 100f64 * n_incorrect_schema as f64 / n_total as f64);
    println!("Schema errors include:");
    psql_schema_errors.print_stats(n_dbs);

    println!("\nOther Postgres Errors:");
    psql_query_errors.print_stats(n_total);
    
    let n_rest: usize = n_total
        - n_ok
        - n_parse_err
        - n_incorrect_schema
        - psql_query_errors.total_errors();
    println!("\nOther convertion errors (AST2Path): {n_rest} / {n_total} ({:.2}%)", 100f64 * n_rest as f64 / n_total as f64);

    let n_valid = n_total - psql_query_errors.total_errors() - n_incorrect_schema;
    println!("\nIn total, {n_ok}/{n_valid} ({:.2}%) of valid queries were converted", 100f64 * n_ok as f64 / n_valid as f64);

    println!("\nOut of schemas, {n_reordered_schemas}/{n_dbs} ({:.2}%) were reordered", 100f64 * n_reordered_schemas as f64 / n_dbs as f64);
    
    println!("\nIn total, {n_duplicates} / {} ({:.2}%) of queries were duplicates", n_duplicates + n_total, 100f64 * n_duplicates as f64 / (n_duplicates + n_total) as f64);

    println!("Failed queries:\n    {}", failed_queries.into_iter().join("\n    "));
}