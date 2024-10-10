use std::{collections::{BTreeMap, BTreeSet, HashMap, HashSet}, error::Error, fmt, hash::Hash, path::Path};

use itertools::Itertools;
use smol_str::SmolStr;
use crate::{query_creation::state_generator::{markov_chain_generator::markov_chain::{CallModifiers, QueryTypes}, subgraph_type::{ContainsSubgraphType, SubgraphType}}, unwrap_variant};

use sqlparser::{ast::{ColumnDef, DataType, Expr, FileFormat, HiveDistributionStyle, HiveFormat, Ident, ObjectName, OnCommit, Query, SelectItem, SetExpr, SqlOption, Statement, TableAlias, TableConstraint, TimezoneInfo, UnaryOperator}, dialect::PostgreSqlDialect, parser::Parser};

use super::Unnested;

macro_rules! define_impersonation {
    ($impersonator:ident, $enum_name:ident, $variant:ident, { $($field:ident: $type:ty),* $(,)? }) => {
        #[derive(Debug, Clone)]
        pub struct $impersonator {
            $(
                pub $field: $type,
            )*
        }

        // Implement the From trait for the impersonator
        impl From<$impersonator> for $enum_name {
            fn from(impersonator: $impersonator) -> Self {
                $enum_name::$variant {
                    $(
                        $field: impersonator.$field,
                    )*
                }
            }
        }

        impl TryFrom<$enum_name> for $impersonator {
            type Error = &'static str;

            fn try_from(value: $enum_name) -> Result<Self, Self::Error> {
                if let $enum_name::$variant {
                    $(
                        $field,
                    )*
                } = value {
                    Ok($impersonator {
                        $(
                            $field,
                        )*
                    })
                } else {
                    Err("Incorrect enum variant to create impersonation")
                }
            }
        }

        // Implement Display for the impersonator
        impl std::fmt::Display for $impersonator {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                // Convert the impersonator to the enum variant and then display it
                let enum_variant: $enum_name = self.clone().into();
                write!(f, "{}", enum_variant)
            }
        }
    };
}

define_impersonation!(CreateTableSt, Statement, CreateTable, {
    or_replace: bool,
    temporary: bool,
    external: bool,
    global: Option<bool>,
    if_not_exists: bool,
    transient: bool,
    name: ObjectName,
    columns: Vec<ColumnDef>,
    constraints: Vec<TableConstraint>,
    hive_distribution: HiveDistributionStyle,
    hive_formats: Option<HiveFormat>,
    table_properties: Vec<SqlOption>,
    with_options: Vec<SqlOption>,
    file_format: Option<FileFormat>,
    location: Option<String>,
    query: Option<Box<Query>>,
    without_rowid: bool,
    like: Option<ObjectName>,
    clone: Option<ObjectName>,
    engine: Option<String>,
    comment: Option<String>,
    auto_increment_offset: Option<u32>,
    default_charset: Option<String>,
    collation: Option<String>,
    on_commit: Option<OnCommit>,
    on_cluster: Option<String>,
    order_by: Option<Vec<Ident>>,
    strict: bool,
});

impl CreateTableSt {
    /// currently returns all table columns only if to_column is a primary key
    /// otherwise, returns only to_column itself
    pub fn get_functionally_connected_columns(&self, to_column: &IdentName) -> Vec<IdentName> {
        if self.constraints.iter().find(|constraint| {
            match *constraint {
                TableConstraint::Unique { name: _, columns, is_primary } => {
                    *is_primary && columns.iter().find(|col_name| {
                        *to_column == <IdentName>::from((**col_name).clone())
                    }).is_some()
                },
                _ => false,
            }
        }).is_some() || self.columns.iter().find(|col| {
            <IdentName>::from((**col).name.clone()) == *to_column
        }).map(|x| x.options.iter().find(|option| {
            match &option.option {
                sqlparser::ast::ColumnOption::Unique { is_primary } => *is_primary,
                _ => false
            }
        }).is_some()) == Some(true) {
            self.columns.iter().map(|column_def| {
                <IdentName>::from(column_def.name.clone())
            }).collect()
        } else { vec![to_column.clone()] }
    }
}

#[derive(Debug)]
pub struct ColumnRetrievalError {
    reason: String,
}

impl Error for ColumnRetrievalError { }

impl fmt::Display for ColumnRetrievalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Column retrieval error: {}", self.reason)
    }
}

impl ColumnRetrievalError {
    pub fn new(reason: String) -> Self {
        Self { reason }
    }
}

#[derive(Debug)]
pub struct TableRetrievalError {
    reason: String,
}

impl Error for TableRetrievalError { }

impl fmt::Display for TableRetrievalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Table retrieval error: {}", self.reason)
    }
}

impl TableRetrievalError {
    pub fn new(reason: String) -> Self {
        Self { reason }
    }
}

#[derive(Debug, Clone)]
pub struct DatabaseSchema {
    pub table_defs: Vec<CreateTableSt>
}

impl std::fmt::Display for DatabaseSchema {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.table_defs.iter().try_for_each(
            |create_table_st| writeln!(f, "{};", create_table_st)
        )
    }
}

impl DatabaseSchema {
    pub fn parse_schema<P: AsRef<Path>>(source_path: P) -> Self {
        let source = std::fs::read_to_string(source_path).unwrap();
        Self::parse_schema_string(source)
    }

    pub fn parse_schema_string(source: String) -> Self {
        let dialect = PostgreSqlDialect {};
        let ast = match Parser::parse_sql(&dialect, source.as_str()) {
            Ok(ast) => ast,
            Err(err) => {
                eprintln!("Schema file parsing error! {}", err);
                panic!();
            },
        };
        Self {
            table_defs: ast.into_iter()
                // keep only create table
                .filter_map(|x| x.try_into().ok())
                .collect()
        }
    }

    pub fn with_tables(table_defs: Vec<CreateTableSt>) -> Self {
        Self { table_defs }
    }

    pub fn get_schema_string(&self) -> String {
        self.table_defs.iter().map(
            |table_def| format!("{table_def};")
        ).join("\n")
    }

    pub fn num_columns_in_table(&self, name: &ObjectName) -> usize {
        self.get_table_def_by_name(name).unwrap().columns.len()
    }

    pub fn get_all_table_names(&self) -> Vec<&ObjectName> {
        self.table_defs.iter().map(|td| &td.name).collect()
    }

    pub fn get_table_def_by_name(&self, name: &ObjectName) -> Result<&CreateTableSt, TableRetrievalError> {
        match self.table_defs.iter().find(
            |x| <IdentName>::from(x.name.0.first().unwrap().clone()) == <IdentName>::from(name.0.first().unwrap().clone())
        ) {
            Some(create_table_st) => Ok(create_table_st),
            None => Err(TableRetrievalError::new(format!("Couldn't find table {name} in the schema!"))),
        }
    }
}

/// Contains all the structures responsible for the generation context while query is being generated.
/// For example, which fields were mentioned in FROM or in GROUP BY clauses. This structure is used in
/// call modifiers to disable nodes.
#[derive(Debug, Clone)]
pub struct ClauseContext {
    database_schema: DatabaseSchema,
    query_props_stack: Vec<QueryProps>,
    all_accessible_froms: AllAccessibleFroms,
    group_by_contents_stack: Vec<GroupByContents>,
}

#[derive(Debug, Clone)]
pub enum ClauseContextCheckpoint {
    Full {
        query_props_stack: Vec<QueryProps>,
        all_accessible_froms: AllAccessibleFroms,
        group_by_contents_stack: Vec<GroupByContents>,
    },
    CutOff {
        query_props_length: usize,
        query_props_last: QueryProps,
        all_accessible_froms_checkpoint: AllAccessibleFromsCutoffCheckpoint,
        group_by_contents_length: usize,
        group_by_contents_last: GroupByContents,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CheckAccessibility {
    QualifiedColumnName,
    ColumnName,
    Either
}

#[derive(Debug, Clone)]
pub struct ColumnRetrievalOptions {
    /// the caller query requires a column to be groupped (mentioned in GROUP BY)
    pub only_group_by_columns: bool,
    /// the select aliases are shading the columns that can possibly be selected\
    /// In other words, if a column is a select alias by name, it can't be selected
    /// TODO: rename to "not_a_select_alias"
    pub shade_by_select_aliases: bool,
    /// If this is true, the caller wants the column to be able to be aggregated
    /// (put into an aggregate function)
    pub only_columns_that_can_be_aggregated: bool,
    /// In Spider SQLLite queries, some queries would\
    /// not have double quotes even though they should\
    /// have them under postgres syntax. This is set to\
    /// true when we should try to artificially add them\
    /// while column searching.\
    /// FALSE by default. Use try_with_quotes to set this.
    pub try_with_quotes: bool,
}

impl ColumnRetrievalOptions {
    pub fn new(only_group_by_columns: bool, shade_by_select_aliases: bool, only_columns_that_can_be_aggregated: bool) -> Self {
        Self {
            only_group_by_columns,
            shade_by_select_aliases,
            only_columns_that_can_be_aggregated,
            try_with_quotes: false,
        }
    }

    pub fn from_call_mods(mods: &CallModifiers) -> Self {
        Self {
            only_group_by_columns: mods.contains(&SmolStr::new("group by columns")),
            shade_by_select_aliases: mods.contains(&SmolStr::new("shade by select aliases")),
            only_columns_that_can_be_aggregated: mods.contains(&SmolStr::new("aggregate-able columns")),
            try_with_quotes: false,
        }
    }

    pub fn try_with_quotes(self, try_with_quotes: bool) -> Self {
        Self {
            only_group_by_columns: self.only_group_by_columns,
            shade_by_select_aliases: self.shade_by_select_aliases,
            only_columns_that_can_be_aggregated: self.only_columns_that_can_be_aggregated,
            try_with_quotes: try_with_quotes,
        }
    }
}

trait TypeAndQualifiedColumnIter<'a>: Iterator<Item = (&'a SubgraphType, [&'a IdentName; 2])> {}
impl<'a, T: Iterator<Item = (&'a SubgraphType, [&'a IdentName; 2])> + 'a> TypeAndQualifiedColumnIter<'a> for T {}

impl ClauseContext {
    pub fn new(database_schema: DatabaseSchema) -> Self {
        Self {
            database_schema,
            query_props_stack: vec![],
            all_accessible_froms: AllAccessibleFroms::new(),
            group_by_contents_stack: vec![],
        }
    }

    pub fn schema_ref(&self) -> &DatabaseSchema {
        &self.database_schema
    }

    pub fn get_checkpoint(&self, cutoff: bool) -> ClauseContextCheckpoint {
        if cutoff {
            ClauseContextCheckpoint::CutOff { 
                query_props_length: self.query_props_stack.len(),
                query_props_last: self.query_props_stack.last().unwrap().clone(),
                all_accessible_froms_checkpoint: self.all_accessible_froms.get_cutoff_checkpoint(),
                group_by_contents_length: self.group_by_contents_stack.len(),
                group_by_contents_last: self.group_by_contents_stack.last().unwrap().clone(),
            }
        } else {
            ClauseContextCheckpoint::Full {
                query_props_stack: self.query_props_stack.clone(),
                all_accessible_froms: self.all_accessible_froms.clone(),
                group_by_contents_stack: self.group_by_contents_stack.clone(),
            }
        }
    }

    pub fn restore_checkpoint(&mut self, checkpoint: ClauseContextCheckpoint) {
        match checkpoint {
            ClauseContextCheckpoint::Full {
                query_props_stack,
                all_accessible_froms,
                group_by_contents_stack
            } => {
                self.query_props_stack = query_props_stack;
                self.all_accessible_froms = all_accessible_froms;
                self.group_by_contents_stack = group_by_contents_stack;
            },
            ClauseContextCheckpoint::CutOff {
                query_props_length,
                query_props_last,
                all_accessible_froms_checkpoint,
                group_by_contents_length,
                group_by_contents_last
            } => {
                self.query_props_stack.truncate(query_props_length);
                *self.query_props_stack.last_mut().unwrap() = query_props_last;
                self.all_accessible_froms.restore_from_cutoff_checkpoint(all_accessible_froms_checkpoint);
                self.group_by_contents_stack.truncate(group_by_contents_length);
                *self.group_by_contents_stack.last_mut().unwrap() = group_by_contents_last;
            },
        }
    }

    pub fn on_query_begin(&mut self, subquery_call_mods: Option<&CallModifiers>) {
        if let Some(subquery_call_mods) = subquery_call_mods {
            self.query_props_stack.last_mut().unwrap().set_subquery_call_mods(subquery_call_mods);
        }
        self.query_props_stack.push(QueryProps::new());

        self.all_accessible_froms.on_query_begin();
        self.group_by_contents_stack.push(GroupByContents::new());
    }

    pub fn on_query_end(&mut self) {
        self.query_props_stack.pop();
        if let Some(query_props) = self.query_props_stack.last_mut() {
            query_props.unset_subquery_call_mods();
        }

        self.all_accessible_froms.on_query_end();
        self.group_by_contents_stack.pop();
    }

    pub fn query(&self) -> &QueryProps {
        self.query_props_stack.last().unwrap()
    }

    pub fn query_mut(&mut self) -> &mut QueryProps {
        self.query_props_stack.last_mut().unwrap()
    }

    /// from is not active when it's not finished yet
    pub fn top_active_from(&self) -> &FromContents {
        self.all_accessible_froms.top_active_from()
    }

    pub fn top_from_mut(&mut self) -> &mut AllAccessibleFroms {
        &mut self.all_accessible_froms
    }

    pub fn top_group_by(&self) -> &GroupByContents {
        self.group_by_contents_stack.last().unwrap()
    }

    pub fn top_group_by_mut(&mut self) -> &mut GroupByContents {
        self.group_by_contents_stack.last_mut().unwrap()
    }

    pub fn eprint_clause_hierarchy(&self) {
        eprintln!("Clause hierarchy: {:#?}", self.get_clause_hierarchy_iter().collect::<Vec<_>>());
    }

    pub fn get_relation_levels_selectable_by_qualified_wildcard_old(&self, allowed_types: Vec<SubgraphType>, single_column: bool) -> Vec<Vec<IdentName>> {
        // first level is always present because we're in SELECT
        self.get_filtered_from_clause_hierarchy_with_allowed_columns(ColumnRetrievalOptions::new(
            self.top_group_by().is_grouping_active(),
            false, // we are in SELECT
            false,  // we are definitely not an aggregate function argument
        )).map(|(from_contents, shaded_rel_names, _, allowed_rel_col_names_opt)| {
            from_contents.relations_iter().filter(|(rel_name, relation)| {
                !shaded_rel_names.contains(rel_name) && {
                    let all_columns: Vec<_> = relation.get_all_columns_iter().collect();
                    if single_column && all_columns.len() != 1 {
                        // the relation is not eligible if the number of columns is > 1 and single_column is on
                        false
                    } else {
                        all_columns.iter().all(|(col_name, col_type)| {
                            allowed_types.contains_generator_of(*col_type) && if let Some(
                                allowed_rel_col_names
                            ) = &allowed_rel_col_names_opt {
                                if let Some(col_name) = col_name {
                                    allowed_rel_col_names.contains(&[(**rel_name).clone(), (**col_name).clone()])
                                } else { false }
                            } else { true }
                        })
                    }
                }
            }).map(|(rel_name, ..)| rel_name.clone()).collect()
        }).collect()
    }

    fn all_columns_match_types_and_names(
        columns: Vec<(IdentName, Option<IdentName>, SubgraphType)>,
        query_types: &QueryTypes,
        allowed_rel_col_names_opt: &Option<HashSet<[IdentName; 2]>>
    ) -> bool {
        match query_types {
            QueryTypes::ColumnTypeLists { column_type_lists } => {
                column_type_lists.len() == columns.len() && columns.into_iter()
                    .zip(column_type_lists.iter()).all(
                        |((rel_name, col_name, col_type), allowed_tps)| {
                            allowed_tps.contains_generator_of(&col_type) && if let Some(
                                allowed_rel_col_names
                            ) = allowed_rel_col_names_opt {  // if the R.C restriction is present, check for it
                                if let Some(col_name) = col_name {
                                    allowed_rel_col_names.contains(&[rel_name, col_name])
                                } else { false }
                            } else { true }
                        }
                    )
            },
            QueryTypes::TypeList { type_list } => {
                columns.into_iter().all(|(rel_name, col_name, col_type)| {
                    type_list.contains_generator_of(&col_type) && if let Some(
                        allowed_rel_col_names
                    ) = allowed_rel_col_names_opt {  // if the R.C restriction is present, check for it
                        if let Some(col_name) = col_name {
                            allowed_rel_col_names.contains(&[rel_name, col_name])
                        } else { false }
                    } else { true }
                })
            },
        }
    }

    /// only works correctly if called after FROM is constructed
    pub fn can_use_wildcard(&self, query_types: &QueryTypes) -> bool {
        // first level is always present because we're in SELECT
        let (
            from_contents,
            shaded_rel_names,
            _,
            allowed_rel_col_names_opt
        ) = self.get_filtered_from_clause_hierarchy_with_allowed_columns(
            ColumnRetrievalOptions::new(
                self.top_group_by().is_grouping_active(),
                false, // we are in SELECT
                false,  // we are definitely not an aggregate function argument
            )
        ).next().unwrap();
        assert!(shaded_rel_names.len() == 0); // must be empty since first level
        let columns = from_contents.ordered_relations_iter().flat_map(|(rel_name, relation)| {
            relation.get_wildcard_columns_iter().map(
                |(col, tp)| ((*rel_name).clone(), col, tp)
            )
        }).collect_vec();
        Self::all_columns_match_types_and_names(columns, query_types, &allowed_rel_col_names_opt)
    }

    pub fn get_relation_levels_selectable_by_qualified_wildcard(&self, query_types: &QueryTypes) -> Vec<Vec<IdentName>> {
        // first level is always present because we're in SELECT (important bc this function's first level is used to check if wildcard is possible)
        self.get_filtered_from_clause_hierarchy_with_allowed_columns(ColumnRetrievalOptions::new(
            self.top_group_by().is_grouping_active(),
            false, // we are in SELECT
            false,  // we are definitely not an aggregate function argument
        )).map(|(from_contents, shaded_rel_names, _, allowed_rel_col_names_opt)| {
            from_contents.relations_iter().filter(|(rel_name, relation)| {
                !shaded_rel_names.contains(rel_name) && {
                    let columns = relation.get_wildcard_columns_iter().map(
                        |(col, tp)| ((**rel_name).clone(), col, tp)
                    ).collect_vec();
                    Self::all_columns_match_types_and_names(columns, query_types, &allowed_rel_col_names_opt)
                }
            }).map(|(rel_name, ..)| rel_name.clone()).collect()
        }).collect()
    }

    pub fn get_relation_by_name(&self, name: &IdentName) -> &Relation {
        self.all_accessible_froms.get_relation_by_name(name)
    }

    /// returns None if the level's from is empty
    fn get_clause_hierarchy_iter(&self) -> impl Iterator<Item=Option<(&FromContents, &GroupByContents, &QueryProps)>> {
        self.all_accessible_froms.get_from_contents_hierarchy_iter()
            .zip(self.group_by_contents_stack.iter().rev())
            .zip(self.query_props_stack.iter().rev())
            .map(|((a, b), c)| a.map(|a| (a, b, c)))
    }

    /// Get FROM clause hierarchy that adheres to column_retrieval_options.\
    /// Iterator produces:\
    /// - FROM clause
    /// - a list of shaded relation names
    /// - a list of shaded column names
    /// - optionally, a set of allowed R.C names
    fn get_filtered_from_clause_hierarchy_with_allowed_columns(
        &self, column_retrieval_options: ColumnRetrievalOptions
    ) -> impl Iterator<Item = (
        &FromContents, HashSet<IdentName>, HashSet<IdentName>, Option<HashSet<[IdentName; 2]>>
    )> {
        let mut shaded_rel_names: HashSet<IdentName> = HashSet::new();
        let mut shaded_col_names: HashSet<IdentName> = if column_retrieval_options.shade_by_select_aliases {
            HashSet::from_iter(self.query().get_all_select_aliases_iter().cloned())
        } else { HashSet::new() };
        self.get_clause_hierarchy_iter().enumerate().filter_map(
            |(i, v_opt)| v_opt.map(|v| (i, v))
        ).filter(move |(i, (.., query_props))| {
            let mut pass = if column_retrieval_options.only_columns_that_can_be_aggregated {
                // either we are in a caller query, where all aggregate-able columns,
                // or we are in a parent query that did not forbid aggregation
                *i == 0 || !query_props.get_current_subquery_call_options().no_aggregate
            } else { true };
            pass = pass && (
                *i == 0 || !query_props.get_current_subquery_call_options().no_column_spec
            );
            pass
        }).map(move |(
            i, (from_contents, group_by_contents, query_props)
        )| {
            let only_group_by_columns = if i == 0 { column_retrieval_options.only_group_by_columns } else {
                query_props.get_current_subquery_call_options().only_group_by_columns
            };
            let allowed_rel_col_names_opt = if only_group_by_columns {
                Some(from_contents.add_functionally_bound_columns(group_by_contents.get_column_name_set()))
            } else { None };
            let ret_srn = shaded_rel_names.clone();
            for rel_name in from_contents.get_all_rel_names_iter() {
                shaded_rel_names.insert((*rel_name).clone());
            }
            let ret_scn = shaded_col_names.clone();
            for col_name in from_contents.get_all_col_names_iter() {
                shaded_col_names.insert((*col_name).clone());
            }
            (from_contents, ret_srn, ret_scn, allowed_rel_col_names_opt)
        })
    }

    /// This checks the current and parent queries for columns:
    /// - are accessible by check_accessibility method, and
    /// - are able to be retrieved with column_retrieval_options\
    /// and retrieves them using the retrieve_by function.
    fn get_accessible_column_levels_iter_retrieve_by<'a, F>(
        &'a self, retrieve_by: F, check_accessibility: CheckAccessibility, column_retrieval_options: ColumnRetrievalOptions
    ) -> impl Iterator<Item = Vec<(&'a SubgraphType, [&'a IdentName; 2])>>
    where
        F: for<'b> Fn(&'b FromContents, Option<HashSet<[IdentName; 2]>>, bool) -> Box<dyn TypeAndQualifiedColumnIter<'b> + 'b>,
    {
        self.get_filtered_from_clause_hierarchy_with_allowed_columns(
            column_retrieval_options
        ).map(move |(
            from_contents, shaded_rel_names, shaded_col_names, allowed_rel_col_names_opt
        )| {
            let accessible_columns = [
                match &check_accessibility {
                    CheckAccessibility::QualifiedColumnName => vec![],
                    CheckAccessibility::ColumnName |
                    CheckAccessibility::Either => {
                        retrieve_by(from_contents, allowed_rel_col_names_opt.clone(), true).filter(
                            |(.., [.., col_name])| !shaded_col_names.contains(col_name)
                        ).collect::<Vec<_>>()
                    },
                },
                match &check_accessibility {
                    CheckAccessibility::ColumnName => vec![],
                    CheckAccessibility::QualifiedColumnName |
                    CheckAccessibility::Either => {
                        retrieve_by(from_contents, allowed_rel_col_names_opt, false).filter(
                            |(.., [rel_name, ..])| !shaded_rel_names.contains(rel_name)
                        ).collect::<Vec<_>>()
                    },
                }
            ].concat();
            accessible_columns
        })
    }

    /// This checks the current and parent queries for columns:
    /// - with a the type that falls into the column_types vec,
    /// - are accessible by check_accessibility method, and
    /// - are able to be retrieved with column_retrieval_options
    fn get_accessible_column_levels_by_types_iter(
        &self, column_types: Vec<SubgraphType>, check_accessibility: CheckAccessibility,
        column_retrieval_options: ColumnRetrievalOptions
    ) -> impl Iterator<Item = Vec<(&SubgraphType, [&IdentName; 2])>> {
        self.get_accessible_column_levels_iter_retrieve_by(
            move |fc, allowed_rel_col_names_opt, only_unique| {
                Box::new(fc.get_accessible_columns_by_types(
                    &column_types, allowed_rel_col_names_opt, only_unique
                ).into_iter())
            }, check_accessibility, column_retrieval_options
        )
    }

    /// This checks the current and parent queries for columns that:
    /// - are accessible by check_accessibility method, and
    /// - are able to be retrieved with column_retrieval_options
    fn get_accessible_column_levels_iter(
        &self, check_accessibility: CheckAccessibility, column_retrieval_options: ColumnRetrievalOptions
    ) -> impl Iterator<Item = Vec<(&SubgraphType, [&IdentName; 2])>> {
        self.get_accessible_column_levels_iter_retrieve_by(
            |fc, allowed_rel_col_names_opt, only_unique| {
                Box::new(fc.get_accessible_columns_iter(allowed_rel_col_names_opt, only_unique))
            }, check_accessibility, column_retrieval_options
        )
    }

    pub fn get_non_empty_column_levels_by_types(
        &self, column_types: Vec<SubgraphType>, check_accessibility: CheckAccessibility,
        column_retrieval_options: ColumnRetrievalOptions
    ) -> Vec<Vec<(&SubgraphType, [&IdentName; 2])>> {
        self.get_accessible_column_levels_by_types_iter(
            column_types, check_accessibility, column_retrieval_options
        ).filter(|col_vec| !col_vec.is_empty()).collect::<Vec<_>>()
    }

    pub fn is_type_available(&self, any_of: Vec<SubgraphType>, column_retrieval_options: ColumnRetrievalOptions) -> bool {
        self.get_accessible_column_levels_by_types_iter(
            any_of, CheckAccessibility::Either, column_retrieval_options
        ).find(|col_vec| !col_vec.is_empty()).is_some()
    }

    pub fn has_columns_for_types(
        &self, column_types: Vec<SubgraphType>, check_accessibility: CheckAccessibility, column_retrieval_options: ColumnRetrievalOptions
    ) -> bool {
        self.get_accessible_column_levels_by_types_iter(
            column_types, check_accessibility, column_retrieval_options
        ).find(|col_vec| !col_vec.is_empty()).is_some()
    }

    pub fn has_columns(&self, check_accessibility: CheckAccessibility, column_retrieval_options: ColumnRetrievalOptions) -> bool {
        self.get_accessible_column_levels_iter(
            check_accessibility, column_retrieval_options
        ).find(|col_vec| !col_vec.is_empty()).is_some()
    }

    pub fn retrieve_column_by_ident_components(
        &self, ident_components: &Vec<Ident>, column_retrieval_options: ColumnRetrievalOptions
    ) -> Result<(SubgraphType, [IdentName; 2]), ColumnRetrievalError> {
        let (
            check_accessibility, searched_rel, searched_col
        ): (_, std::option::Option<IdentName>, IdentName) = match ident_components.as_slice() {
            [col_ident] => {
                (CheckAccessibility::ColumnName, None, col_ident.clone().into())
            },
            [rel_ident, col_ident] => {
                (CheckAccessibility::QualifiedColumnName, Some(rel_ident.clone().into()), col_ident.clone().into())
            }
            any => return Err(ColumnRetrievalError::new(format!(
                "Can't access column. Name requires 1 or 2 elements, got: {:?}", any
            ))),
        };
        let rel_col_tp_opt = self.get_accessible_column_levels_iter(
            check_accessibility.clone(), column_retrieval_options.clone()
        ).find_map(
            |cols_and_types| cols_and_types.into_iter()
                .find_map(|(tp, [rel_name, col_name])|
                    if (
                        *col_name == searched_col || (
                            column_retrieval_options.try_with_quotes &&
                            *col_name == searched_col.clone().with_double_quotes()
                        )
                    ) && searched_rel.as_ref().map(
                        |s_rel| s_rel == rel_name
                    ).unwrap_or(true) {
                        Some((tp.clone(), [rel_name.clone(), col_name.clone()]))
                    } else { None }
                )
        );
        match rel_col_tp_opt {
            Some(val) => Ok(val),
            None => Err(ColumnRetrievalError::new(format!(
                "Couldn't find column named {}.\nAccessible columns: {:#?}",
                    ObjectName(ident_components.clone()), self.get_accessible_column_levels_iter(
                        check_accessibility, column_retrieval_options
                    ).collect::<Vec<_>>(),
            ))),
        }
    }

    pub fn retrieve_column_by_column_expr(
        &self, column_expr: &Expr, column_retrieval_options: ColumnRetrievalOptions
    ) -> Result<(SubgraphType, [IdentName; 2]), ColumnRetrievalError> {
        let ident_components = match column_expr {
            Expr::Identifier(ident) => vec![ident.clone()],
            Expr::CompoundIdentifier(ident_components) => ident_components.clone(),
            any => panic!("Unexpected expression for GROUP BY column: {:#?}", any),
        };
        self.retrieve_column_by_ident_components(&ident_components, column_retrieval_options)
    }

    pub fn add_from_table_by_name(&mut self, name: &ObjectName, alias: Option<TableAlias>) -> Result<(), TableRetrievalError> {
        self.all_accessible_froms.append_table(
            self.schema_ref().get_table_def_by_name(name)?.clone(),
            alias
        );
        Ok(())
    }

    pub fn get_unused_table_names(&self) -> Vec<ObjectName> {
        let used_rel_names = self.all_accessible_froms.top_from().get_all_rel_names_iter().collect::<HashSet<_>>();
        self.schema_ref().get_all_table_names().into_iter()
            .filter(|table_name| !used_rel_names.contains::<IdentName>(&(**table_name).0[0].clone().into()))
            .cloned().collect()
    }
}

#[derive(Debug, Clone)]
pub struct AllAccessibleFroms {
    from_is_active_stack: Vec<bool>,
    from_contents_stack: Vec<FromContents>,
    subfrom_opt_stack: Vec<Option<FromContents>>,
    unactive_subfrom_opt_stack: Vec<Option<FromContents>>,
}

#[derive(Debug, Clone)]
pub struct AllAccessibleFromsCutoffCheckpoint {
    stack_length: usize,
    from_is_active_last: bool,
    from_contents_last: FromContents,
    subfrom_opt_last: Option<FromContents>,
    unactive_subfrom_opt_last: Option<FromContents>,
}

impl AllAccessibleFroms {
    fn new() -> Self {
        Self {
            from_is_active_stack: vec![],
            from_contents_stack: vec![],
            subfrom_opt_stack: vec![],
            unactive_subfrom_opt_stack: vec![],
        }
    }

    fn append_table(&mut self, create_table_st: CreateTableSt, alias: Option<TableAlias>) {
        let (
            alias, column_renames
        ) = alias.map(|x| (
            (x.name.value, x.columns)
        )).unwrap_or((create_table_st.name.0[0].value.clone(), vec![]));
        self.top_from_mut().append_table(create_table_st.clone(), alias.clone(), column_renames.clone());
        if let Some(subfrom) = self.current_subfrom_opt_mut() {
            subfrom.append_table(create_table_st, alias, column_renames);
        }
    }

    pub fn append_query(&mut self, column_idents_and_graph_types: Vec<(Option<IdentName>, SubgraphType)>, alias: TableAlias) {
        let (alias, column_renames) = (alias.name.value, alias.columns);
        self.top_from_mut().append_query(column_idents_and_graph_types.clone(), alias.clone(), column_renames.clone());
        if let Some(subfrom) = self.current_subfrom_opt_mut() {
            subfrom.append_query(column_idents_and_graph_types, alias, column_renames.clone());
        }
    }

    pub fn activate_from(&mut self) {
        *self.from_is_active_stack.last_mut().unwrap() = true;
    }

    pub fn add_subfrom(&mut self) {
        *self.current_subfrom_opt_mut() = Some(FromContents::new());
    }

    pub fn activate_subfrom(&mut self) {
        *self.subfrom_opt_stack.last_mut().unwrap() = self.unactive_subfrom_opt_stack.last_mut().unwrap().take();
    }

    pub fn deactivate_subfrom(&mut self) {
        *self.unactive_subfrom_opt_stack.last_mut().unwrap() = self.subfrom_opt_stack.last_mut().unwrap().take();
    }

    pub fn delete_subfrom(&mut self) {
        // subfrom should be deactivated here
        self.current_subfrom_opt_mut().take().unwrap();
    }

    fn get_relation_by_name(&self, name: &IdentName) -> &Relation {
        self.get_from_contents_hierarchy_iter().filter_map(|fc| fc)
            .find_map(|fc| fc.get_relation_by_name(name)).unwrap()
    }

    fn get_cutoff_checkpoint(&self) -> AllAccessibleFromsCutoffCheckpoint {
        AllAccessibleFromsCutoffCheckpoint {
            stack_length: self.from_contents_stack.len(),
            from_contents_last: self.from_contents_stack.last().unwrap().clone(),
            from_is_active_last: *self.from_is_active_stack.last().unwrap(),
            subfrom_opt_last: self.subfrom_opt_stack.last().unwrap().clone(),
            unactive_subfrom_opt_last: self.unactive_subfrom_opt_stack.last().unwrap().clone(),
        }
    }

    fn restore_from_cutoff_checkpoint(&mut self, checkpoint: AllAccessibleFromsCutoffCheckpoint) {
        *self.from_is_active_stack.last_mut().unwrap() = checkpoint.from_is_active_last;
        self.from_contents_stack.truncate(checkpoint.stack_length);
        *self.from_contents_stack.last_mut().unwrap() = checkpoint.from_contents_last;
        self.subfrom_opt_stack.truncate(checkpoint.stack_length);
        *self.subfrom_opt_stack.last_mut().unwrap() = checkpoint.subfrom_opt_last;
        self.unactive_subfrom_opt_stack.truncate(checkpoint.stack_length);
        *self.unactive_subfrom_opt_stack.last_mut().unwrap() = checkpoint.unactive_subfrom_opt_last;
    }

    fn current_subfrom_opt_mut(&mut self) -> &mut Option<FromContents> {
        assert!(self.subfrom_opt_stack.last().unwrap().is_none());
        self.unactive_subfrom_opt_stack.last_mut().unwrap()
    }

    fn current_subfrom_opt(&self) -> &Option<FromContents> {
        assert!(self.unactive_subfrom_opt_stack.last().unwrap().is_none());
        self.subfrom_opt_stack.last().unwrap()
    }

    /// returns an iterator of FromContents that first contains current subfrom, then
    /// the top from, then all the other froms from bottom query level to top query level
    fn get_from_contents_hierarchy_iter(&self) -> impl Iterator<Item = Option<&FromContents>> {
        self.from_contents_stack.iter().rev()
            .zip(self.from_is_active_stack.iter().rev())
            .zip(self.subfrom_opt_stack.iter().rev())
            .map(|((from_contents, from_is_active), subfrom_opt)|
                subfrom_opt.as_ref().or(
                    if *from_is_active { Some(from_contents) } else { None }
                )
            )
    }

    fn top_active_from(&self) -> &FromContents {
        self.current_subfrom_opt().as_ref().unwrap_or(self.top_from())
    }

    fn top_from(&self) -> &FromContents {
        self.from_contents_stack.last().unwrap()
    }

    fn top_from_mut(&mut self) -> &mut FromContents {
        self.from_contents_stack.last_mut().unwrap()
    }

    fn on_query_begin(&mut self) {
        self.subfrom_opt_stack.push(None);
        self.unactive_subfrom_opt_stack.push(None);
        self.from_is_active_stack.push(false);
        self.from_contents_stack.push(FromContents::new());
    }

    fn on_query_end(&mut self) {
        self.subfrom_opt_stack.pop();
        self.unactive_subfrom_opt_stack.pop();
        self.from_is_active_stack.pop();
        self.from_contents_stack.pop();
    }
}

#[derive(Debug, Clone)]
struct SubqueryCallOptions {
    only_group_by_columns: bool,
    no_aggregate: bool,
    no_column_spec: bool,
}

#[derive(Debug, Clone)]
pub struct QueryProps {
    distinct: bool,
    is_aggregation_indicated: bool,
    column_idents_and_graph_types: Option<Vec<(Option<IdentName>, SubgraphType)>>,
    current_subquery_call_options: Option<SubqueryCallOptions>,
}

impl QueryProps {
    fn new() -> QueryProps {
        Self {
            distinct: false,
            is_aggregation_indicated: false,
            column_idents_and_graph_types: None,
            current_subquery_call_options: None,
        }
    }

    fn get_current_subquery_call_options(&self) -> &SubqueryCallOptions {
        self.current_subquery_call_options.as_ref().unwrap()
    }

    fn set_subquery_call_mods(&mut self, subquery_call_mods: &CallModifiers) {
        if *subquery_call_mods == CallModifiers::None {
            self.current_subquery_call_options = Some(SubqueryCallOptions {
                only_group_by_columns: false,
                no_aggregate: false,
                no_column_spec: false,
            });
        } else {
            let mods = unwrap_variant!(subquery_call_mods, CallModifiers::StaticList);
            self.current_subquery_call_options = Some(SubqueryCallOptions {
                only_group_by_columns: mods.contains(&SmolStr::new("group by columns")),
                no_aggregate: mods.contains(&SmolStr::new("no aggregate")),
                no_column_spec: mods.contains(&SmolStr::new("no column spec")),
            })
        }
    }

    fn unset_subquery_call_mods(&mut self) {
        self.current_subquery_call_options.take();
    }

    pub fn is_distinct(&self) -> bool {
        self.distinct
    }

    pub fn set_distinct(&mut self) {
        self.distinct = true;
    }

    pub fn set_select_type(&mut self, column_idents_and_graph_types: Vec<(Option<IdentName>, SubgraphType)>) {
        self.column_idents_and_graph_types = Some(column_idents_and_graph_types);
    }

    pub fn create_select_type(&mut self) {
        self.column_idents_and_graph_types = Some(vec![]);
    }

    pub fn select_type_mut(&mut self) -> &mut Vec<(Option<IdentName>, SubgraphType)> {
        self.column_idents_and_graph_types.as_mut().unwrap()
    }

    pub fn select_type(&self) -> &Vec<(Option<IdentName>, SubgraphType)> {
        &self.column_idents_and_graph_types.as_ref().unwrap()
    }

    pub fn get_all_select_aliases_iter(&self) -> impl Iterator<Item = &IdentName> {
        self.select_type().iter().filter_map(|(alias, ..)| alias.as_ref())
    }

    pub fn pop_output_type(&mut self) -> Vec<(Option<IdentName>, SubgraphType)> {
        let mut column_idents_and_graph_types = self.column_idents_and_graph_types.take().unwrap();
        // select pg_typeof((select null)); -- returns text
        for (_, column_type) in column_idents_and_graph_types.iter_mut() {
            if *column_type == SubgraphType::Undetermined {
                *column_type = SubgraphType::Text;
            }
        }
        column_idents_and_graph_types
    }

    pub fn extract_alias(expr: &Expr) -> Option<IdentName> {
        match &expr.unnested() {
            Expr::Cast { expr, data_type, format: _ } if matches!(**expr, Expr::Value(..)) || matches!(
                &**expr, Expr::UnaryOp { op, expr: inner_expr } if *op == UnaryOperator::Minus && matches!(**inner_expr, Expr::Value(..))
            ) => {
                Some(Ident {
                    value: match data_type {
                        DataType::Timestamp(_, tz) if *tz == TimezoneInfo::None => "timestamp",
                        DataType::Numeric(_) => "numeric",
                        DataType::Integer(_) => "int4",
                        DataType::Interval => "interval",
                        DataType::BigInt(_) => "int8",
                        DataType::Boolean => "bool",
                        DataType::Text => "text",
                        DataType::Date => "date",
                        _ => unimplemented!(),
                    }.to_string(),
                    quote_style: Some('"')
                })
            },
            Expr::Identifier(ident) => Some(ident.clone()),
            Expr::CompoundIdentifier(idents) => Some(idents.last().unwrap().clone()),
            // unnnamed aggregation can be referred to by function name in postgres
            Expr::Function(func) => Some(func.name.0.last().unwrap().clone()),
            Expr::Case { operand: _, conditions: _, results: _, else_result } => {
                else_result.as_ref()
                    .and_then(|else_expr| if matches!(
                        else_expr.unnested(), Expr::Value(sqlparser::ast::Value::Boolean(_))
                    ) || matches!(
                        else_expr.unnested(), Expr::TypedString { data_type, value: _ } if [
                            DataType::Date, DataType::Interval, DataType::Timestamp(None, TimezoneInfo::None)
                        ].contains(data_type)
                    ) || matches!(
                        else_expr.unnested(), Expr::Cast { expr, data_type: _, format: _ } if matches!(**expr, Expr::Value(..)) || matches!(
                            &**expr, Expr::UnaryOp { op, expr: inner_expr } if *op == UnaryOperator::Minus && matches!(**inner_expr, Expr::Value(..))
                        )
                    ) || matches!(
                        else_expr.unnested(), Expr::Interval { .. }
                    ) { None } else {
                        Self::extract_alias(else_expr).map(IdentName::into)
                    })
                    .or(
                        if else_result.as_ref().map_or(
                            false, |x|
                                matches!(*x.unnested(), Expr::Subquery(..)) ||
                                matches!(*x.unnested(), Expr::Case { .. })
                        ) { None } else {
                            Some(Ident { value: "case".to_string(), quote_style: Some('"') })
                        }
                    )
            },
            Expr::Value(value) => {
                let value = match value {
                    sqlparser::ast::Value::Boolean(_) => Some("bool".to_string()),
                    sqlparser::ast::Value::Number(_, _) => None,
                    sqlparser::ast::Value::SingleQuotedString(_) => None,
                    sqlparser::ast::Value::Null => None,
                    any => panic!("Unexpected value type: {:?}", any),
                };
                value.map(|value| Ident { value, quote_style: Some('"') })
            },
            Expr::TypedString {
                data_type, value: _,
            } if [
                DataType::Date,
                DataType::Interval,
                DataType::Timestamp(None, TimezoneInfo::None),
            ].contains(data_type) => {
                let s = match *data_type {
                    DataType::Date => "date",
                    DataType::Interval => "interval",
                    DataType::Timestamp(..) => "timestamp",
                    _ => unreachable!(),
                };
                Some(Ident { value: s.to_string(), quote_style: Some('"') })
            },
            Expr::Interval { .. } => {
                Some(Ident { value: "interval".to_string(), quote_style: Some('"') })
            },
            Expr::Subquery(query) => {
                let select_body = unwrap_variant!(&*query.body, SetExpr::Select);
                match select_body.projection.as_slice() {
                    &[SelectItem::UnnamedExpr(ref expr)] => Self::extract_alias(expr).map(IdentName::into),
                    &[SelectItem::ExprWithAlias { expr: _, ref alias }] => Some(alias.clone().into()),
                    _ => None,
                }
            },
            Expr::Trim { expr: _, trim_where, trim_what: _, trim_characters: _ } => {
                let func_name = if let Some(trim_where) = trim_where {
                    match trim_where {
                        sqlparser::ast::TrimWhereField::Both => "btrim".to_string(),
                        sqlparser::ast::TrimWhereField::Leading => "ltrim".to_string(),
                        sqlparser::ast::TrimWhereField::Trailing => "rtrim".to_string(),
                    }
                } else {
                    "btrim".to_string()
                };
                Some(Ident { value: func_name, quote_style: None })
            },
            Expr::Position { expr: _, r#in: _ } => Some(Ident { value: "position".to_string(), quote_style: Some('"') }),
            Expr::Substring { expr: _, substring_from: _, substring_for: _, special: _ } => Some(Ident { value: "substring".to_string(), quote_style: Some('"') }),
            Expr::Extract { field: _, expr: _ } => Some(Ident { value: "extract".to_string(), quote_style: Some('"') }),
            Expr::Exists { subquery: _, negated } => {
                if *negated { None } else {
                    Some(Ident { value: "exists".to_string(), quote_style: Some('"') })
                }
            },
            _ => None,
        }.map(Ident::into)
    }

    pub fn is_aggregation_indicated(&self) -> bool {
        self.is_aggregation_indicated
    }

    pub fn set_aggregation_indicated(&mut self) {
        self.is_aggregation_indicated = true;
    }
}

#[derive(Debug, Clone)]
struct ColumnContainer<ColumnNameType: Ord + Clone + Hash> {
    /// A BTreeMap (ordered for consistency) with column
    /// names by graph types
    pub columns: BTreeMap<SubgraphType, BTreeSet<ColumnNameType>>,
}

impl<ColumnNameType: Ord + Clone + Hash + std::fmt::Debug> ColumnContainer<ColumnNameType> {
    fn new() -> Self {
        Self {
            columns: BTreeMap::new(),
        }
    }

    fn append_column(&mut self, column_name: ColumnNameType, graph_type: SubgraphType) {
        self.columns.entry(graph_type)
            .and_modify(|v| { v.insert(column_name.clone()); })
            .or_insert(BTreeSet::from([column_name.clone()]));
    }

    fn get_column_names_iter(&self) -> impl Iterator<Item = &ColumnNameType> {
        self.columns.values().flat_map(|x| x.iter())
    }

    fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }
}

#[derive(Debug, Clone)]
pub struct GroupByContents {
    /// A container of columns accessible by names
    columns: ColumnContainer<[IdentName; 2]>,
    /// Whether a catch-all single group is used
    /// In Postgres, this is either GROUP BY () or aggregation without group by
    single_group_grouping: bool,
    /// whether single group grouping is also single row
    single_row_grouping: bool,
}

impl GroupByContents {
    fn new() -> GroupByContents {
        GroupByContents {
            columns: ColumnContainer::new(),
            single_group_grouping: false,
            single_row_grouping: false,
        }
    }

    pub fn append_column(&mut self, column_name: [IdentName; 2], graph_type: SubgraphType) {
        self.columns.append_column(column_name, graph_type)
    }

    pub fn contains_columns(&self) -> bool {
        !self.columns.is_empty()
    }

    /// returns whether the groupping is enabled
    pub fn is_grouping_active(&self) -> bool {
        return self.is_single_group() || !self.columns.is_empty()
    }

    pub fn set_single_group_grouping(&mut self) {
        self.single_group_grouping = true;
    }

    pub fn is_single_group(&self) -> bool {
        return self.single_group_grouping
    }

    pub fn set_single_row_grouping(&mut self) {
        self.single_row_grouping = true;
    }

    pub fn is_single_row(&self) -> bool {
        return self.single_row_grouping
    }

    fn get_column_name_set(&self) -> HashSet<[IdentName; 2]> {
        HashSet::from_iter(self.columns.get_column_names_iter().map(
            |idents| idents.clone()
        ))
    }
}

/// An Ident wrapper that only compares/hashes
/// the lowercase string, unless the quote style
/// is ", then it compares/hashes original value
/// in quotes (Abc for "Abc"), without quotes
/// themselves
#[derive(Clone, Debug, Eq)]
pub struct IdentName {
    ident: Ident
}

impl IdentName {
    pub fn with_double_quotes(mut self) -> Self {
        self.ident.quote_style = Some('"');
        self
    }
}

impl fmt::Display for IdentName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.ident.quote_style != Some('"') {
            write!(f, "{}", self.ident.value.to_lowercase())
        } else {
            write!(f, "{}", self.ident.value)
        }
    }
}

impl PartialEq for IdentName {
    fn eq(&self, other: &Self) -> bool {
        self.to_string() == other.to_string()
    }
}

impl Ord for IdentName {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.to_string().cmp(&other.to_string())
    }
}

impl PartialOrd for IdentName {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.to_string().partial_cmp(&other.to_string())
    }
}

impl Hash for IdentName {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.to_string().hash(state);
    }
}

impl From<Ident> for IdentName {
    fn from(ident: Ident) -> Self {
        Self { ident }
    }
}

impl From<IdentName> for Ident {
    fn from(ident_name: IdentName) -> Self {
        ident_name.ident
    }
}

#[derive(Debug, Clone)]
pub struct FromContents {
    /// maps aliases to relations themselves
    /// This is ordered because we need consistent
    /// same-seed runs
    relations: BTreeMap<IdentName, Relation>,
    /// order of relations (relevant for wildcard)
    relation_name_order: Vec<IdentName>,
}

impl FromContents {
    pub fn new() -> Self {
        Self { relations: BTreeMap::new(), relation_name_order: vec![] }
    }

    fn append_table(&mut self, create_table_st: CreateTableSt, alias: String, column_renames: Vec<Ident>) {
        let relation = Relation::from_table(alias, create_table_st, column_renames);
        self.relation_name_order.push(relation.alias.clone());
        self.relations.insert(relation.alias.clone(), relation);
    }

    fn append_query(&mut self, column_idents_and_graph_types: Vec<(Option<IdentName>, SubgraphType)>, alias: String, column_renames: Vec<Ident>) {
        let relation = Relation::from_query(alias, column_idents_and_graph_types, column_renames);
        self.relation_name_order.push(relation.alias.clone());
        self.relations.insert(relation.alias.clone(), relation);
    }

    /// get all the columns represented in only a single relation in FROM
    fn get_unique_columns(&self) -> HashSet<[IdentName; 2]> {
        self.relations.iter()
            .flat_map(|(_, rel)| rel.get_all_column_names_iter().cloned().map(
                |col_name| [rel.alias.clone(), col_name]
            ))
            .fold(HashMap::new(), |mut acc, [rel_name, col_name]| {
                acc.entry(col_name).or_insert(vec![]).push(rel_name);
                acc
            }).into_iter()
            .filter(|(_, rels)| rels.len() == 1)
            .map(|(col_name, rels)| [rels.into_iter().next().unwrap(), col_name])
            .collect()
    }

    // does not add functionally related columns from relations that are higher up
    fn add_functionally_bound_columns(&self, columns: HashSet<[IdentName; 2]>) -> HashSet<[IdentName; 2]> {
        columns.into_iter().flat_map(|rel_col| {
            let it: Box<dyn Iterator<Item = [IdentName; 2]>> = match self.relations.get(&rel_col[0]) {
                Some(relation) => Box::new(
                    relation.get_functionally_connected_columns(&rel_col[1])
                    .into_iter().map(move |col| [rel_col[0].clone(), col])
                ),
                None => Box::new([rel_col].into_iter()),
            };
            it
        }).collect()
    }

    /// returns an accessible columns iterator\
    /// - if allowed_col_names_opt is not None, checks that the columns name is in the set
    /// - if only_unique is true, only columns with unique names for this FROM clause are returned
    fn get_accessible_columns_iter(
        &self, allowed_rel_col_names_opt: Option<HashSet<[IdentName; 2]>>, only_unique: bool
    ) -> impl Iterator<Item = (&SubgraphType, [&IdentName; 2])> {
        let (
            allowed_col_names_opt, allowed_rel_col_names_opt
        ) = if only_unique {
            let mut allowed_columns = self.get_unique_columns();
            if let Some(allowed_rel_col_names) = allowed_rel_col_names_opt {
                allowed_columns = allowed_columns.intersection(&allowed_rel_col_names).cloned().collect()
            }
            (Some(allowed_columns.into_iter().map(
                |x| x.into_iter().last().unwrap()
            ).collect::<HashSet<_>>()), None)
        } else {
            (None, allowed_rel_col_names_opt)
        };

        self.relations.iter()
            .flat_map(|(_, rel)| rel.get_accessible_columns_iter().map(
                |(col_ident, col_tp)| (
                    col_tp, [&rel.alias, col_ident]
                )
            ))
            .filter(move |(.., [.., col_name])| if let Some(allowed_col_names) = &allowed_col_names_opt {
                allowed_col_names.contains(*col_name)
            } else { true })
            .filter(move |(.., [rel_name, col_name])| if let Some(allowed_rel_col_names) = &allowed_rel_col_names_opt {
                allowed_rel_col_names.contains(&[(*rel_name).clone(), (*col_name).clone()])
            } else { true })
    }

    fn get_all_rel_names_iter(&self) -> impl Iterator<Item = &IdentName> {
        self.relations.iter().map(|(rel_name, ..)| rel_name)
    }

    fn get_all_col_names_iter(&self) -> impl Iterator<Item = &IdentName> {
        self.relations.iter().flat_map(|(.., relation)| relation.get_all_column_names_iter())
    }

    /// returns accessible columns which types are in the column_types vec\
    /// - if allowed_col_names_opt is not None, checks that the columns name is in the set
    /// - if only_unique is true, only columns with unique names for this FROM clause are returned
    fn get_accessible_columns_by_types(
        &self, column_types: &Vec<SubgraphType>, allowed_rel_col_names_opt: Option<HashSet<[IdentName; 2]>>, only_unique: bool
    ) -> Vec<(&SubgraphType, [&IdentName; 2])> {
        self.get_accessible_columns_iter(allowed_rel_col_names_opt, only_unique)
            .filter(|(col_tp, ..)| column_types.contains_generator_of(*col_tp)).collect()
    }

    fn get_relation_by_name(&self, name: &IdentName) -> Option<&Relation> {
        self.relations.iter().find( // why is this not .get ? todo: change to .get
            |(rl_name, _)| *rl_name == name
        ).map(|(.., r)| r)
    }

    /// returns column names in the order that their respective relations are listed
    pub fn get_wildcard_columns_iter(&self) -> impl Iterator<Item = (Option<IdentName>, SubgraphType)> + '_ {
        self.ordered_relations_iter().flat_map(|(_, relation)| {
            relation.get_wildcard_columns_iter()
        })
    }

    pub fn get_n_wildcard_columns(&self) -> usize {
        self.relations.iter().map(|(_, rel)| {
            rel.get_n_wildcard_columns()
        }).sum()
    }

    /// returns relations in the order that they were listed (used for wildcard)
    fn ordered_relations_iter(&self) -> impl Iterator<Item = (&IdentName, &Relation)> {
        self.relation_name_order.iter().map(|rel_name| {
            self.relations.get_key_value(rel_name).unwrap()
        })
    }

    pub fn num_relations(&self) -> usize {
        self.relations.len()
    }

    fn relations_iter(&self) -> impl Iterator<Item = (&IdentName, &Relation)> {
        self.relations.iter()
    }
}

#[derive(Debug, Clone)]
pub struct Relation {
    /// projection alias
    alias: IdentName,
    /// A container of columns accessible by names
    accessible_columns: ColumnContainer<IdentName>,
    /// A list of unnamed column's types, as they
    /// can be referenced with wildcards
    unnamed_columns: Vec<SubgraphType>,
    /// A list of ambiguous column names and types,
    /// as they can be referenced with wildcards
    ambiguous_columns: Vec<(IdentName, SubgraphType)>,
    /// this is what is returned by get_wildcard_columns
    /// includes unnamed, ambiguous, and standard columns
    wildcard_columns: Vec<(Option<IdentName>, SubgraphType)>,
    /// optionally, a create table command used for this
    /// relation (only for tables)
    create_table: Option<CreateTableSt>,
}

impl Relation {
    fn with_alias(alias: String) -> Self {
        Self {
            alias: Ident::new(alias).into(),
            accessible_columns: ColumnContainer::new(),
            unnamed_columns: vec![],
            ambiguous_columns: vec![],
            wildcard_columns: vec![],
            create_table: None,
        }
    }

    fn from_table(alias: String, create_table: CreateTableSt, column_renames: Vec<Ident>) -> Self {
        let mut _self = Relation::with_alias(alias);
        let mut column_renames = column_renames.into_iter();
        for column in create_table.columns.iter() {
            let graph_type = SubgraphType::from_data_type(&column.data_type);
            let column_rename = column_renames.next();
            let column_name: IdentName = column_rename.unwrap_or(column.name.clone()).into();
            _self.wildcard_columns.push((Some(column_name.clone()), graph_type.clone()));
            _self.accessible_columns.append_column(column_name, graph_type);
        }
        _self.create_table = Some(create_table);
        _self
    }

    /// always returns at least to_column itself
    fn get_functionally_connected_columns(&self, to_column: &IdentName) -> Vec<IdentName> {
        match self.create_table.as_ref() {
            Some(create_table) => create_table.get_functionally_connected_columns(to_column),
            None => vec![to_column.clone()],
        }
    }

    fn from_query(alias: String, column_idents_and_graph_types: Vec<(Option<IdentName>, SubgraphType)>, column_renames: Vec<Ident>) -> Self {
        let mut _self = Relation::with_alias(alias);
        let mut named_columns = vec![];
        let mut column_renames = column_renames.into_iter();
        for (column_name, graph_type) in column_idents_and_graph_types {
            let column_rename = column_renames.next().map(Ident::into);
            if let Some(column_name) = column_rename.or(column_name) {
                _self.wildcard_columns.push((Some(column_name.clone()), graph_type.clone()));
                named_columns.push((column_name, graph_type));
            } else {
                _self.wildcard_columns.push((None, graph_type.clone()));
                _self.unnamed_columns.push(graph_type);
            }
        }
        let mut names_to_types = BTreeMap::new();
        for (column_name, graph_type) in named_columns {
            names_to_types.entry(column_name).or_insert(vec![]).push(graph_type);
        }
        for (column_name, graph_types) in names_to_types {
            match graph_types.as_slice() {
                &[ref graph_type] => _self.accessible_columns.append_column(column_name, graph_type.clone()),
                other => {
                    _self.ambiguous_columns.extend(other.into_iter().map(|x| (column_name.clone(), x.clone())))
                }
            }
        }
        _self
    }

    fn get_accessible_columns_iter(&self) -> impl Iterator<Item = (&IdentName, &SubgraphType)> {
        self.accessible_columns.columns.iter()
            .flat_map(|(graph_type, column_names)| column_names.iter().map(
                move |column_name| (column_name, graph_type)
            ))
    }

    /// Returns accessible and ambiguous column names
    fn get_all_column_names_iter(&self) -> impl Iterator<Item = &IdentName> {
        self.accessible_columns.get_column_names_iter()
            .chain(self.ambiguous_columns.iter().map(|(x, ..)| x))
    }

    /// get all columns with their types, including the unnamed ones and ambiguous ones
    fn get_all_columns_iter(&self) -> impl Iterator<Item = (Option<&IdentName>, &SubgraphType)> {
        self.get_accessible_columns_iter()
            .map(|(col_ident, col_tp)| (Some(col_ident), col_tp))
            .chain(self.unnamed_columns.iter().map(|x| (None, x)))
            .chain(self.ambiguous_columns.iter().map(|(column_name, graph_type)| (
                Some(column_name), graph_type
            )))
    }

    /// get all columns with their types, including the unnamed ones and ambiguous ones
    pub fn get_wildcard_columns_iter(&self) -> impl Iterator<Item = (Option<IdentName>, SubgraphType)> + '_ {
        self.wildcard_columns.iter().cloned()
    }

    pub fn get_n_wildcard_columns(&self) -> usize {
        self.wildcard_columns.len()
    }
}
