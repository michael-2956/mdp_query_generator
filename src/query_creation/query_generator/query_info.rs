use std::{collections::{BTreeMap, BTreeSet, HashMap, HashSet}, error::Error, fmt, hash::Hash, path::Path};

use rand::Rng;
use rand_chacha::ChaCha8Rng;
use smol_str::SmolStr;
use crate::{query_creation::state_generator::{markov_chain_generator::markov_chain::CallModifiers, subgraph_type::SubgraphType}, unwrap_variant};

use sqlparser::{ast::{ColumnDef, DataType, Expr, FileFormat, HiveDistributionStyle, HiveFormat, Ident, ObjectName, OnCommit, Query, SelectItem, SetExpr, SqlOption, Statement, TableAlias, TableConstraint}, dialect::PostgreSqlDialect, parser::Parser};

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
    default_charset: Option<String>,
    collation: Option<String>,
    on_commit: Option<OnCommit>,
    on_cluster: Option<String>,
});

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
        let dialect = PostgreSqlDialect {};
        let ast = match Parser::parse_sql(&dialect, source.as_str()) {
            Ok(ast) => ast,
            Err(err) => {
                println!("Schema file parsing error! {}", err);
                panic!();
            },
        };
        Self {
            table_defs: ast.into_iter().map(|x| x.try_into().unwrap()).collect()
        }
    }

    pub fn get_random_table_def(&self, rng: &mut ChaCha8Rng) -> &CreateTableSt {
        &self.table_defs[rng.gen_range(0..self.table_defs.len())]
    }

    pub fn get_table_def_by_name(&self, name: &ObjectName) -> &CreateTableSt {
        match self.table_defs.iter().find(
            |x| x.name.to_string().to_uppercase() == name.to_string().to_uppercase()
        ) {
            Some(create_table_st) => create_table_st,
            None => panic!("Couldn't find table {name} in the schema!"),
        }
    }
}

/// Contains all the structures responsible for the generation context while query is being generated.
/// For example, which fields were mentioned in FROM or in GROUP BY clauses. This structure is used in
/// call modifiers to disable nodes.
#[derive(Debug, Clone)]
pub struct ClauseContext {
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

trait TypeAndQualifiedColumnIter<'a>: Iterator<Item = (&'a SubgraphType, [&'a IdentName; 2])> {}
impl<'a, T: Iterator<Item = (&'a SubgraphType, [&'a IdentName; 2])> + 'a> TypeAndQualifiedColumnIter<'a> for T {}

impl ClauseContext {
    pub fn new() -> Self {
        Self {
            query_props_stack: vec![],
            all_accessible_froms: AllAccessibleFroms::new(),
            group_by_contents_stack: vec![],
        }
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

    pub fn from(&self) -> &FromContents {
        self.all_accessible_froms.active_from()
    }

    pub fn from_mut(&mut self) -> &mut AllAccessibleFroms {
        &mut self.all_accessible_froms
    }

    pub fn group_by(&self) -> &GroupByContents {
        self.group_by_contents_stack.last().unwrap()
    }

    pub fn group_by_mut(&mut self) -> &mut GroupByContents {
        self.group_by_contents_stack.last_mut().unwrap()
    }

    fn get_clause_hierarchy_iter(&self) -> impl Iterator<Item=(&FromContents, &GroupByContents, &QueryProps)> {
        self.all_accessible_froms.get_from_contents_hierarchy_iter()
            .zip(self.group_by_contents_stack.iter().rev())
            .zip(self.query_props_stack.iter().rev())
            .map(|((a, b), c)| (a, b, c))
    }

    fn get_accessible_column_levels_iter_retrieve_by<'a, F>(
        &'a self, retrieve_by: F, only_group_by_columns: bool, only_unique_column_names: bool, 
    ) -> impl Iterator<Item = Vec<(&'a SubgraphType, [&'a IdentName; 2])>>
    where
        F: for<'b> Fn(&'b FromContents, Option<HashSet<[IdentName; 2]>>, bool) -> Box<dyn TypeAndQualifiedColumnIter<'b> + 'b>,
    {
        let mut used_col_names: HashSet<IdentName> = HashSet::new();
        let mut used_rel_col_names: HashSet<[IdentName; 2]> = HashSet::new();
        self.get_clause_hierarchy_iter().enumerate().map(move |(
            i, (from_contents, group_by_contents, query_props)
        )| {
            let only_group_by_columns = if i == 0 { only_group_by_columns } else {
                query_props.get_current_subquery_call_options().only_group_by_columns
            };
            let allowed_rel_col_names_opt = if only_group_by_columns {
                Some(group_by_contents.get_column_name_set())
            } else { None };
            let accessible_columns = retrieve_by(from_contents, allowed_rel_col_names_opt, only_unique_column_names)
                // check is the R.C or C (if unique column names are required)
                // are unique across the subquery levels (which is required for them)
                // to be accessible
                .filter(|(.., [rel_name, col_name])| {
                    !used_rel_col_names.contains(&[(*rel_name).clone(), (*col_name).clone()]) &&
                    if only_unique_column_names { !used_col_names.contains(col_name) } else { true }
                })
                .collect::<Vec<_>>();
            for (.., [rel_name, col_name]) in accessible_columns.iter() {
                // add the R.C and C to their respective
                used_rel_col_names.insert([(*rel_name).clone(), (*col_name).clone()]);
                if only_unique_column_names { used_col_names.insert((*col_name).clone()); }
            }
            accessible_columns
        })
    }

    /// This checks the current and parent queries for columns with\
    /// a the type that falls into the column_types vec. The columns should
    /// be accessible from the respeccove relations
    ///
    /// The 'only_unique_column_names' argument, if true, signals that only\
    /// unique column names should be considered (needed for unqualified columns)
    ///
    /// If 'only_group_by_columns' is true, the current query requires\
    /// a column to be groupped (mentioned in GROUP BY)
    ///
    /// For the parent queries, the column source is determined by the\
    /// modifiers present at the time of the subquery call.
    fn get_accessible_column_levels_by_types_iter(
        &self, column_types: Vec<SubgraphType>, only_group_by_columns: bool, only_unique_column_names: bool,
    ) -> impl Iterator<Item = Vec<(&SubgraphType, [&IdentName; 2])>> {
        self.get_accessible_column_levels_iter_retrieve_by(
            move |fc, allowed_rel_col_names_opt, only_unique_column_names| {
                Box::new(fc.get_accessible_columns_by_types(
                    &column_types, allowed_rel_col_names_opt, only_unique_column_names
                ).into_iter())
            }, only_group_by_columns, only_unique_column_names
        )
    }

    fn get_accessible_column_levels_iter(
        &self, only_group_by_columns: bool, only_unique_column_names: bool,
    ) -> impl Iterator<Item = Vec<(&SubgraphType, [&IdentName; 2])>> {
        self.get_accessible_column_levels_iter_retrieve_by(
            |fc, allowed_rel_col_names_opt, only_unique_column_names| {
                Box::new(fc.get_accessible_columns_iter(
                    allowed_rel_col_names_opt, only_unique_column_names
                ))
            }, only_group_by_columns, only_unique_column_names
        )
    }

    pub fn get_non_empty_column_levels_by_types(
        &self, column_types: Vec<SubgraphType>, only_group_by_columns: bool, only_unique_column_names: bool,
    ) -> Vec<Vec<(&SubgraphType, [&IdentName; 2])>> {
        self.get_accessible_column_levels_by_types_iter(
            column_types, only_group_by_columns, only_unique_column_names
        ).filter(|col_vec| !col_vec.is_empty()).collect::<Vec<_>>()
    }

    pub fn is_type_available(&self, any_of: Vec<SubgraphType>, only_group_by_columns: bool) -> bool {
        self.get_accessible_column_levels_by_types_iter(
            any_of, only_group_by_columns, false
        ).find(|col_vec| !col_vec.is_empty()).is_some()
    }

    /// checks if there are columns:
    /// 1) with types that fall into any_of,
    /// 2) with names that are unique for their respective FROMs,
    /// 3) at any subquery nesting level.
    pub fn has_unique_columns_for_types(
        &self, any_of: Vec<SubgraphType>, only_group_by_columns: bool
    ) -> bool {
        self.get_accessible_column_levels_by_types_iter(
            any_of, only_group_by_columns, true
        ).find(|col_vec| !col_vec.is_empty()).is_some()
    }

    /// checks if there are columns:
    /// 1) with names that are unique for their respective FROMs,
    /// 2) at any subquery nesting level.
    pub fn has_unique_columns(&self, only_group_by_columns: bool) -> bool {
        self.get_accessible_column_levels_iter(
            only_group_by_columns, true
        ).find(|col_vec| !col_vec.is_empty()).is_some()
    }

    pub fn retrieve_column_by_ident_components(
        &self, ident_components: &Vec<Ident>, only_group_by_columns: bool
    ) -> Result<(SubgraphType, [IdentName; 2]), ColumnRetrievalError> {
        let (
            only_unique_column_names, searched_rel, searched_col
        ): (bool, std::option::Option<IdentName>, IdentName) = match ident_components.as_slice() {
            [col_ident] => {
                (true, None, col_ident.clone().into())
            },
            [rel_ident, col_ident] => {
                (false, Some(rel_ident.clone().into()), col_ident.clone().into())
            }
            any => return Err(ColumnRetrievalError::new(format!(
                "Can't access column. Name requires 1 or 2 elements, got: {:?}", any
            ))),
        };
        let rel_col_tp_opt = self.get_accessible_column_levels_iter(
            only_group_by_columns, only_unique_column_names
        ).find_map(
            |cols_and_types| cols_and_types.into_iter()
                .find_map(|(tp, [rel_name, col_name])|
                    if *col_name == searched_col && searched_rel.as_ref().map(
                        |s_rel| s_rel == rel_name
                    ).unwrap_or(true) {
                        Some((tp.clone(), [rel_name.clone(), col_name.clone()]))
                    } else { None }
                )
        );
        match rel_col_tp_opt {
            Some(val) => Ok(val),
            None => Err(ColumnRetrievalError::new(format!(
                "Couldn't find column named {}.\nAccessible columns:{:#?}",
                    ObjectName(ident_components.clone()), self.get_accessible_column_levels_iter(
                        only_group_by_columns, only_unique_column_names
                    ).collect::<Vec<_>>()
            ))),
        }
    }

    pub fn retrieve_column_by_column_expr(
        &self, column_expr: &Expr, only_group_by_columns: bool
    ) -> Result<(SubgraphType, [IdentName; 2]), ColumnRetrievalError> {
        let ident_components = match column_expr {
            Expr::Identifier(ident) => vec![ident.clone()],
            Expr::CompoundIdentifier(ident_components) => ident_components.clone(),
            any => panic!("Unexpected expression for GROUP BY column: {:#?}", any),
        };
        self.retrieve_column_by_ident_components(&ident_components, only_group_by_columns)
    }
}

#[derive(Debug, Clone)]
pub struct AllAccessibleFroms {
    from_contents_stack: Vec<FromContents>,
    subfrom_opt_stack: Vec<Option<FromContents>>,
}

#[derive(Debug, Clone)]
pub struct AllAccessibleFromsCutoffCheckpoint {
    stack_length: usize,
    from_contents_last: FromContents,
    subfrom_opt_last: Option<FromContents>,
}

impl AllAccessibleFroms {
    fn new() -> Self {
        Self {
            from_contents_stack: vec![],
            subfrom_opt_stack: vec![],
        }
    }

    pub fn append_table(&mut self, create_table_st: &CreateTableSt) -> TableAlias {
        let top_from_alias = self.top_from_mut().append_table(create_table_st, None);
        if let Some(subfrom) = self.current_subfrom_opt_mut() {
            subfrom.append_table(create_table_st, Some(top_from_alias))
        } else { top_from_alias }
    }

    pub fn append_query(&mut self, column_idents_and_graph_types: Vec<(Option<IdentName>, SubgraphType)>) -> TableAlias {
        let top_from_alias = self.top_from_mut().append_query(column_idents_and_graph_types.clone(), None);
        if let Some(subfrom) = self.current_subfrom_opt_mut() {
            subfrom.append_query(column_idents_and_graph_types, Some(top_from_alias))
        } else { top_from_alias }
    }

    pub fn add_subfrom(&mut self) {
        *self.current_subfrom_opt_mut() = Some(FromContents::new());
    }

    pub fn delete_subfrom(&mut self) {
        self.current_subfrom_opt_mut().take().unwrap();
    }

    fn get_cutoff_checkpoint(&self) -> AllAccessibleFromsCutoffCheckpoint {
        AllAccessibleFromsCutoffCheckpoint {
            stack_length: self.from_contents_stack.len(),
            from_contents_last: self.from_contents_stack.last().unwrap().clone(),
            subfrom_opt_last: self.subfrom_opt_stack.last().unwrap().clone(),
        }
    }

    fn restore_from_cutoff_checkpoint(&mut self, checkpoint: AllAccessibleFromsCutoffCheckpoint) {
        self.from_contents_stack.truncate(checkpoint.stack_length);
        *self.from_contents_stack.last_mut().unwrap() = checkpoint.from_contents_last;
        self.subfrom_opt_stack.truncate(checkpoint.stack_length);
        *self.subfrom_opt_stack.last_mut().unwrap() = checkpoint.subfrom_opt_last;
    }

    fn current_subfrom_opt_mut(&mut self) -> &mut Option<FromContents> {
        self.subfrom_opt_stack.last_mut().unwrap()
    }

    fn current_subfrom_opt(&self) -> &Option<FromContents> {
        self.subfrom_opt_stack.last().unwrap()
    }

    /// returns an iterator of FromContents that first contains current subfrom, then
    /// the top from, then all the other froms from bottom query level to top query level
    fn get_from_contents_hierarchy_iter(&self) -> impl Iterator<Item = &FromContents> {
        self.from_contents_stack.iter().rev().zip(self.subfrom_opt_stack.iter().rev()).map(
            |(from_contents, subfrom_opt)|
                subfrom_opt.as_ref().unwrap_or(from_contents)
        )
    }

    fn active_from(&self) -> &FromContents {
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
        self.from_contents_stack.push(FromContents::new());
    }

    fn on_query_end(&mut self) {
        self.subfrom_opt_stack.pop();
        self.from_contents_stack.pop();
    }
}

#[derive(Debug, Clone)]
struct SubqueryCallOptions {
    only_group_by_columns: bool,
    _no_aggregate: bool,
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
                _no_aggregate: false,
            });
        } else {
            let mods = unwrap_variant!(subquery_call_mods, CallModifiers::StaticList);
            self.current_subquery_call_options = Some(SubqueryCallOptions {
                only_group_by_columns: mods.contains(&SmolStr::new("group by columns")),
                _no_aggregate: mods.contains(&SmolStr::new("no aggregate")),
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

    pub fn select_type(&self) -> &Vec<(Option<IdentName>, SubgraphType)> {
        &self.column_idents_and_graph_types.as_ref().unwrap()
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
            Expr::Identifier(ident) => Some(ident.clone()),
            Expr::CompoundIdentifier(idents) => Some(idents.last().unwrap().clone()),
            // unnnamed aggregation can be referred to by function name in postgres
            Expr::Function(func) => Some(func.name.0.last().unwrap().clone()),
            Expr::Case { operand: _, conditions: _, results: _, else_result } => {
                else_result.as_ref()
                    .and_then(|else_expr| if matches!(
                        else_expr.unnested(), Expr::Value(sqlparser::ast::Value::Boolean(_))
                    ) || matches!(
                        else_expr.unnested(), Expr::TypedString { data_type, value: _ } if *data_type == DataType::Date
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
            }
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
            } if *data_type == DataType::Date => {
                Some(Ident { value: "date".to_string(), quote_style: Some('"') })
            },
            Expr::Subquery(query) => {
                let select_body = unwrap_variant!(&*query.body, SetExpr::Select);
                match select_body.projection.as_slice() {
                    &[SelectItem::UnnamedExpr(ref expr)] => Self::extract_alias(expr).map(IdentName::into),
                    &[SelectItem::ExprWithAlias { expr: _, ref alias }] => Some(alias.clone().into()),
                    _ => None,
                }
            },
            Expr::Trim { expr: _, trim_where, trim_what: _ } => {
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
            Expr::Substring { expr: _, substring_from: _, substring_for: _ } => Some(Ident { value: "substring".to_string(), quote_style: Some('"') }),
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
/// the uppercase string.
#[derive(Clone, Debug, Eq)]
pub struct IdentName {
    ident: Ident
}

impl ToString for IdentName {
    fn to_string(&self) -> String {
        self.ident.value.to_uppercase()
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
    free_relation_name_index: usize,
}

impl FromContents {
    pub fn new() -> Self {
        Self { relations: BTreeMap::new(), free_relation_name_index: 0 }
    }

    fn create_relation_alias(&mut self, with_alias: Option<TableAlias>) -> TableAlias {
        with_alias.unwrap_or({
            let alias = format!("T{}", self.free_relation_name_index);
            self.free_relation_name_index += 1;
            TableAlias { name: Ident { value: alias, quote_style: None }, columns: vec![] }
        })
    }

    fn append_table(&mut self, create_table_st: &CreateTableSt, with_alias: Option<TableAlias>) -> TableAlias {
        let alias = self.create_relation_alias(with_alias);
        let relation = Relation::from_table(SmolStr::new(alias.name.value.clone()), create_table_st);
        self.relations.insert(relation.alias.clone(), relation);
        alias
    }

    fn append_query(&mut self, column_idents_and_graph_types: Vec<(Option<IdentName>, SubgraphType)>, with_alias: Option<TableAlias>) -> TableAlias {
        let alias = self.create_relation_alias(with_alias);
        let relation = Relation::from_query(SmolStr::new(alias.name.value.clone()), column_idents_and_graph_types);
        self.relations.insert(relation.alias.clone(), relation);
        alias
    }

    /// get all the columns represented in only a single relation in FROM
    fn get_unique_columns(&self) -> HashSet<IdentName> {
        self.relations.iter()
            .flat_map(|(_, rel)| rel.get_all_column_names_iter().cloned())
            .fold(HashMap::new(), |mut acc, x| {
                *acc.entry(x).or_insert(0usize) += 1usize;
                acc
            }).into_iter()
            .filter(|(_, num)| *num == 1)
            .map(|(col_name, _)| col_name)
            .collect()
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
                allowed_columns = allowed_columns.intersection(
                    &allowed_rel_col_names.into_iter().map(
                        |x| x.into_iter().last().unwrap()
                    ).collect()
                ).cloned().collect()
            }
            (Some(allowed_columns), None)
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

    /// returns accessible columns which types are in the column_types vec\
    /// - if allowed_col_names_opt is not None, checks that the columns name is in the set
    /// - if only_unique is true, only columns with unique names for this FROM clause are returned
    fn get_accessible_columns_by_types(
        &self, column_types: &Vec<SubgraphType>, allowed_rel_col_names_opt: Option<HashSet<[IdentName; 2]>>, only_unique: bool
    ) -> Vec<(&SubgraphType, [&IdentName; 2])> {
        self.get_accessible_columns_iter(allowed_rel_col_names_opt, only_unique)
            .filter(|(col_tp, ..)| column_types.iter().any(
                |searched_type| (*col_tp).is_same_or_more_determined_or_undetermined(searched_type)
            )).collect()
    }

    pub fn get_relation_by_name(&self, name: &Ident) -> &Relation {
        self.relations.iter().find(|(rl_name, _)| **rl_name == name.clone().into()).unwrap().1
    }

    /// Returns columns in the following format: alias.column_name
    pub fn get_wildcard_columns_iter(&self) -> impl Iterator<Item = (Option<IdentName>, SubgraphType)> + '_ {
        self.relations.values().flat_map(
            |relation| relation.get_all_columns_iter_cloned()
        )
    }

    pub fn num_relations(&self) -> usize {
        self.relations.len()
    }

    pub fn relations_iter(&self) -> impl Iterator<Item = (&IdentName, &Relation)> {
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
}

impl Relation {
    fn with_alias(alias: SmolStr) -> Self {
        Self {
            alias: Ident::new(alias).into(),
            accessible_columns: ColumnContainer::new(),
            unnamed_columns: vec![],
            ambiguous_columns: vec![],
        }
    }

    fn from_table(alias: SmolStr, create_table: &CreateTableSt) -> Self {
        let mut _self = Relation::with_alias(alias);
        for column in &create_table.columns {
            let graph_type = SubgraphType::from_data_type(&column.data_type);
            _self.accessible_columns.append_column(column.name.clone().into(), graph_type);
        }
        _self
    }

    fn from_query(alias: SmolStr, column_idents_and_graph_types: Vec<(Option<IdentName>, SubgraphType)>) -> Self {
        let mut _self = Relation::with_alias(alias.clone());
        let mut named_columns = vec![];
        for (column_name, graph_type) in column_idents_and_graph_types {
            if let Some(column_name) = column_name {
                named_columns.push((column_name, graph_type));
            } else {
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
    pub fn get_all_columns_iter(&self) -> impl Iterator<Item = (Option<&IdentName>, &SubgraphType)> {
        self.get_accessible_columns_iter()
            .map(|(col_ident, col_tp)| (Some(col_ident), col_tp))
            .chain(self.unnamed_columns.iter().map(|x| (None, x)))
            .chain(self.ambiguous_columns.iter().map(|(column_name, graph_type)| (
                Some(column_name), graph_type
            )))
    }

    /// get all columns with their types, including the unnamed ones and ambiguous ones
    pub fn get_all_columns_iter_cloned(&self) -> impl Iterator<Item = (Option<IdentName>, SubgraphType)> + '_ {
        self.get_all_columns_iter().map(|(x, y)| (x.cloned(), y.clone()))
    }
}
