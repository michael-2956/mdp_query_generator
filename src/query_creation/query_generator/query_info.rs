use std::{collections::{BTreeMap, HashMap, HashSet, BTreeSet}, error::Error, fmt, hash::Hash, path::Path};

use rand::Rng;
use rand_chacha::ChaCha8Rng;
use smol_str::SmolStr;
use crate::query_creation::state_generator::subgraph_type::SubgraphType;

use sqlparser::{ast::{ColumnDef, Expr, FileFormat, HiveDistributionStyle, HiveFormat, Ident, ObjectName, OnCommit, Query, SqlOption, Statement, TableAlias, TableConstraint}, dialect::PostgreSqlDialect, parser::Parser};

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
        &self.table_defs.iter().find(|x| &x.name == name).unwrap()
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

impl ClauseContext {
    pub fn new() -> Self {
        Self {
            query_props_stack: vec![],
            all_accessible_froms: AllAccessibleFroms::new(),
            group_by_contents_stack: vec![],
        }
    }

    pub fn on_query_begin(&mut self) {
        self.all_accessible_froms.on_query_begin();
        self.query_props_stack.push(QueryProps::new());
        self.group_by_contents_stack.push(GroupByContents::new());
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

    pub fn on_query_end(&mut self) {
        self.all_accessible_froms.on_query_end();
        self.query_props_stack.pop();
        self.group_by_contents_stack.pop();
    }
}

#[derive(Debug, Clone)]
pub struct AllAccessibleFroms {
    from_contents_stack: Vec<FromContents>,
    current_subfrom: Option<FromContents>,
}

impl AllAccessibleFroms {
    fn new() -> Self {
        Self {
            from_contents_stack: vec![],
            current_subfrom: None,
        }
    }

    pub fn append_table(&mut self, create_table_st: &CreateTableSt) -> TableAlias {
        let top_from_alias = self.top_from_mut().append_table(create_table_st, None);
        if let Some(ref mut subfrom) = self.current_subfrom {
            subfrom.append_table(create_table_st, Some(top_from_alias))
        } else { top_from_alias }
    }

    pub fn append_query(&mut self, column_idents_and_graph_types: Vec<(Option<Ident>, SubgraphType)>) -> TableAlias {
        let top_from_alias = self.top_from_mut().append_query(column_idents_and_graph_types.clone(), None);
        if let Some(ref mut subfrom) = self.current_subfrom {
            subfrom.append_query(column_idents_and_graph_types, Some(top_from_alias))
        } else { top_from_alias }
    }

    fn active_from(&self) -> &FromContents {
        self.current_subfrom.as_ref().unwrap_or(self.top_from())
    }

    fn top_from(&self) -> &FromContents {
        self.from_contents_stack.last().unwrap()
    }

    fn top_from_mut(&mut self) -> &mut FromContents {
        self.from_contents_stack.last_mut().unwrap()
    }

    pub fn create_empty_subfrom(&mut self) {
        self.current_subfrom = Some(FromContents::new());
    }

    pub fn delete_subfrom(&mut self) {
        self.current_subfrom.take();
    }

    fn on_query_begin(&mut self) {
        self.from_contents_stack.push(FromContents::new());
    }

    fn on_query_end(&mut self) {
        self.current_subfrom.take();
        self.from_contents_stack.pop();
    }
}

#[derive(Debug, Clone)]
pub struct QueryProps {
    is_aggregation_indicated: bool,
}

impl QueryProps {
    fn new() -> QueryProps {
        Self {
            is_aggregation_indicated: false
        }
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

    fn is_type_available(&self, graph_type: &SubgraphType, allowed_columns_opt: Option<&HashSet<ColumnNameType>>) -> bool {
        self.columns.iter()
            .filter(|(_, cols)| if let Some(allowed_columns) = allowed_columns_opt {
                cols.iter().any(|x| allowed_columns.contains(x))
            } else { true })
            .any(|(col_type, _)| col_type.is_same_or_more_determined_or_undetermined(graph_type))
    }

    /// Retrieve an identifier vector of a random column of the specified type.
    fn get_random_column_with_type(&self, rng: &mut ChaCha8Rng, graph_type: &SubgraphType, allowed_columns_opt: Option<&HashSet<ColumnNameType>>) -> (SubgraphType, ColumnNameType) {
        let type_columns = self.columns.iter()
            .filter(|(column_type, column_names)|
                column_type.is_same_or_more_determined_or_undetermined(graph_type) &&
                allowed_columns_opt.map(|allowed| column_names.iter().any(
                    |column_name| allowed.contains(column_name)
                )).unwrap_or(true)
            )
            .flat_map(|(column_type, column_names)| column_names.iter()
                .filter(|column_name| allowed_columns_opt.map(|allowed|
                    allowed.contains(column_name)
                ).unwrap_or(true))
                .map(|column_name| (column_type.clone(), column_name))
            )
            .collect::<Vec<_>>();
        let (column_type, column_name) = type_columns[rng.gen_range(0..type_columns.len())].clone();
        (column_type, column_name.clone())
    }

    fn try_get_column_type_by_name(&self, column_name: &ColumnNameType) -> Option<SubgraphType> {
        self.columns.iter()
            .find(|(_, cols)| cols.contains(column_name))
            .map(|x| x.0.clone())
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
    columns: ColumnContainer<Vec<Ident>>,
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

    pub fn append_column(&mut self, column_name: Vec<Ident>, graph_type: SubgraphType) {
        if column_name.len() != 2 {
            panic!("GroupByContents only accepts qualified identifiers. Got: {:?}", column_name)
        }
        self.columns.append_column(column_name, graph_type)
    }

    /// checks if the types is available in any of the columns mentioned in GROUP BY
    pub fn is_type_available(&self, graph_type: &SubgraphType) -> bool {
        self.columns.is_type_available(graph_type, None)
    }

    pub fn get_random_column_with_type_of(&self, rng: &mut ChaCha8Rng, column_types: &Vec<SubgraphType>) -> (SubgraphType, Vec<Ident>) {
        let available_graph_types = column_types.into_iter().filter(
            |x| self.is_type_available(x)
        ).collect::<Vec<_>>();
        // println!("available_graph_types: {:#?}", available_graph_types);
        let selected_graph_type = available_graph_types[rng.gen_range(0..available_graph_types.len())];
        self.get_random_column_with_type(rng, selected_graph_type)
    }

    /// Retrieve an identifier vector of a random column of the specified type.
    pub fn get_random_column_with_type(&self, rng: &mut ChaCha8Rng, graph_type: &SubgraphType) -> (SubgraphType, Vec<Ident>) {
        self.columns.get_random_column_with_type(rng, graph_type, None)
    }

    pub fn get_column_type_by_ident_components(&self, ident_components: &Vec<Ident>) -> Result<SubgraphType, ColumnRetrievalError> {
        match self.columns.try_get_column_type_by_name(ident_components) {
            Some(column_type) => Ok(column_type),
            None => Err(ColumnRetrievalError::new(format!(
                "Couldn't find column named {} GROUP BY: {:#?}", ObjectName(ident_components.clone()), self
            ))),
        }
    }

    pub fn get_column_name_set(&self) -> HashSet<Ident> {
        HashSet::from_iter(self.columns.get_column_names_iter().map(
            |idents| idents.last().unwrap().clone()
        ))
    }

    pub fn get_column_names_iter(&self) -> impl Iterator<Item = &Vec<Ident>> {
        self.columns.get_column_names_iter()
    }
}

#[derive(Debug, Clone)]
pub struct FromContents {
    /// maps aliases to relations themselves
    /// This is ordered because we need consistent
    /// same-seed runs
    relations: BTreeMap<Ident, Relation>,
    free_relation_name_index: usize,
}

impl FromContents {
    pub fn new() -> Self {
        Self { relations: BTreeMap::new(), free_relation_name_index: 0 }
    }

    /// Checks if the type is accessible in any of the relations
    /// 
    /// the allowed_columns_opt is set when we want to only allow columns that are non-duplicate
    /// in any of the tables
    pub fn is_type_available(&self, graph_type: &SubgraphType, allowed_columns_opt: Option<&HashSet<Ident>>) -> bool {
        self.relations.iter().any(|x| x.1.is_type_accessible(graph_type, allowed_columns_opt))
    }

    /// checks if FROM has columns with names that are unique for all its tables,
    /// for any of the specified types
    pub fn has_unique_columns_for_types(
        &self,
        graph_types: &Vec<SubgraphType>,
        allowed_col_names: Option<HashSet<Ident>>,
    ) -> bool {
        let mut unique_columns = self.get_unique_columns();
        if let Some(allowed_col_names) = allowed_col_names {
            unique_columns = unique_columns.intersection(&allowed_col_names).cloned().collect();
        }
        graph_types.iter().any(|graph_type|
            self.relations.iter().any(|(_, relation)| relation.is_type_accessible(
                graph_type, Some(&unique_columns)
            ))
        )
    }

    /// get all the columns represented in only a single relation in FROM
    fn get_unique_columns(&self) -> HashSet<Ident> {
        self.relations.iter()
            .flat_map(|(_, rel)| rel.get_column_names_iter())
            .fold(HashMap::new(), |mut acc, x| {
                *acc.entry(x).or_insert(0usize) += 1usize;
                acc
            }).into_iter()
            .filter(|(_, num)| *num == 1)
            .map(|(col_name, _)| col_name.clone())
            .collect()
    }

    /// if qualified is false, returns only the column name, only considering columns which
    /// have names unique among all the tables
    pub fn get_random_column_with_type_of(
        &self, rng: &mut ChaCha8Rng,
        column_types: &Vec<SubgraphType>,
        qualified: bool,
        allowed_col_names: Option<HashSet<Ident>>,
    ) -> (SubgraphType, Vec<Ident>) {
        let allowed_columns_opt = if !qualified {
            let mut allowed_columns = self.get_unique_columns();
            if let Some(allowed_col_names) = allowed_col_names {
                allowed_columns = allowed_columns.intersection(&allowed_col_names).cloned().collect()
            }
            Some(allowed_columns)
        } else { None };
        let available_graph_types = column_types.into_iter().filter(
            |x| self.is_type_available(x, allowed_columns_opt.as_ref())
        ).collect::<Vec<_>>();
        let selected_graph_type = available_graph_types[rng.gen_range(0..available_graph_types.len())];
        self.get_random_column_with_type(rng, selected_graph_type, allowed_columns_opt.as_ref(), qualified)
    }

    pub fn get_relation_alias_by_column_name(&self, column_name: &Ident) -> Ident {
        if let Some((_, relation)) = self.relations.iter().find(|(_, relation)|
            relation.try_get_column_type_by_name(column_name).is_some()
        ) {
            relation.alias.clone()
        } else {
            panic!("Couldn't find column named {}.", column_name)
        }
    }

    pub fn get_qualified_ident_components(&self, column_expr: &Expr) -> Vec<Ident> {
        match column_expr {
            Expr::Identifier(ident) => self.qualify_column(vec![ident.clone()]),
            Expr::CompoundIdentifier(ident_components) => ident_components.clone(),
            any => panic!("Unexpected expression for GROUP BY column: {:#?}", any),
        }
    }

    pub fn qualify_column(&self, ident_components: Vec<Ident>) -> Vec<Ident> {
        match ident_components.len() {
            1 => {
                let col_ident = ident_components.into_iter().next().unwrap();
                let rel_ident = self.get_relation_alias_by_column_name(&col_ident);
                vec![rel_ident, col_ident]
            },
            2 => ident_components,
            _ => panic!("Can't qualify {:?}", ident_components),
        }
    }

    pub fn get_column_type_by_ident_components(&self, ident_components: &Vec<Ident>) -> Result<SubgraphType, ColumnRetrievalError> {
        let type_opt = match ident_components.as_slice() {
            [col_ident] => {
                self.relations.iter().find_map(|(_, relation)|
                    relation.try_get_column_type_by_name(col_ident)
                )
            },
            [rl_ident, col_ident] => {
                self.get_relation_by_name(rl_ident).try_get_column_type_by_name(col_ident)
            },
            _ => None,
        };
        match type_opt {
            Some(val) => Ok(val),
            None => Err(ColumnRetrievalError::new(format!(
                "Couldn't find column named {}. FromContents: {:#?}", ObjectName(ident_components.clone()), self
            ))),
        }
    }

    fn get_random_column_with_type(&self, rng: &mut ChaCha8Rng, graph_type: &SubgraphType, allowed_columns_opt: Option<&HashSet<Ident>>, qualified: bool) -> (SubgraphType, Vec<Ident>) {
        let relations_with_type: Vec<&Relation> = self.relations.iter()
            .filter(|x| x.1.is_type_accessible(graph_type, allowed_columns_opt))
            .map(|x| x.1)
            .collect::<Vec<_>>();
        let selected_relation = relations_with_type[rng.gen_range(0..relations_with_type.len())];
        selected_relation.get_random_column_with_type(rng, graph_type, allowed_columns_opt, qualified)
    }

    /// Returns a relation and its alias
    pub fn get_random_relation(&self, rng: &mut ChaCha8Rng) -> (&Ident, &Relation) {
        self.relations.iter().skip(rng.gen_range(0..self.relations.len())).next().unwrap()
    }

    pub fn get_relation_by_name(&self, name: &Ident) -> &Relation {
        self.relations.iter().find(|(rl_name, _)| *rl_name == name).unwrap().1
    }

    /// Returns columns in the following format: alias.column_name
    pub fn get_wildcard_columns(&self) -> Vec<(Option<Ident>, SubgraphType)> {
        self.relations.keys()
            .map(|alias|
                self.relations.get(alias).unwrap().get_columns_with_types()
            )
            .collect::<Vec<_>>()
            .concat()
    }

    pub fn relations_num(&self) -> usize {
        self.relations.len()
    }

    pub fn relations_iter(&self) -> impl Iterator<Item = (&Ident, &Relation)> {
        self.relations.iter()
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

    fn append_query(&mut self, column_idents_and_graph_types: Vec<(Option<Ident>, SubgraphType)>, with_alias: Option<TableAlias>) -> TableAlias {
        let alias = self.create_relation_alias(with_alias);
        let relation = Relation::from_query(SmolStr::new(alias.name.value.clone()), column_idents_and_graph_types);
        self.relations.insert(relation.alias.clone(), relation);
        alias
    }
}

#[derive(Debug, Clone)]
pub struct Relation {
    /// projection alias
    pub alias: Ident,
    /// A container of columns accessible by names
    accessible_columns: ColumnContainer<Ident>,
    /// A list of unnamed column's types, as they
    /// can be referenced with wildcards
    pub unnamed_columns: Vec<SubgraphType>,
    /// A list of ambiguous column names and types,
    /// as they can be referenced with wildcards
    pub ambiguous_columns: Vec<(Ident, SubgraphType)>,
}

impl Relation {
    fn with_alias(alias: SmolStr) -> Self {
        Self {
            alias: Ident::new(alias),
            accessible_columns: ColumnContainer::new(),
            unnamed_columns: vec![],
            ambiguous_columns: vec![],
        }
    }

    fn from_table(alias: SmolStr, create_table: &CreateTableSt) -> Self {
        let mut _self = Relation::with_alias(alias);
        for column in &create_table.columns {
            let graph_type = SubgraphType::from_data_type(&column.data_type);
            _self.accessible_columns.append_column(column.name.clone(), graph_type);
        }
        _self
    }

    fn from_query(alias: SmolStr, column_idents_and_graph_types: Vec<(Option<Ident>, SubgraphType)>) -> Self {
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

    pub fn has_accessible_columns(&self) -> bool {
        !self.accessible_columns.columns.is_empty()
    }

    pub fn get_columns_with_types_iter(&self) -> impl Iterator<Item = (Option<&Ident>, &SubgraphType)> {
        self.accessible_columns.columns.iter()
            .flat_map(|(graph_type, column_names)| column_names.iter().map(
                move |column_name| (Some(column_name), graph_type)
            ))
            .chain(self.unnamed_columns.iter().map(|x| (None, x)))
            .chain(self.ambiguous_columns.iter().map(|(column_name, graph_type)| (
                Some(column_name), graph_type
            )))
    }

    /// get all columns with their types, including the unnamed ones and ambiguous ones, by reference
    pub fn get_column_types(&self) -> Vec<SubgraphType> {
        self.get_columns_with_types_iter().map(|(_, col_type)| col_type.clone()).collect()
    }

    /// get all columns with their types, including the unnamed ones and ambiguous ones
    pub fn get_columns_with_types(&self) -> Vec<(Option<Ident>, SubgraphType)> {
        self.get_columns_with_types_iter().map(|(x, y)| (x.cloned(), y.clone())).collect()
    }
    
    pub fn is_type_accessible(&self, graph_type: &SubgraphType, allowed_columns_opt: Option<&HashSet<Ident>>) -> bool {
        self.accessible_columns.is_type_available(graph_type, allowed_columns_opt)
    }

    /// Retrieve an identifier vector of a random column of the specified type.
    pub fn get_random_column_with_type(&self, rng: &mut ChaCha8Rng, graph_type: &SubgraphType, allowed_columns_opt: Option<&HashSet<Ident>>, qualified: bool) -> (SubgraphType, Vec<Ident>) {
        let (column_type, column_name) = self.accessible_columns.get_random_column_with_type(rng, graph_type, allowed_columns_opt);
        let mut column_name = vec![column_name.clone()];
        if qualified { column_name = [vec![self.alias.clone()], column_name ].concat(); }
        (column_type, column_name)
    }

    pub fn try_get_column_type_by_name(&self, column_name: &Ident) -> Option<SubgraphType> {
        self.accessible_columns.try_get_column_type_by_name(column_name)
    }

    /// Returns accessible and ambiguous column names
    pub fn get_column_names_iter(&self) -> impl Iterator<Item = &Ident> {
        self.accessible_columns.get_column_names_iter()
            .chain(self.ambiguous_columns.iter().map(|(x, ..)| x))
    }
}
