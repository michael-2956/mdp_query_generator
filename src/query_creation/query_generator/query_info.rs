use std::{collections::{BTreeMap, HashMap, HashSet}, path::Path};

use rand::Rng;
use rand_chacha::ChaCha8Rng;
use smol_str::SmolStr;
use crate::query_creation::state_generator::subgraph_type::SubgraphType;

use sqlparser::{ast::{Ident, ObjectName, Statement, ColumnDef, HiveDistributionStyle, TableConstraint, HiveFormat, SqlOption, FileFormat, Query, OnCommit, TableAlias}, dialect::PostgreSqlDialect, parser::Parser};

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
    from_contents_stack: Vec<FromContents>,
    group_by_contents_stack: Vec<GroupByContents>
}

impl ClauseContext {
    pub fn new() -> Self{
        Self {
            from_contents_stack: vec![],
            group_by_contents_stack: vec![],
        }
    }

    pub fn on_query_begin(&mut self) {
        self.from_contents_stack.push(FromContents::new());
        self.group_by_contents_stack.push(GroupByContents::new());
    }

    pub fn from(&self) -> &FromContents {
        self.from_contents_stack.last().unwrap()
    }

    pub fn from_mut(&mut self) -> &mut FromContents {
        self.from_contents_stack.last_mut().unwrap()
    }

    pub fn group_by(&self) -> &GroupByContents {
        self.group_by_contents_stack.last().unwrap()
    }

    pub fn group_by_mut(&mut self) -> &mut GroupByContents {
        self.group_by_contents_stack.last_mut().unwrap()
    }

    pub fn on_query_end(&mut self) {
        self.from_contents_stack.pop();
        self.group_by_contents_stack.pop();
    }
}

#[derive(Debug, Clone)]
pub struct GroupByContents {

}

impl GroupByContents {
    fn new() -> GroupByContents {
        GroupByContents {

        }
    }

    pub fn is_type_available(&self, _graph_type: &SubgraphType) -> bool {
        true
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

    fn create_alias(&mut self) -> SmolStr {
        let name = SmolStr::new(format!("T{}", self.free_relation_name_index));
        self.free_relation_name_index += 1;
        name
    }

    fn get_table_alias(alias: SmolStr) -> TableAlias {
        TableAlias { name: Ident { value: alias.to_string(), quote_style: None }, columns: vec![] }
    }

    pub fn is_type_available(&self, graph_type: &SubgraphType, allowed_columns: Option<&HashSet<Ident>>) -> bool {
        self.relations.iter().any(|x| x.1.is_type_available(graph_type, allowed_columns))
    }

    /// checks if FROM has columns with names that are unique for all its tables
    pub fn has_unique_columns_for_types(&self, graph_types: &Vec<SubgraphType>) -> bool {
        let allowed_columns = self.get_unique_columns();
        graph_types.iter().any(|graph_type|
            self.relations.iter().any(|(_, relation)| relation.is_type_available(graph_type, Some(&allowed_columns)))
        )
    }

    /// get all the columns represented in only a single relation in FROM
    fn get_unique_columns(&self) -> HashSet<Ident> {
        self.relations.iter()
            .flat_map(|(_, rel)| rel.columns.values())
            .flat_map(|x| x.iter())
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
    pub fn get_random_column_with_type_of(&self, rng: &mut ChaCha8Rng, graph_type_list: &Vec<SubgraphType>, qualified: bool) -> (SubgraphType, Vec<Ident>) {
        let allowed_columns = if !qualified {
            Some(self.get_unique_columns())
        } else { None };
        let available_graph_types = graph_type_list.into_iter()
            .filter(|x| self.is_type_available(x, allowed_columns.as_ref()))
            .collect::<Vec<_>>();
        let selected_graph_type = available_graph_types.iter()
            .skip(rng.gen_range(0..available_graph_types.len()))
            .next().unwrap();
        self.get_random_column_with_type(rng, selected_graph_type, allowed_columns.as_ref(), qualified)
    }

    pub fn get_column_type_by_ident_components(&self, ident_components: &Vec<Ident>) -> SubgraphType {
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
            Some(val) => val,
            None => panic!("Couldn't find column named {}", ObjectName(ident_components.clone())),
        }
    }

    fn get_random_column_with_type(&self, rng: &mut ChaCha8Rng, graph_type: &SubgraphType, allowed_columns: Option<&HashSet<Ident>>, qualified: bool) -> (SubgraphType, Vec<Ident>) {
        let relations_with_type: Vec<&Relation> = self.relations.iter()
            .filter(|x| x.1.is_type_available(graph_type, allowed_columns))
            .map(|x| x.1)
            .collect::<Vec<_>>();
        let selected_relation = relations_with_type[rng.gen_range(0..relations_with_type.len())];
        selected_relation.get_random_column_with_type(rng, graph_type, qualified)
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

    pub fn append_table(&mut self, create_table_st: &CreateTableSt) -> TableAlias {
        let alias = self.create_alias();
        let relation = Relation::from_table(alias.clone(), create_table_st);
        self.relations.insert(relation.alias.clone(), relation);
        Self::get_table_alias(alias)
    }

    pub fn append_query(&mut self, column_idents_and_graph_types: Vec<(Option<Ident>, SubgraphType)>) -> TableAlias {
        let alias = self.create_alias();
        let relation = Relation::from_query(alias.clone(), column_idents_and_graph_types);
        self.relations.insert(relation.alias.clone(), relation);
        Self::get_table_alias(alias)
    }
}

#[derive(Debug, Clone)]
pub struct Relation {
    /// projection alias
    pub alias: Ident,
    /// A BTreeMap (ordered for consistency) with column
    /// names by graph types
    pub columns: BTreeMap<SubgraphType, Vec<Ident>>,
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
            columns: BTreeMap::new(),
            unnamed_columns: vec![],
            ambiguous_columns: vec![],
        }
    }

    fn from_table(alias: SmolStr, create_table: &CreateTableSt) -> Self {
        let mut _self = Relation::with_alias(alias);
        for column in &create_table.columns {
            let graph_type = SubgraphType::from_data_type(&column.data_type);
            _self.append_column(column.name.clone(), graph_type);
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
                &[ref graph_type] => _self.append_column(column_name, graph_type.clone()),
                other => {
                    _self.ambiguous_columns.extend(other.into_iter().map(|x| (column_name.clone(), x.clone())))
                }
            }
        }
        _self
    }

    fn append_column(&mut self, column_name: Ident, graph_type: SubgraphType) {
        self.columns.entry(graph_type)
            .and_modify(|v| v.push(column_name.clone()))
            .or_insert(vec![column_name.clone()]);
    }

    pub fn is_type_available(&self, graph_type: &SubgraphType, allowed_columns: Option<&HashSet<Ident>>) -> bool {
        self.columns.iter()
            .filter(|(_, cols)| if let Some(allowed_columns) = allowed_columns {
                cols.iter().any(|x| allowed_columns.contains(x))
            } else { true })
            .any(|(col_type, _)| col_type.is_same_or_more_determined_or_undetermined(graph_type))
    }

    /// get all columns with their types, including the unnamed ones and ambiguous ones
    pub fn get_columns_with_types(&self) -> Vec<(Option<Ident>, SubgraphType)> {
        self.columns.iter()
            .flat_map(|(graph_type, column_names)| column_names.iter().map(
                |column_name| (Some(column_name.clone()), graph_type.clone())
            ))
            .chain(self.unnamed_columns.iter().map(|x| (None, x.to_owned())))
            .chain(self.ambiguous_columns.iter().map(|(column_name, graph_type)| (
                Some(column_name.clone()), graph_type.clone()
            )))
            .collect()
    }

    /// Retrieve an identifier vector of a random column of the specified type.
    pub fn get_random_column_with_type(&self, rng: &mut ChaCha8Rng, graph_type: &SubgraphType, qualified: bool) -> (SubgraphType, Vec<Ident>) {
        let type_columns = self.columns.keys()
            .filter(|x| x.is_same_or_more_determined_or_undetermined(graph_type))
            .flat_map(|col_graph_type| self.columns.get(col_graph_type).unwrap().iter()
                .map(|column_name| (col_graph_type.clone(), column_name))
            )
            .collect::<Vec<_>>();
        let num_skip = rng.gen_range(0..type_columns.len());
        let (column_type, column) = type_columns.into_iter().skip(num_skip).next().unwrap();
        let mut column_name = vec![column.clone()];
        if qualified { column_name = [vec![self.alias.clone()], column_name ].concat(); }
        (column_type, column_name)
    }

    pub fn try_get_column_type_by_name(&self, column_name: &Ident) -> Option<SubgraphType> {
        self.columns.iter()
            .find(|(_, cols)| cols.contains(column_name))
            .map(|x| x.0.clone())
    }
}
