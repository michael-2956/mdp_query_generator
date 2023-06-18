use std::collections::HashMap;

use rand::{SeedableRng, Rng};
use rand_chacha::ChaCha8Rng;
use smol_str::SmolStr;
use super::super::super::unwrap_variant;
use super::super::state_generators::SubgraphType;
use sqlparser::ast::{Ident, ObjectName, Statement, ColumnDef, HiveDistributionStyle, TableConstraint, HiveFormat, SqlOption, FileFormat, Query, OnCommit};

macro_rules! define_impersonation {
    ($impersonator:ident, $enum_name:ident, $variant:ident, { $($field:ident: $type:ty),* $(,)? }) => {
        #[derive(Debug, Clone)]
        struct $impersonator {
            $(
                $field: $type,
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

#[derive(Debug, Clone)]
pub struct Projection {
    pub alias: ObjectName,
    /// A HashMap with column names by graph types
    pub columns: HashMap<SubgraphType, Vec<ObjectName>>,
    rng: ChaCha8Rng,
}

impl Projection {
    fn with_name(name: SmolStr) -> Self {
        Self {
            alias: ObjectName(vec![Ident::new(name)]),
            columns: HashMap::new(),
            rng: ChaCha8Rng::seed_from_u64(1),
        }
    }

    fn append_graph_type(&mut self, column_name: ObjectName, graph_type: SubgraphType) {
        self.columns.entry(graph_type)
            .and_modify(|v| v.push(column_name.clone()))
            .or_insert(vec![column_name.clone()]);
    }

    /// Retrieve an identifier vector of a random column of the specified type.
    pub fn get_random_column_with_type(&mut self, graph_type: SubgraphType) -> Vec<Ident> {
        let type_columns = self.columns.get(&graph_type).unwrap();
        let num_skip = self.rng.gen_range(0..type_columns.len());
        vec![
            self.alias.0.clone(),
            type_columns.iter().skip(num_skip).next().unwrap().0.clone()
        ].concat()
    }
}

#[derive(Debug, Clone)]
pub struct TableRelation {
    create_table: CreateTableSt,
    free_column_name_index: u32,
    projection: Projection,
}

impl std::fmt::Display for TableRelation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.create_table)
    }
}

impl TableRelation {
    fn with_name(name: SmolStr) -> Self {
        let projection = Projection::with_name(name);
        Self {
            free_column_name_index: 1,
            create_table: CreateTableSt {
                or_replace: false,
                temporary: false,
                external: false,
                global: None,
                if_not_exists: false,
                transient: false,
                name: projection.alias.clone(),
                columns: vec![],
                constraints: vec![],
                hive_distribution: HiveDistributionStyle::NONE,
                hive_formats: None,
                table_properties: vec![],
                with_options: vec![],
                file_format: None,
                location: None,
                query: None,
                without_rowid: false,
                like: None,
                clone: None,
                engine: None,
                default_charset: None,
                collation: None,
                on_commit: None,
                on_cluster: None,
            },
            projection: projection,
        }
    }

    pub fn get_projection(&self) -> &Projection {
        &self.projection
    }

    pub fn add_typed_column(&mut self, graph_type: SubgraphType) -> Ident {
        let data_type = graph_type.clone().to_data_type();
        let name: String = format!("C{}", self.free_column_name_index);
        self.free_column_name_index += 1;

        let column_ident = Ident { value: name.clone(), quote_style: None };
        let column_name = ObjectName(vec![column_ident.clone()]);
        self.projection.append_graph_type(column_name, graph_type);

        self.create_table.columns.push(ColumnDef {
            name: column_ident,
            data_type: data_type,
            collation: None,
            options: vec![]
        });
        self.create_table.columns.last().unwrap().name.clone()
    }
}

pub struct RelationManager {
    relations: HashMap<ObjectName, TableRelation>,
    free_relation_name_index: u32,
}

impl std::fmt::Display for RelationManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.relations.iter().try_for_each(
            |relation| writeln!(f, "{};", relation.1)
        )
    }
}

impl RelationManager {
    pub fn new() -> Self {
        Self {
            relations: HashMap::<_, _>::new(),
            free_relation_name_index: 1,
        }
    }

    fn new_name(&mut self) -> SmolStr {
        let name = SmolStr::new(format!("T{}", self.free_relation_name_index));
        self.free_relation_name_index += 1;
        name
    }

    pub fn new_relation(&mut self) -> &mut TableRelation {
        let name = self.new_name();
        let relation = TableRelation::with_name(name);
        let o_name = relation.get_projection().alias.clone();
        self.relations.insert(o_name.clone(), relation);
        self.relations.get_mut(&o_name).unwrap()
    }

    pub fn get_relation_by_oname(&mut self, key: &ObjectName) -> &mut TableRelation {
        self.relations.get_mut(key).unwrap()
    }

    pub fn new_ident(&mut self) -> Ident {
        Ident::new(self.new_name())
    }
}

impl SubgraphType {
    /// get a list of compatible types
    pub fn get_compat_types(&self) -> Vec<SubgraphType> {
        vec![self.clone()]
    }
}


#[derive(Debug, Clone, PartialEq)]
pub enum TypesSelectedType {
    Type(SubgraphType), Any
}

impl TypesSelectedType {
    /// get a list of compatible types
    pub fn get_compat_types(&self) -> Vec<SubgraphType> {
        match self {
            Self::Any => SubgraphType::get_all(),
            Self::Type(type_name) => type_name.get_compat_types(),
        }
    }

    pub fn is_equal_to(&self, other: &TypesSelectedType) -> bool {
        *other == Self::Any || *self == Self::Any || *other == *self
    }

    pub fn is_compat_with(&self, other: &TypesSelectedType) -> bool {
        self.is_equal_to(other) || self.get_compat_types().contains(unwrap_variant!(other, TypesSelectedType::Type))
    }
}