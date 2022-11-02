use std::collections::HashMap;

use smol_str::SmolStr;
use sqlparser::ast::{Ident, ObjectName};

pub struct Relation {
    name: String,
    free_column_name_index: u32
}

impl Relation {
    fn with_name(name: String) -> Self {
        Self {
            name, free_column_name_index: 1
        }
    }

    pub fn gen_ident(&self) -> Ident {
        Ident { value: self.name.clone(), quote_style: None }
    }

    pub fn gen_object_name(&self) -> ObjectName {
        ObjectName(vec![self.gen_ident()])
    }

    pub fn gen_column_ident(&mut self) -> Ident {
        let name = format!("C{}", self.free_column_name_index);
        self.free_column_name_index += 1;
        Ident { value: name.clone(), quote_style: None }
    }
}

pub struct RelationGenerator {
    pub relations: HashMap<String, Relation>,
    free_relation_name_index: u32
}

impl RelationGenerator {
    fn new() -> Self {
        Self {
            relations: HashMap::<_, _>::new(),
            free_relation_name_index: 1
        }
    }

    pub fn new_relation(&mut self) -> &mut Relation {
        let name = format!("T{}", self.free_relation_name_index);
        let relation = Relation::with_name(name.clone());
        self.free_relation_name_index += 1;
        self.relations.insert(name.clone(), relation);
        self.relations.get_mut(&name).unwrap()
    }

    pub fn new_ident(&mut self) -> Ident {
        Ident::new(format!("T{}", self.free_relation_name_index))
    }
}

pub struct QueryInfo {
    /// used to generate and store info about
    /// all available relations
    pub relation_generator: RelationGenerator,
    /// used to store running query statistics, such as
    /// the current level of nesting
    pub stats: QueryStats,
}

impl QueryInfo {
    pub fn new() -> QueryInfo {
        QueryInfo {
            relation_generator: RelationGenerator::new(),
            stats: QueryStats::new()
        }
    }
}

pub struct QueryStats {
    /// Remember to increase this value before
    /// and decrease after generating a subquery, to
    /// control the maximum level of nesting
    /// allowed
    #[allow(dead_code)]
    current_nest_level: u32,
}

impl QueryStats {
    fn new() -> Self {
        Self {
            current_nest_level: 0
        }
    }
}

#[derive(Debug, Clone)]
pub enum TypesSelectedType {
    Numeric, Val3, Array, ListExpr, String, Any
}

impl PartialEq for TypesSelectedType {
    fn eq(&self, other: &Self) -> bool {
        core::mem::discriminant(self) == core::mem::discriminant(other) ||
        core::mem::discriminant(self) == core::mem::discriminant(&Self::Any) ||
        core::mem::discriminant(other) == core::mem::discriminant(&Self::Any)
    }
}

impl TypesSelectedType {
    fn to_smolstr(&self) -> SmolStr {
        match self {
            TypesSelectedType::Numeric => SmolStr::new("numeric"),
            TypesSelectedType::Val3 => SmolStr::new("3VL Value"),
            TypesSelectedType::Array => SmolStr::new("array"),
            TypesSelectedType::ListExpr => SmolStr::new("list expr"),
            TypesSelectedType::String => SmolStr::new("string"),
            TypesSelectedType::Any => SmolStr::new("any"),
        }
    }

    pub fn get_types(&self) -> Vec<SmolStr> {
        match self {
            TypesSelectedType::Any => vec![
                (TypesSelectedType::String).to_smolstr(),
                (TypesSelectedType::ListExpr).to_smolstr(),
                (TypesSelectedType::Array).to_smolstr(),
                (TypesSelectedType::Val3).to_smolstr(),
                (TypesSelectedType::Numeric).to_smolstr(),
            ],
            _ => vec![self.to_smolstr()],
        }
    }

    /// get a list of compatible types
    pub fn get_compat_types(&self) -> Vec<SmolStr> {
        self.get_types()
    }

    pub fn is_compat_with(&self, other: &TypesSelectedType) -> bool {
        *other == TypesSelectedType::Any || *self == TypesSelectedType::Any ||
        self.get_compat_types().contains(&other.to_smolstr())
    }
}