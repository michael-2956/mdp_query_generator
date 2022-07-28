use std::collections::HashMap;

use smol_str::SmolStr;
use sqlparser::ast::{
    Expr, Ident, ObjectName, Query, Select, TableFactor,
    BinaryOperator, UnaryOperator, TrimWhereField,
};

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
    /// variables needed during query generation
    pub named_var_stack: HashMap<String, Vec<Variable>>,
}

impl QueryInfo {
    pub fn new() -> QueryInfo {
        QueryInfo {
            relation_generator: RelationGenerator::new(),
            stats: QueryStats::new(),
            named_var_stack: HashMap::<_, _>::new()
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

pub enum Variable {
    SelectLimit(Option<Expr>),
    SelectBody(Select),
    Query(Query),
    LastRelation(TableFactor),
    Val3(Expr),
    IsNullNotFlag(bool),
    TypesValue(Expr),
    IsDistinctNotFlag(bool),
    TypesSelectedType(TypesSelectedType),
    ExistsNotFlag(bool),
    InListNotFlag(bool),
    InSubqueryNotFlag(bool),
    BetweenNotFlag(bool),
    BinaryCompOp(BinaryOperator),
    AnyAllOp(BinaryOperator),
    Array(Expr),
    StringLikeNotFlag(bool),
    BinaryBoolOp(BinaryOperator),
    Numeric(Expr),
    NumericBinaryOp(BinaryOperator),
    NumericUnaryOp(UnaryOperator),
    String(Expr),
    TrimSpecFlag(bool),
    TrimSpecValue(TrimWhereField),
    ColumnSpec(Expr),
    ListExpr(Expr),
}

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
    pub fn get_types(&self) -> Vec<SmolStr> {
        match self {
            TypesSelectedType::Numeric => vec![SmolStr::new("numeric")],
            TypesSelectedType::Val3 => vec![SmolStr::new("3VL Value")],
            TypesSelectedType::Array => vec![SmolStr::new("array")],
            TypesSelectedType::ListExpr => vec![SmolStr::new("list expr")],
            TypesSelectedType::String => vec![SmolStr::new("string")],
            TypesSelectedType::Any => vec![SmolStr::new("string"), SmolStr::new("list expr"), SmolStr::new("array"), SmolStr::new("3VL Value"), SmolStr::new("numeric")],
        }
    }
}

// push value into its stack
macro_rules! push_var {
    ($info: expr, $variant: ident, $value: expr) => {
        if let Some(stack) = $info.named_var_stack.get_mut(stringify!($variant)) {
            stack.push(Variable::$variant($value));
        } else {
            $info.named_var_stack.insert(stringify!($variant).to_string(), vec![Variable::$variant($value)]);
        }
    };
}

// pop value from its stack
macro_rules! pop_var {
    ($info: expr, $variant: ident) => {
        if let Some(stack) = $info.named_var_stack.get_mut(stringify!($variant)) {
            if let Some(Variable::$variant(value)) = stack.pop() {
                value
            } else {
                panic!("Failed to pop variant: {}", stringify!($variant));
            }
        } else {
            panic!("Failed to find variant stack: {}", stringify!($variant));
        }
    };
}

// return mutable reference to the last value in its stack
macro_rules! get_mut_var {
    ($info: expr, $variant: ident) => {
        if let Some(stack) = $info.named_var_stack.get_mut(stringify!($variant)) {
            if let Some(Variable::$variant(value)) = stack.last_mut() {
                value
            } else {
                panic!("Failed to get last mut variant: {}", stringify!($variant));
            }
        } else {
            panic!("Failed to find variant stack: {}", stringify!($variant));
        }
    };
}