use std::fmt::Display;

use sqlparser::ast::{DataType, ExactNumberInfo};

use crate::unwrap_variant;

use super::error::SyntaxError;

#[derive(Debug, Clone, PartialEq, Eq, Ord, PartialOrd, Hash)]
pub enum SubgraphType {
    /// This type is used if the type is yet to be determined
    Undetermined,
    Numeric,
    Val3,
    ListExpr(Box<SubgraphType>),
    RowExpr(Option<Vec<SubgraphType>>),
    String,
    Date,
    // Query(Option<Vec<(Option<ObjectName>, SubgraphType)>>)
}

impl SubgraphType {
    pub fn wrap_in_func(&self, func_name: &str) -> SubgraphType {
        let outer = SubgraphType::from_func_name(func_name);
        self.wrap_in_type(&outer)
    }

    pub fn wrap_in_type(&self, outer: &SubgraphType) -> SubgraphType {
        match outer {
            SubgraphType::ListExpr(..) => SubgraphType::ListExpr(Box::new(self.clone())),
            any => panic!("Cannot wrap into {any}")
        }
    }

    pub fn from_func_name(func_name: &str) -> Self {
        match func_name {
            "numeric" => SubgraphType::Numeric,
            "VAL_3" => SubgraphType::Val3,
            "list_expr" => SubgraphType::ListExpr(Box::new(SubgraphType::Undetermined)),
            "string" => SubgraphType::String,
            "date" => SubgraphType::Date,
            "row_expr" => SubgraphType::RowExpr(None),
            // "query" => SubgraphType::Query(None),
            any => panic!("Unexpected function name, can't convert to a SubgraphType: {any}")
        }
    }

    pub fn get_subgraph_func_name(&self) -> &str {
        match *self {
            SubgraphType::Numeric => "numeric",
            SubgraphType::Val3 => "VAL_3",
            SubgraphType::ListExpr(..) => "list_expr",
            SubgraphType::String => "string",
            SubgraphType::Date => "date",
            SubgraphType::RowExpr(..) => "row_expr",
            SubgraphType::Undetermined => panic!("SubgraphType::Undetermined does not have an associated subgraph"),
            // SubgraphType::Query(..) => "query",
        }
    }

    pub fn from_type_name(s: &str) -> Result<Self, SyntaxError> {
        match s {
            "numeric" => Ok(SubgraphType::Numeric),
            "3VL Value" => Ok(SubgraphType::Val3),
            "list expr" => Ok(SubgraphType::ListExpr(Box::new(SubgraphType::Undetermined))),
            "string" => Ok(SubgraphType::String),
            "date" => Ok(SubgraphType::Date),
            "row expr" => Ok(SubgraphType::RowExpr(None)),
            // "query" => Ok(SubgraphType::Query(None)),
            any => Err(SyntaxError::new(format!("Type {any} does not exist!")))
        }
    }

    pub fn has_inner(&self) -> bool {
        match self {
            SubgraphType::ListExpr(..) => true,
            _ => false,
        }
    }

    pub fn inner(&self) -> SubgraphType {
        match self {
            SubgraphType::ListExpr(inner) => *inner.clone(),
            any => panic!("{any} has no inner type"),
        }
    }

    pub fn from_data_type(data_type: &DataType) -> Self {
        match data_type {
            DataType::Integer(_) => Self::Numeric,  /// TODO
            DataType::Varchar(_) => Self::String,
            DataType::CharVarying(_) => Self::String,
            DataType::Char(_) => Self::String,
            DataType::Numeric(_) => Self::Numeric,
            DataType::Date => Self::Numeric,  /// TODO
            DataType::Boolean => Self::Val3,
            any => panic!("DataType not implemented: {any}"),
        }
    }

    pub fn is_determined(&self) -> bool {
        match self {
            SubgraphType::Undetermined => false,
            SubgraphType::ListExpr(inner) => inner.is_determined(),
            _ => true
        }
    }

    /// Used only for null type casting. Fails if encounters row expression or "Unndetermined"
    pub fn try_to_data_type(&self) -> Option<DataType> {
        match self {
            SubgraphType::Numeric => Some(DataType::Numeric(ExactNumberInfo::None)),
            SubgraphType::Val3 => Some(DataType::Boolean),
            // TODO: here should be a separate instruction
            // to define a row type.
            // Question 1: When is this behaviour needed?
            // Question 2: How would we design code to handle
            // query-accompanying instructions? (transactions?)
            SubgraphType::RowExpr(..) => None, // Some(DataType::Custom(ObjectName(vec![Ident::new("my_row_type")]), vec![])),
            /// should this even not produce a panic here?
            SubgraphType::ListExpr(..) => None, // Some(DataType::Custom(ObjectName(vec![Ident::new("my_row_type")]), vec![])),
            SubgraphType::String => Some(DataType::Text),
            SubgraphType::Undetermined => None, // panic!("Can't convert SubgraphType::Undetermined to DataType"),
            SubgraphType::Date => Some(DataType::Date),
        }
    }
}

impl From<DataType> for SubgraphType {
    fn from(value: DataType) -> Self {
        SubgraphType::from_data_type(&value)
    }
}

impl Display for SubgraphType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let str = match self {
            SubgraphType::Numeric => "numeric".to_string(),
            SubgraphType::Val3 => "3VL Value".to_string(),
            SubgraphType::ListExpr(inner) => format!("list expr[{}]", inner),
            SubgraphType::String => "string".to_string(),
            SubgraphType::Undetermined => "undetermined".to_string(),
            SubgraphType::Date => "date".to_string(),
            SubgraphType::RowExpr(..) => "row expr".to_string(),
        };
        write!(f, "{}", str)
    }
}

impl SubgraphType {
    /// get a list of compatible types
    pub fn get_compat_types(&self) -> Vec<SubgraphType> {
        let (inner_type, wrapper): (&Box<SubgraphType>, Box<dyn Fn(SubgraphType) -> SubgraphType>) = match self {
            SubgraphType::ListExpr(inner) => (inner, Box::new(|x| SubgraphType::ListExpr(Box::new(x)))),
            any => return vec![any.clone()],
        };
        inner_type.get_compat_types()
            .into_iter()
            .map(wrapper)
            .collect()
    }

    /// checks is self is convertable to other
    pub fn is_compat_with(&self, other: &SubgraphType) -> bool {
        other.is_same_or_more_determined_or_undetermined(self) ||
        self.is_same_or_more_determined_or_undetermined(other) ||
        self.get_compat_types().iter().any(|x| other.is_same_or_more_determined_or_undetermined(x)) ||
        other.get_compat_types().iter().any(|x| self.is_same_or_more_determined_or_undetermined(x))
    }

    /// returns whether type is same, more determined or undetermined at all (NULL)
    pub fn is_same_or_more_determined_or_undetermined(&self, as_what: &SubgraphType) -> bool {
        if *self == SubgraphType::Undetermined {
            return true;
        }
        if *as_what == SubgraphType::Undetermined {
            return true;
        }
        match self {
            SubgraphType::ListExpr(inner) => {
                if matches!(as_what, SubgraphType::ListExpr(..)) {
                    let other_inner = unwrap_variant!(as_what, SubgraphType::ListExpr);
                    **other_inner == SubgraphType::Undetermined || inner.is_same_or_more_determined_or_undetermined(&other_inner)
                } else {
                    false
                }
            },
            any => any == as_what,
        }
    }
}