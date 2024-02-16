use std::{cmp::Ordering, fmt::Display};

use serde::{Serialize, Deserialize};
use sqlparser::ast::{DataType, ExactNumberInfo, TimezoneInfo};

use crate::unwrap_variant;

use super::error::SyntaxError;

#[derive(Debug, Clone, PartialEq, Eq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub enum SubgraphType {
    /// This type is used if the type is yet to be determined
    Undetermined,
    ListExpr(Box<SubgraphType>),
    Timestamp,
    Interval,
    Numeric,
    Integer,
    BigInt,
    Val3,
    Text,
    Date,
}

impl SubgraphType {
    pub fn from_type_name(s: &str) -> Result<Self, SyntaxError> {
        match s {
            "numeric" => Ok(Self::Numeric),
            "integer" => Ok(Self::Integer),
            "bigint" => Ok(Self::BigInt),
            "3VL Value" => Ok(Self::Val3),
            "list expr" => Ok(Self::ListExpr(Box::new(Self::Undetermined))),
            "text" => Ok(Self::Text),
            "date" => Ok(Self::Date),
            "interval" => Ok(Self::Interval),
            "timestamp" => Ok(Self::Timestamp),
            any => Err(SyntaxError::new(format!("Type {any} does not exist!")))
        }
    }

    pub fn has_inner(&self) -> bool {
        match self {
            Self::ListExpr(..) => true,
            _ => false,
        }
    }

    pub fn inner(&self) -> SubgraphType {
        match self {
            Self::ListExpr(inner) => *inner.clone(),
            any => panic!("{any} has no inner type"),
        }
    }

    pub fn from_data_type(data_type: &DataType) -> Self {
        match data_type {
            DataType::Timestamp(_, tz) if *tz == TimezoneInfo::None => Self::Timestamp,
            DataType::CharVarying(_) => Self::Text,
            DataType::Numeric(_) => Self::Numeric,
            DataType::Integer(_) => Self::Integer,
            DataType::Interval => Self::Interval,
            DataType::BigInt(_) => Self::BigInt,
            DataType::Varchar(_) => Self::Text,
            DataType::Boolean => Self::Val3,
            DataType::Char(_) => Self::Text,
            DataType::Text => Self::Text,
            DataType::Date => Self::Date,
            any => panic!("DataType not implemented: {any}"),
        }
    }

    pub fn is_determined(&self) -> bool {
        match self {
            Self::Undetermined => false,
            Self::ListExpr(inner) => inner.is_determined(),
            _ => true
        }
    }

    /// Used only for null type casting. Fails if encounters "Undetermined"
    pub fn to_data_type(&self) -> DataType {
        match self {
            Self::Numeric => DataType::Numeric(ExactNumberInfo::None),
            Self::Integer => DataType::Integer(None),
            Self::BigInt => DataType::BigInt(None),
            Self::Val3 => DataType::Boolean,
            Self::Text => DataType::Text,
            Self::Date => DataType::Date,
            Self::Interval => DataType::Interval,
            Self::Timestamp => DataType::Timestamp(None, TimezoneInfo::None),
            any => panic!("Can't convert {any} to DataType"),
        }
    }
}

impl Display for SubgraphType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let str = match self {
            Self::Undetermined => "undetermined".to_string(),
            Self::ListExpr(inner) => format!("list expr[{}]", inner),
            Self::Numeric => "numeric".to_string(),
            Self::Integer => "integer".to_string(),
            Self::BigInt => "bigint".to_string(),
            Self::Val3 => "3VL Value".to_string(),
            Self::Text => "text".to_string(),
            Self::Date => "date".to_string(),
            Self::Interval => "interval".to_string(),
            Self::Timestamp => "timestamp".to_string(),
        };
        write!(f, "{}", str)
    }
}

impl SubgraphType {
    /// Sorted in a way that latter are convertible
    /// to former, if any compatibility exists
    pub fn sort_by_compatibility(mut type_vec: Vec<SubgraphType>) -> Vec<SubgraphType> {
        type_vec.sort_unstable_by(
            |tp1, tp2| 
            if tp1.get_compat_types().contains(tp2) {
                Ordering::Greater
            } else if tp2.get_compat_types().contains(tp1) {
                Ordering::Less
            } else {
                Ordering::Equal
            }
        );
        type_vec
    }

    /// get a list of compatible types\
    /// if the returned vector includes the needed type, this type is compatible
    pub fn get_compat_types(&self) -> Vec<SubgraphType> {
        let (inner_type, wrapper): (&Box<SubgraphType>, Box<dyn Fn(SubgraphType) -> SubgraphType>) = match self {
            SubgraphType::ListExpr(inner) => (inner, Box::new(|x| SubgraphType::ListExpr(Box::new(x)))),
            SubgraphType::Numeric => return vec![SubgraphType::Numeric, SubgraphType::BigInt, SubgraphType::Integer],
            SubgraphType::BigInt => return vec![SubgraphType::BigInt, SubgraphType::Integer],
            SubgraphType::Timestamp => return vec![SubgraphType::Timestamp, SubgraphType::Date],
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