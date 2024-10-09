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
    ByteA,
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
            "bytea" => Ok(Self::ByteA),
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
            DataType::Datetime(_) => Self::Timestamp,
            DataType::Timestamp(_, tz) if *tz == TimezoneInfo::None => Self::Timestamp,
            DataType::Numeric(_) |
            DataType::Decimal(_) |
            DataType::Real |
            DataType::Double |
            DataType::Float(_) => Self::Numeric,
            DataType::Custom(id, _) if format!("{}", id).to_lowercase().as_str() == "number" => Self::Numeric,
            DataType::UnsignedMediumInt(_) |
            DataType::UnsignedSmallInt(_) |
            DataType::UnsignedTinyInt(_) |
            DataType::MediumInt(_) |
            DataType::SmallInt(_) |
            DataType::TinyInt(_) |
            DataType::Integer(_) |
            DataType::Int(_) => Self::Integer,
            DataType::Custom(id, _) if format!("{}", id).to_lowercase().as_str() == "year" => Self::Integer,
            DataType::Interval => Self::Interval,
            DataType::BigInt(_) => Self::BigInt,
            DataType::Bool |
            DataType::Boolean => Self::Val3,
            DataType::CharVarying(_) |
            DataType::Varchar(_) |
            DataType::Char(_) |
            DataType::Text => Self::Text,
            DataType::Custom(id, _) if format!("{}", id).to_lowercase().as_str() == "varchar2" => Self::Text,
            DataType::Date => Self::Date,
            DataType::Blob(_) |
            DataType::Bytea => Self::ByteA,
            any => panic!("DataType not implemented: {any} ({:?})", any),
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
            Self::ByteA => "bytea".to_string(),
            Self::Interval => "interval".to_string(),
            Self::Timestamp => "timestamp".to_string(),
        };
        write!(f, "{}", str)
    }
}

impl SubgraphType {
    /// converts type to an llm readable string
    pub fn to_llm_str(&self) -> String {
        match self {
            Self::Undetermined => unimplemented!(),
            Self::ListExpr(_) => unimplemented!(),
            Self::Numeric => "numeric".to_string(),
            Self::Integer => "integer".to_string(),
            Self::BigInt => "bigint".to_string(),
            Self::Val3 => "bool".to_string(),
            Self::Text => "text".to_string(),
            Self::Date => "date".to_string(),
            Self::ByteA => "bytea".to_string(),
            Self::Interval => "interval".to_string(),
            Self::Timestamp => "timestamp".to_string(),
        }
    }
}

pub trait ContainsSubgraphType {
    fn contains_generator_of(&self, tp: &SubgraphType) -> bool;
}

impl ContainsSubgraphType for Vec<SubgraphType> {
    fn contains_generator_of(&self, tp: &SubgraphType) -> bool {
        self.iter().any(|searched_type|
            (*tp).is_same_or_more_determined_or_undetermined(searched_type)
        )
    }
}

impl ContainsSubgraphType for [SubgraphType] {
    fn contains_generator_of(&self, tp: &SubgraphType) -> bool {
        self.iter().any(|searched_type|
            (*tp).is_same_or_more_determined_or_undetermined(searched_type)
        )
    }
}

impl SubgraphType {
    /// Sorted in a way that latter are convertible
    /// to former, if any compatibility exists
    pub fn sort_by_compatibility(mut type_vec: Vec<SubgraphType>) -> Vec<SubgraphType> {
        type_vec.sort_unstable_by(
            |tp1, tp2| 
            if tp1.get_compat_types().contains(tp2) && tp2.get_compat_types().contains(tp1) {
                Ordering::Equal
            } else if tp1.get_compat_types().contains(tp2) {
                Ordering::Greater
            } else if tp2.get_compat_types().contains(tp1) {
                Ordering::Less
            } else {
                Ordering::Equal
            }
        );
        type_vec
    }

    /// filters the selected_types by selected_type
    pub fn filter_by_selected(selected_types: &Vec<SubgraphType>, selected_type: SubgraphType) -> Vec<SubgraphType> {
        match selected_type {
            with_inner @ SubgraphType::ListExpr(..) => {
                selected_types.iter()
                    .map(|x| x.to_owned())
                    .filter(|x| std::mem::discriminant(x) == std::mem::discriminant(&with_inner))
                    .collect::<Vec<_>>()
            }
            any => vec![any]
        }
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