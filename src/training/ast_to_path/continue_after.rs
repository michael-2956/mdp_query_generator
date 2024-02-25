use std::fmt;

use sqlparser::ast::Expr;

use crate::query_creation::{query_generator::TypeAssertion, state_generator::subgraph_type::SubgraphType};

use super::{ConvertionError, PathGenerator, TypesValue};

#[derive(Clone, Debug, PartialEq)]
pub enum TypesContinueAfter {
    None,
    ExprType(SubgraphType),
}

#[derive(Clone, Debug)]
pub struct SelectContinueAfter {
    current_continue_after_series: Vec<TypesContinueAfter>,
    series_variants: Vec<Vec<TypesContinueAfter>>,
    series_variants_is_complete: Vec<bool>,
    series_variants_index: Vec<usize>,
    types_call_happened: bool,
    length: usize,
}

impl SelectContinueAfter {
    pub fn new(length: usize) -> Self {
        Self {
            current_continue_after_series: vec![TypesContinueAfter::None; length],
            series_variants: vec![vec![TypesContinueAfter::None]; length],
            series_variants_is_complete: vec![false; length],
            series_variants_index: vec![0; length],
            types_call_happened: false,
            length,
        }
    }

    pub fn prepare(&mut self) {
        self.types_call_happened = false;
    }

    pub fn mark_no_types_call_for_index(&mut self, i: usize) {
        assert!(self.series_variants[i].len() == 1);
        self.series_variants_is_complete[i] = true;
    }

    pub fn run_types(
        &mut self, pg: &mut PathGenerator, i: usize, expr: &Expr, type_assertion: TypeAssertion
    ) -> Result<SubgraphType, ConvertionError> {
        self.types_call_happened = true;
        let continue_after = self.current_continue_after_series[i].clone();
        let (continue_after, worth_exploring, res) = match pg.handle_types_continue_after(expr, type_assertion, continue_after) {
            Ok(TypesValue { tp, worth_exploring }) => (Some(TypesContinueAfter::ExprType(tp.clone())), worth_exploring, Ok(tp)),
            Err(err) => (None, false, Err(err)),
        };
        if continue_after.is_some() && worth_exploring {
            let continue_after = continue_after.unwrap();
            if !self.series_variants_is_complete[i] {
                // push possibly succcessful variant
                // which would be confirmed successful by pushing another variant
                self.series_variants[i].push(continue_after);
            } else {
                assert!(self.series_variants[i][
                    self.series_variants_index[i] + 1
                ] == continue_after);
            }
        } else {
            if !self.series_variants_is_complete[i] {
                // pop unsuccessful variant
                self.series_variants[i].pop();
                self.series_variants_is_complete[i] = true;
            } else { unreachable!(); }
        }
        res
    }

    pub fn update_current_series(&mut self) -> Result<(), ConvertionError> {
        if !self.types_call_happened {
            return Err(ConvertionError::new(format!(
                "Select did not iterate through the variants of\
                the types of the projection items"
            )))
        }
        if let Some(i) = self.series_variants.iter().position(|vs| vs.is_empty()) {
            return Err(ConvertionError::new(format!(
                "Select did not iterate through the variants of\
                the types of the projection items, because the item\
                at index {i} did not succeed for TypesContinueAfter::None"
            )))
        }
        for i in 0..self.length {
            if self.series_variants_is_complete[i] {
                if (self.series_variants_index[i] + 1) < self.series_variants[i].len() {
                    // if we know all variants, and can update index, update it
                    self.series_variants_index[i] += 1;
                    self.current_continue_after_series[i] = self.series_variants[i][
                        self.series_variants_index[i]
                    ].clone();
                    return Ok(())
                } else {
                    // if we know all variants, and can't update index, this is an overflow,
                    // so we should reset index and continue
                    self.series_variants_index[i] = 0;
                    self.current_continue_after_series[i] = self.series_variants[i][0].clone();
                }
            } else {
                // if we don't know all the variants for i, this means that
                // a new untested continue_after was just added to the variants list,
                // and we should test is out
                self.series_variants_index[i] += 1;
                self.current_continue_after_series[i] = self.series_variants[i][
                    self.series_variants_index[i]
                ].clone();
                return Ok(())
            }
        }
        Err(ConvertionError::new(format!(
            "SELECT exhausted the variants for types of the projection items"
        )))
    }
}

impl fmt::Display for SelectContinueAfter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self.current_continue_after_series)
    }
}
