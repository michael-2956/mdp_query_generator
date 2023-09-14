use rand::{SeedableRng, Rng};
use rand_chacha::ChaCha8Rng;
use sqlparser::ast::ObjectName;

use crate::training::ast_to_path::PathNode;

use super::query_info::{DatabaseSchema, CreateTableSt};

pub trait QueryValueChooser {
    fn new() -> Self;

    fn choose_table<'a>(&mut self, database_schema: &'a DatabaseSchema) -> &'a CreateTableSt;

    fn choose_integer(&mut self) -> String;

    fn choose_numeric(&mut self) -> String;
}

pub struct RandomValueChooser {
    rng: ChaCha8Rng,
}

impl QueryValueChooser for RandomValueChooser {
    fn new() -> Self {
        Self {
            rng: ChaCha8Rng::seed_from_u64(1),
        }
    }

    fn choose_table<'a>(&mut self, database_schema: &'a DatabaseSchema) -> &'a CreateTableSt {
        database_schema.get_random_table_def(&mut self.rng)
    }

    fn choose_integer(&mut self) -> String {
        self.rng.gen_range(0..=10).to_string()
    }

    fn choose_numeric(&mut self) -> String {
        self.rng.gen_range(0f64..=10f64).to_string()
    }
}

pub struct DeterministicValueChooser {
    chosen_integers: (Vec<String>, usize),
    chosen_numerics: (Vec<String>, usize),
    chosen_tables: (Vec<ObjectName>, usize),
}

impl DeterministicValueChooser {
    pub fn from_path_nodes(path: &Vec<PathNode>) -> Self {
        Self {
            chosen_integers: (path.iter().filter_map(
                |x| if let PathNode::IntegerValue(value) = x { Some(value) } else { None }
            ).cloned().collect(), 0),
            chosen_numerics: (path.iter().filter_map(
                |x| if let PathNode::NumericValue(value) = x { Some(value) } else { None }
            ).cloned().collect(), 0),
            chosen_tables: (path.iter().filter_map(
                |x| if let PathNode::SelectedTableName(name) = x { Some(name) } else { None }
            ).cloned().collect(), 0),
        }
    }
}

impl QueryValueChooser for DeterministicValueChooser {
    fn new() -> Self {
        Self {
            chosen_integers: (vec![], 0),
            chosen_numerics: (vec![], 0),
            chosen_tables: (vec![], 0),
        }
    }

    fn choose_table<'a>(&mut self, database_schema: &'a DatabaseSchema) -> &'a CreateTableSt {
        let new_table_name = &self.chosen_tables.0[self.chosen_tables.1];
        self.chosen_tables.1 += 1;
        database_schema.get_table_def_by_name(new_table_name)
    }

    fn choose_integer(&mut self) -> String {
        let value = &self.chosen_integers.0[self.chosen_integers.1];
        self.chosen_integers.1 += 1;
        value.clone()
    }

    fn choose_numeric(&mut self) -> String {
        let value = &self.chosen_numerics.0[self.chosen_numerics.1];
        self.chosen_numerics.1 += 1;
        value.clone()
    }
}
