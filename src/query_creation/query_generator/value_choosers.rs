use rand::{SeedableRng, Rng, seq::SliceRandom};
use rand_chacha::ChaCha8Rng;
use sqlparser::ast::{ObjectName, Ident};

use crate::{training::ast_to_path::PathNode, query_creation::state_generator::subgraph_type::SubgraphType};

use super::{query_info::{DatabaseSchema, CreateTableSt, FromContents, Relation, GroupByContents}, call_modifiers::WildcardRelationsValue};

pub trait QueryValueChooser {
    fn new() -> Self;

    fn choose_table<'a>(&mut self, database_schema: &'a DatabaseSchema) -> &'a CreateTableSt;

    fn choose_column_from(&mut self, from_contents: &FromContents, column_types: &Vec<SubgraphType>, qualified: bool) -> (SubgraphType, Vec<Ident>);

    fn choose_column_group_by(&mut self, group_by_contents: &GroupByContents, column_types: &Vec<SubgraphType>) -> (SubgraphType, Vec<Ident>);

    fn choose_integer(&mut self) -> String;

    fn choose_numeric(&mut self) -> String;

    fn choose_qualified_wildcard_relation<'a>(&mut self, from_contents: &'a FromContents, wildcard_relations: &WildcardRelationsValue) -> (Ident, &'a Relation);
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

    fn choose_column_from(&mut self, from_contents: &FromContents, column_types: &Vec<SubgraphType>, qualified: bool) -> (SubgraphType, Vec<Ident>) {
        from_contents.get_random_column_with_type_of(&mut self.rng, column_types, qualified)
    }

    fn choose_column_group_by(&mut self, group_by_contents: &GroupByContents, column_types: &Vec<SubgraphType>) -> (SubgraphType, Vec<Ident>) {
        group_by_contents.get_random_column_with_type_of(&mut self.rng, column_types)
    }

    fn choose_integer(&mut self) -> String {
        self.rng.gen_range(0..=5).to_string()
    }

    fn choose_numeric(&mut self) -> String {
        self.rng.gen_range(0f64..=5f64).to_string()
    }

    fn choose_qualified_wildcard_relation<'a>(&mut self, from_contents: &'a FromContents, wildcard_relations: &WildcardRelationsValue) -> (Ident, &'a Relation) {
        let alias = wildcard_relations.wildcard_selectable_relations.choose(&mut self.rng).unwrap();
        let relation = from_contents.get_relation_by_name(alias);
        (alias.clone(), relation)
    }
}

pub struct DeterministicValueChooser {
    chosen_integers: (Vec<String>, usize),
    chosen_numerics: (Vec<String>, usize),
    chosen_tables: (Vec<ObjectName>, usize),
    chosen_columns_from: (Vec<Vec<Ident>>, usize),
    chosen_columns_group_by: (Vec<Vec<Ident>>, usize),
    chosen_qualified_wildcard_tables: (Vec<Ident>, usize),
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
            chosen_columns_from: (path.iter().filter_map(
                |x| if let PathNode::SelectedColumnNameFROM(ident_components) = x { Some(ident_components) } else { None }
            ).cloned().collect(), 0),
            chosen_columns_group_by: (path.iter().filter_map(
                |x| if let PathNode::SelectedColumnNameGROUPBY(ident) = x { Some(ident) } else { None }
            ).cloned().collect(), 0),
            chosen_qualified_wildcard_tables: (path.iter().filter_map(
                |x| if let PathNode::QualifiedWildcardSelectedRelation(ident) = x { Some(ident) } else { None }
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
            chosen_columns_from: (vec![], 0),
            chosen_columns_group_by: (vec![], 0),
            chosen_qualified_wildcard_tables: (vec![], 0),
        }
    }

    fn choose_table<'a>(&mut self, database_schema: &'a DatabaseSchema) -> &'a CreateTableSt {
        let new_table_name = &self.chosen_tables.0[self.chosen_tables.1];
        self.chosen_tables.1 += 1;
        database_schema.get_table_def_by_name(new_table_name)
    }

    fn choose_column_from(&mut self, from_contents: &FromContents, column_types: &Vec<SubgraphType>, qualified: bool) -> (SubgraphType, Vec<Ident>) {
        let ident_components = &self.chosen_columns_from.0[self.chosen_columns_from.1];
        self.chosen_columns_from.1 += 1;
        let col_type = from_contents.get_column_type_by_ident_components(ident_components);
        if !column_types.contains(&col_type) {
            panic!("column_types = {:?} does not contain col_type = {:?}", column_types, col_type)
        }
        if ident_components.len() != (if qualified { 2 } else { 1 }) {
            panic!("qualified is {qualified} but ident_components has {} elements: {:?}", ident_components.len(), ident_components)
        }
        (col_type, ident_components.clone())
    }

    fn choose_column_group_by(&mut self, group_by_contents: &GroupByContents, column_types: &Vec<SubgraphType>) -> (SubgraphType, Vec<Ident>) {
        let ident_components = &self.chosen_columns_group_by.0[self.chosen_columns_group_by.1];
        self.chosen_columns_group_by.1 += 1;
        let col_type = group_by_contents.get_column_type_by_ident_components(ident_components);
        if !column_types.contains(&col_type) {
            panic!("column_types = {:?} does not contain col_type = {:?}", column_types, col_type)
        }
        (col_type, ident_components.clone())
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

    fn choose_qualified_wildcard_relation<'a>(&mut self, from_contents: &'a FromContents, wildcard_relations: &WildcardRelationsValue) -> (Ident, &'a Relation) {
        let alias = &self.chosen_qualified_wildcard_tables.0[self.chosen_qualified_wildcard_tables.1];
        if !wildcard_relations.wildcard_selectable_relations.contains(alias) {
            panic!("Relation cannot be selected by wildcard in this context: wildcard_relations = {:?}, rel. alias: {alias}", wildcard_relations)
        }
        self.chosen_qualified_wildcard_tables.1 += 1;
        let relation = from_contents.get_relation_by_name(alias);
        (alias.clone(), relation)
    }
}
