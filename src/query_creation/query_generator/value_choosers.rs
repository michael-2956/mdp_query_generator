use rand::{SeedableRng, Rng, seq::SliceRandom};
use rand_chacha::ChaCha8Rng;
use sqlparser::ast::{ObjectName, Ident};

use crate::{training::ast_to_path::PathNode, query_creation::state_generator::subgraph_type::SubgraphType};

use super::{call_modifiers::WildcardRelationsValue, query_info::{CreateTableSt, DatabaseSchema, FromContents, GroupByContents, IdentName, Relation}};

pub trait QueryValueChooser {
    fn new() -> Self;

    fn choose_table<'a>(&mut self, database_schema: &'a DatabaseSchema) -> &'a CreateTableSt;

    fn choose_column_from(&mut self, from_contents: &FromContents, column_types: &Vec<SubgraphType>, qualified: bool) -> (SubgraphType, Vec<Ident>);

    fn choose_column_group_by(&mut self, from_contents: &FromContents, group_by_contents: &GroupByContents, column_types: &Vec<SubgraphType>, qualified: bool) -> (SubgraphType, Vec<Ident>);
    
    fn choose_select_alias_order_by(&mut self, aliases: &Vec<&IdentName>) -> Ident;

    fn choose_bigint(&mut self) -> String;
    
    fn choose_integer(&mut self) -> String;

    fn choose_numeric(&mut self) -> String;

    fn choose_qualified_wildcard_relation<'a>(&mut self, from_contents: &'a FromContents, wildcard_relations: &WildcardRelationsValue) -> (Ident, &'a Relation);

    fn choose_select_alias(&mut self) -> Ident;

    fn reset(&mut self);
}

pub struct RandomValueChooser {
    rng: ChaCha8Rng,
    free_projection_alias_index: u32,
}

impl QueryValueChooser for RandomValueChooser {
    fn new() -> Self {
        Self {
            rng: ChaCha8Rng::seed_from_u64(1),
            free_projection_alias_index: 0,
        }
    }

    fn choose_table<'a>(&mut self, database_schema: &'a DatabaseSchema) -> &'a CreateTableSt {
        database_schema.get_random_table_def(&mut self.rng)
    }

    fn choose_column_from(&mut self, from_contents: &FromContents, column_types: &Vec<SubgraphType>, qualified: bool) -> (SubgraphType, Vec<Ident>) {
        from_contents.get_random_column_with_type_of(&mut self.rng, column_types, qualified, None)
    }

    fn choose_column_group_by(&mut self, from_contents: &FromContents, group_by_contents: &GroupByContents, column_types: &Vec<SubgraphType>, qualified: bool) -> (SubgraphType, Vec<Ident>) {
        if !qualified {
            from_contents.get_random_column_with_type_of(&mut self.rng, column_types, qualified,
                Some(group_by_contents.get_column_name_set())
            )
        } else {
            group_by_contents.get_random_column_with_type_of(&mut self.rng, column_types)
        }
    }

    fn choose_select_alias_order_by(&mut self, aliases: &Vec<&IdentName>) -> Ident {
        (**aliases.choose(&mut self.rng).as_ref().unwrap()).clone().into()
    }

    fn choose_bigint(&mut self) -> String {
        self.rng.gen_range(0..=5).to_string()
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

    fn choose_select_alias(&mut self) -> Ident {
        let name = format!("C{}", self.free_projection_alias_index);
        self.free_projection_alias_index += 1;
        Ident { value: name.clone(), quote_style: None }
    }

    fn reset(&mut self) {
        self.free_projection_alias_index = 0;
    }
}

pub struct DeterministicValueChooser {
    chosen_bigints: (Vec<String>, usize),
    chosen_integers: (Vec<String>, usize),
    chosen_numerics: (Vec<String>, usize),
    chosen_tables: (Vec<ObjectName>, usize),
    chosen_select_aliases: (Vec<Ident>, usize),
    chosen_columns_from: (Vec<Vec<Ident>>, usize),
    chosen_columns_group_by: (Vec<Vec<Ident>>, usize),
    chosen_columns_order_by: (Vec<Ident>, usize),
    chosen_qualified_wildcard_tables: (Vec<Ident>, usize),
}

impl DeterministicValueChooser {
    pub fn from_path_nodes(path: &Vec<PathNode>) -> Self {
        Self {
            chosen_bigints: (path.iter().filter_map(
                |x| if let PathNode::BigIntValue(value) = x { Some(value) } else { None }
            ).cloned().collect(), 0),
            chosen_integers: (path.iter().filter_map(
                |x| if let PathNode::IntegerValue(value) = x { Some(value) } else { None }
            ).cloned().collect(), 0),
            chosen_numerics: (path.iter().filter_map(
                |x| if let PathNode::NumericValue(value) = x { Some(value) } else { None }
            ).cloned().collect(), 0),
            chosen_tables: (path.iter().filter_map(
                |x| if let PathNode::SelectedTableName(name) = x { Some(name) } else { None }
            ).cloned().collect(), 0),
            chosen_select_aliases: (path.iter().filter_map(
                |x| if let PathNode::SelectAlias(value) = x { Some(value) } else { None }
            ).cloned().collect(), 0),
            chosen_columns_from: (path.iter().filter_map(
                |x| if let PathNode::SelectedColumnNameFROM(ident_components) = x { Some(ident_components) } else { None }
            ).cloned().collect(), 0),
            chosen_columns_group_by: (path.iter().filter_map(
                |x| if let PathNode::SelectedColumnNameGROUPBY(ident) = x { Some(ident) } else { None }
            ).cloned().collect(), 0),
            chosen_columns_order_by: (path.iter().filter_map(
                |x| if let PathNode::SelectedColumnNameORDERBY(ident) = x { Some(ident) } else { None }
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
            chosen_bigints: (vec![], 0),
            chosen_integers: (vec![], 0),
            chosen_numerics: (vec![], 0),
            chosen_tables: (vec![], 0),
            chosen_select_aliases: (vec![], 0),
            chosen_columns_from: (vec![], 0),
            chosen_columns_group_by: (vec![], 0),
            chosen_columns_order_by: (vec![], 0),
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
        let col_type = from_contents.get_column_type_by_ident_components(ident_components).unwrap();
        if !column_types.contains(&col_type) {
            panic!("column_types = {:?} does not contain col_type = {:?}", column_types, col_type)
        }
        if ident_components.len() != (if qualified { 2 } else { 1 }) {
            panic!("qualified is {qualified} but ident_components has {} elements: {:?}", ident_components.len(), ident_components)
        }
        (col_type, ident_components.clone())
    }

    fn choose_column_group_by(&mut self, _from_contents: &FromContents, group_by_contents: &GroupByContents, column_types: &Vec<SubgraphType>, _qualified: bool) -> (SubgraphType, Vec<Ident>) {
        let ident_components = self.chosen_columns_group_by.0[self.chosen_columns_group_by.1].clone();
        self.chosen_columns_group_by.1 += 1;
        let col_type = group_by_contents.get_column_type_by_ident_components(&ident_components).unwrap();
        if !column_types.contains(&col_type) {
            panic!("column_types = {:?} does not contain col_type = {:?}", column_types, col_type)
        }
        (col_type, ident_components)
    }

    fn choose_select_alias_order_by(&mut self, aliases: &Vec<&IdentName>) -> Ident {
        let ident = self.chosen_columns_order_by.0[self.chosen_columns_order_by.1].clone();
        self.chosen_columns_order_by.1 += 1;
        if !aliases.contains(&&ident.clone().into()) {
            panic!("Cannot choose {ident} in ORDER BY: it is not present among SELECT aliases: {:?}", aliases);
        }
        ident
    }

    fn choose_bigint(&mut self) -> String {
        let value = &self.chosen_bigints.0[self.chosen_bigints.1];
        self.chosen_bigints.1 += 1;
        value.clone()
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

    fn choose_select_alias(&mut self) -> Ident {
        let value = &self.chosen_select_aliases.0[self.chosen_select_aliases.1];
        self.chosen_select_aliases.1 += 1;
        value.clone()
    }

    fn reset(&mut self) {
        self.chosen_integers.1 = 0;
        self.chosen_numerics.1 = 0;
        self.chosen_tables.1 = 0;
        self.chosen_select_aliases.1 = 0;
        self.chosen_columns_from.1 = 0;
        self.chosen_columns_group_by.1 = 0;
        self.chosen_qualified_wildcard_tables.1 = 0;
    }
}
