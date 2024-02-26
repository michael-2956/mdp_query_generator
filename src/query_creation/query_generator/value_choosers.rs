use rand::{distributions::{Distribution, WeightedIndex}, seq::SliceRandom, Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use sqlparser::ast::{DateTimeField, Ident, ObjectName};

use crate::{training::ast_to_path::PathNode, query_creation::state_generator::subgraph_type::SubgraphType};

use super::{call_modifiers::WildcardRelationsValue, query_info::{CheckAccessibility, ClauseContext, ColumnRetrievalOptions, CreateTableSt, DatabaseSchema, FromContents, IdentName, Relation}};

pub trait QueryValueChooser {
    fn new() -> Self;

    fn choose_table<'a>(&mut self, database_schema: &'a DatabaseSchema) -> &'a CreateTableSt;

    fn choose_column(&mut self, clause_context: &ClauseContext, column_types: Vec<SubgraphType>, check_accessibility: CheckAccessibility, column_retrieval_options: ColumnRetrievalOptions) -> (SubgraphType, [IdentName; 2]);

    fn choose_select_alias_order_by(&mut self, aliases: &Vec<&IdentName>) -> Ident;

    fn choose_aggregate_function_name<'a>(&mut self, func_names_iter: impl Iterator::<Item = &'a String>, dist: WeightedIndex<f64>) -> ObjectName;

    fn choose_bigint(&mut self) -> String;
    
    fn choose_integer(&mut self) -> String;

    fn choose_numeric(&mut self) -> String;

    fn choose_string(&mut self) -> String;

    fn choose_date(&mut self) -> String;

    fn choose_timestamp(&mut self) -> String;

    fn choose_interval(&mut self, with_field: bool) -> (String, Option<DateTimeField>);

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

    fn choose_column(&mut self, clause_context: &ClauseContext, column_types: Vec<SubgraphType>, check_accessibility: CheckAccessibility, column_retrieval_options: ColumnRetrievalOptions) -> (SubgraphType, [IdentName; 2]) {
        let column_levels = clause_context.get_non_empty_column_levels_by_types(column_types.clone(), check_accessibility, column_retrieval_options.clone());
        let columns = *column_levels.choose(&mut self.rng).as_ref().unwrap();
        let (col_tp, [rel_name, col_name]) = *columns.choose(&mut self.rng).as_ref().unwrap();
        // if **rel_name == Ident::new("T2").into() && **col_name == Ident::new("case").into() {
        //     eprintln!("selected {} by {:#?} with {:?} from {:#?}", ObjectName([(*rel_name).clone().into(), (*col_name).clone().into()].to_vec()), check_accessibility, column_types, column_levels);
        //     clause_context.eprint_clause_hierarchy();
        // }
        ((*col_tp).clone(), [(*rel_name).clone(), (*col_name).clone()])
    }

    fn choose_select_alias_order_by(&mut self, aliases: &Vec<&IdentName>) -> Ident {
        (**aliases.choose(&mut self.rng).as_ref().unwrap()).clone().into()
    }

    fn choose_aggregate_function_name<'a>(&mut self, mut func_names_iter: impl Iterator::<Item = &'a String>, dist: WeightedIndex<f64>) -> ObjectName {
        let selected_name = func_names_iter.nth(dist.sample(&mut self.rng)).unwrap();
        ObjectName(vec![Ident {
            value: selected_name.clone(),
            quote_style: (None),
        }])
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

    fn choose_string(&mut self) -> String {
        "HJeihfbwei".to_string()
    }

    fn choose_date(&mut self) -> String {
        "2023-08-27".to_string()
    }

    fn choose_timestamp(&mut self) -> String {
        "2023-01-30 14:37:05".to_string()
    }

    fn choose_interval(&mut self, with_field: bool) -> (String, Option<DateTimeField>) {
        if with_field {
            ("1".to_string(), Some(DateTimeField::Day))
        } else { ("1 day".to_string(), None) }
    }

    fn choose_qualified_wildcard_relation<'a>(&mut self, from_contents: &'a FromContents, wildcard_relations: &WildcardRelationsValue) -> (Ident, &'a Relation) {
        let alias = wildcard_relations.relations_selectable_by_qualified_wildcard.choose(&mut self.rng).unwrap();
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

struct VecWithIndex<Tp> where Tp: Clone {
    v: Vec<Tp>,
    ind: usize
}

impl<Tp> VecWithIndex<Tp> where Tp: Clone {
    fn new() -> Self {
        Self {
            v: vec![],
            ind: 0
        }
    }

    fn with_vec(v: Vec<Tp>) -> Self {
        Self { v, ind: 0 }
    }

    fn next(&mut self) -> Tp {
        let ret = &self.v[self.ind];
        self.ind += 1;
        ret.clone()
    }

    fn next_ref(&mut self) -> &Tp {
        let ret = &self.v[self.ind];
        self.ind += 1;
        ret
    }

    fn reset(&mut self) {
        self.ind = 0;
    }
}

macro_rules! init_from_nodes {
    ($nodes: expr, $t: ty, $v: ident) => {{
        VecWithIndex::<$t>::with_vec($nodes.iter().filter_map(
            |x| if let PathNode::$v(value) = x { Some(value) } else { None }
        ).cloned().collect())
    }};
}

pub struct DeterministicValueChooser {
    chosen_bigints: VecWithIndex<String>,
    chosen_integers: VecWithIndex<String>,
    chosen_numerics: VecWithIndex<String>,
    chosen_strings: VecWithIndex<String>,
    chosen_dates: VecWithIndex<String>,
    chosen_timestamps: VecWithIndex<String>,
    chosen_intervals: VecWithIndex<(String, Option<DateTimeField>)>,
    chosen_tables: VecWithIndex<ObjectName>,
    chosen_select_aliases: VecWithIndex<Ident>,
    chosen_columns: VecWithIndex<[IdentName; 2]>,
    chosen_order_by_select_refs: VecWithIndex<Ident>,
    chosen_aggregate_functions: VecWithIndex<ObjectName>,
    chosen_qualified_wildcard_tables: VecWithIndex<Ident>,
}

impl DeterministicValueChooser {
    pub fn from_path_nodes(path: &Vec<PathNode>) -> Self {
        Self {
            chosen_bigints: init_from_nodes!(path, String, BigIntValue),
            chosen_integers: init_from_nodes!(path, String, IntegerValue),
            chosen_numerics: init_from_nodes!(path, String, NumericValue),
            chosen_strings: init_from_nodes!(path, String, StringValue),
            chosen_dates: init_from_nodes!(path, String, DateValue),
            chosen_timestamps: init_from_nodes!(path, String, TimestampValue),
            chosen_intervals: init_from_nodes!(path, (String, Option<DateTimeField>), IntervalValue),
            chosen_tables: init_from_nodes!(path, ObjectName, SelectedTableName),
            chosen_select_aliases: init_from_nodes!(path, Ident, SelectAlias),
            chosen_columns: init_from_nodes!(path, [IdentName; 2], SelectedColumnName),
            chosen_order_by_select_refs: init_from_nodes!(path, Ident, ORDERBYSelectReference),
            chosen_aggregate_functions: init_from_nodes!(path, ObjectName, SelectedAggregateFunctions),
            chosen_qualified_wildcard_tables: init_from_nodes!(path, Ident, QualifiedWildcardSelectedRelation),
        }
    }
}

impl QueryValueChooser for DeterministicValueChooser {
    fn new() -> Self {
        Self {
            chosen_bigints: VecWithIndex::new(),
            chosen_integers: VecWithIndex::new(),
            chosen_numerics: VecWithIndex::new(),
            chosen_strings: VecWithIndex::new(),
            chosen_dates: VecWithIndex::new(),
            chosen_timestamps: VecWithIndex::new(),
            chosen_intervals: VecWithIndex::new(),
            chosen_tables: VecWithIndex::new(),
            chosen_select_aliases: VecWithIndex::new(),
            chosen_columns: VecWithIndex::new(),
            chosen_order_by_select_refs: VecWithIndex::new(),
            chosen_aggregate_functions: VecWithIndex::new(),
            chosen_qualified_wildcard_tables: VecWithIndex::new(),
        }
    }

    fn choose_table<'a>(&mut self, database_schema: &'a DatabaseSchema) -> &'a CreateTableSt {
        let new_table_name = self.chosen_tables.next_ref();
        database_schema.get_table_def_by_name(new_table_name)
    }

    fn choose_column(&mut self, clause_context: &ClauseContext, column_types: Vec<SubgraphType>, check_accessibility: CheckAccessibility, column_retrieval_options: ColumnRetrievalOptions) -> (SubgraphType, [IdentName; 2]) {
        let qualified_column_name = self.chosen_columns.next();
        let ident_components: Vec<Ident> = if check_accessibility == CheckAccessibility::ColumnName {
            vec![qualified_column_name.iter().last().cloned().unwrap().into()]
        } else {
            qualified_column_name.iter().cloned().map(IdentName::into).collect()
        };
        let (col_tp, retrieved_column_name) = clause_context.retrieve_column_by_ident_components(
            &ident_components, column_retrieval_options
        ).unwrap();
        assert!(column_types.iter().any(|tp| col_tp.is_same_or_more_determined_or_undetermined(&tp)));
        assert!(qualified_column_name == retrieved_column_name);
        (col_tp, retrieved_column_name)
    }

    fn choose_select_alias_order_by(&mut self, aliases: &Vec<&IdentName>) -> Ident {
        let ident = self.chosen_order_by_select_refs.next();
        if !aliases.contains(&&ident.clone().into()) {
            panic!("Cannot choose {ident} in ORDER BY: it is not present among SELECT aliases: {:?}", aliases);
        }
        ident
    }

    fn choose_aggregate_function_name<'a>(&mut self, mut func_names_iter: impl Iterator::<Item = &'a String>, _dist: WeightedIndex<f64>) -> ObjectName {
        let aggr_func = self.chosen_aggregate_functions.next();
        assert!(func_names_iter.any(|func_name| *func_name == aggr_func.0[0].value.to_uppercase()), "Selected aggregate function is not available: {}", aggr_func);
        aggr_func
    }
    
    fn choose_bigint(&mut self) -> String { self.chosen_bigints.next() }
    
    fn choose_integer(&mut self) -> String { self.chosen_integers.next() }
    
    fn choose_numeric(&mut self) -> String { self.chosen_numerics.next() }
    
    fn choose_string(&mut self) -> String { self.chosen_strings.next() }
    
    fn choose_date(&mut self) -> String { self.chosen_dates.next() }

    fn choose_timestamp(&mut self) -> String { self.chosen_timestamps.next() }

    fn choose_interval(&mut self, with_field: bool) -> (String, Option<DateTimeField>) {
        let (value, field) = self.chosen_intervals.next();
        if with_field ^ field.is_some() {
            panic!("with_field is {with_field} but the field is set to {:?}", field)
        }
        (value, field)
    }

    fn choose_qualified_wildcard_relation<'a>(&mut self, from_contents: &'a FromContents, wildcard_relations: &WildcardRelationsValue) -> (Ident, &'a Relation) {
        let alias = self.chosen_qualified_wildcard_tables.next_ref();
        if !wildcard_relations.relations_selectable_by_qualified_wildcard.contains(alias) {
            panic!("Relation cannot be selected by wildcard in this context: wildcard_relations = {:?}, rel. alias: {alias}", wildcard_relations)
        }
        let relation = from_contents.get_relation_by_name(alias);
        (alias.clone(), relation)
    }

    fn choose_select_alias(&mut self) -> Ident { self.chosen_select_aliases.next() }

    fn reset(&mut self) {
        self.chosen_bigints.reset();
        self.chosen_integers.reset();
        self.chosen_numerics.reset();
        self.chosen_strings.reset();
        self.chosen_dates.reset();
        self.chosen_intervals.reset();
        self.chosen_tables.reset();
        self.chosen_select_aliases.reset();
        self.chosen_columns.reset();
        self.chosen_order_by_select_refs.reset();
        self.chosen_qualified_wildcard_tables.reset();
    }
}
