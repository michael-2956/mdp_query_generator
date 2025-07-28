use std::sync::atomic::AtomicPtr;

use rand::{distributions::{Distribution, WeightedIndex}, seq::SliceRandom, Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use sqlparser::ast::{DateTimeField, Ident, ObjectName, Query};

use crate::{query_creation::state_generator::subgraph_type::{ContainsSubgraphType, SubgraphType}, training::{ast_to_path::PathNode, models::DecisionResult}};

use super::{call_modifiers::WildcardRelationsValue, query_info::{CheckAccessibility, ClauseContext, ColumnRetrievalOptions, IdentName, Relation}};

pub trait QueryValueChooser: Send + Sync {
    /// choose table name (from those avilable in schema)
    fn choose_table_name(&mut self, available_table_names: &Vec<ObjectName>) -> DecisionResult<ObjectName>;

    /// choose column name and relation it comes from based on criteria
    fn choose_column(&mut self, clause_context: &ClauseContext, column_types: Vec<SubgraphType>, check_accessibility: CheckAccessibility, column_retrieval_options: ColumnRetrievalOptions) -> DecisionResult<(SubgraphType, [IdentName; 2])>;

    /// choose unique select identifiers (aliased or not) to place in ORDER BY
    fn choose_select_ident_for_order_by(&mut self, aliases: &Vec<&IdentName>) -> DecisionResult<Ident>;

    fn choose_aggregate_function_name(&mut self, func_names: Vec<&String>, dist: WeightedIndex<f64>) -> DecisionResult<ObjectName>;

    fn choose_bigint(&mut self) -> DecisionResult<String>;
    
    fn choose_integer(&mut self) -> DecisionResult<String>;

    fn choose_numeric(&mut self) -> DecisionResult<String>;

    fn choose_text(&mut self) -> DecisionResult<String>;

    fn choose_date(&mut self) -> DecisionResult<String>;

    fn choose_timestamp(&mut self) -> DecisionResult<String>;

    fn choose_interval(&mut self, with_field: bool) -> DecisionResult<(String, Option<DateTimeField>)>;

    fn choose_qualified_wildcard_relation<'a>(&mut self, clause_context: &'a ClauseContext, wildcard_relations: &WildcardRelationsValue) -> DecisionResult<(Ident, &'a Relation)>;

    fn choose_select_alias(&mut self) -> DecisionResult<Ident>;

    fn choose_from_alias(&mut self) -> DecisionResult<Ident>;

    fn choose_from_column_renames(&mut self, n_columns: usize) -> DecisionResult<Vec<Ident>>;

    fn reset(&mut self);

    /// set the current query AST for the choice that's going to be taken next
    fn set_choice_query_ast(&mut self, _current_query_ast_ptr_opt: &mut Option<AtomicPtr<Query>>) { }
}

pub struct RandomValueChooser {
    rng: ChaCha8Rng,
    free_select_alias_index: usize,
    free_from_rename_index: usize,
    free_from_alias_index: usize,
}

impl QueryValueChooser for RandomValueChooser {
    fn choose_table_name(&mut self, available_table_names: &Vec<ObjectName>) -> DecisionResult<ObjectName> {
        Ok(available_table_names[self.rng.gen_range(0..available_table_names.len())].clone())
    }

    fn choose_column(&mut self, clause_context: &ClauseContext, column_types: Vec<SubgraphType>, check_accessibility: CheckAccessibility, column_retrieval_options: ColumnRetrievalOptions) -> DecisionResult<(SubgraphType, [IdentName; 2])> {
        let column_levels = clause_context.get_non_empty_column_levels_by_types(column_types.clone(), check_accessibility, column_retrieval_options.clone());
        let columns = *column_levels.choose(&mut self.rng).as_ref().unwrap();
        let (col_tp, [rel_name, col_name]) = *columns.choose(&mut self.rng).as_ref().unwrap();
        // if **rel_name == Ident::new("T2").into() && **col_name == Ident::new("case").into() {
        //     eprintln!("selected {} by {:#?} with {:?} from {:#?}", ObjectName([(*rel_name).clone().into(), (*col_name).clone().into()].to_vec()), check_accessibility, column_types, column_levels);
        //     clause_context.eprint_clause_hierarchy();
        // }
        Ok(((*col_tp).clone(), [(*rel_name).clone(), (*col_name).clone()]))
    }

    fn choose_select_ident_for_order_by(&mut self, aliases: &Vec<&IdentName>) -> DecisionResult<Ident> {
        Ok((**aliases.choose(&mut self.rng).as_ref().unwrap()).clone().into())
    }

    fn choose_aggregate_function_name(&mut self, func_names: Vec<&String>, dist: WeightedIndex<f64>) -> DecisionResult<ObjectName> {
        let selected_name = *func_names.iter().nth(dist.sample(&mut self.rng)).unwrap();
        Ok(ObjectName(vec![Ident {
            value: selected_name.clone(),
            quote_style: None,
        }]))
    }

    fn choose_bigint(&mut self) -> DecisionResult<String> {
        Ok(self.rng.gen_range(-5..=5).to_string())
    }

    fn choose_integer(&mut self) -> DecisionResult<String> {
        Ok(self.rng.gen_range(-5..=5).to_string())
    }

    fn choose_numeric(&mut self) -> DecisionResult<String> {
        Ok(self.rng.gen_range(-5f64..=5f64).to_string())
    }

    fn choose_text(&mut self) -> DecisionResult<String> {
        Ok("HJeihfbwei".to_string())
    }

    fn choose_date(&mut self) -> DecisionResult<String> {
        Ok("2023-08-27".to_string())
    }

    fn choose_timestamp(&mut self) -> DecisionResult<String> {
        Ok("2023-01-30 14:37:05".to_string())
    }

    fn choose_interval(&mut self, with_field: bool) -> DecisionResult<(String, Option<DateTimeField>)> {
        Ok(if with_field {
            ("1".to_string(), Some(DateTimeField::Day))
        } else { ("1 day".to_string(), None) })
    }

    fn choose_qualified_wildcard_relation<'a>(&mut self, clause_context: &'a ClauseContext, wildcard_relations: &WildcardRelationsValue) -> DecisionResult<(Ident, &'a Relation)> {
        let alias = wildcard_relations.relation_levels_selectable_by_qualified_wildcard
            .iter().filter(|x| !x.is_empty()).collect::<Vec<_>>()
            .choose(&mut self.rng).unwrap()
            .choose(&mut self.rng).unwrap();
        let relation = clause_context.get_relation_by_name(alias);
        Ok((alias.clone().into(), relation))
    }

    fn choose_select_alias(&mut self) -> DecisionResult<Ident> {
        self.free_select_alias_index += 1;
        Ok(Ident::new(format!("C{}", self.free_select_alias_index)))
    }

    fn choose_from_alias(&mut self) -> DecisionResult<Ident> {
        self.free_from_alias_index += 1;
        Ok(Ident::new(format!("T{}", self.free_from_alias_index)))
    }

    fn choose_from_column_renames(&mut self, n_columns: usize) -> DecisionResult<Vec<Ident>> {
        let mut out = vec![];
        for _ in 0..self.rng.gen_range(0..n_columns) {
            self.free_from_rename_index += 1;
            out.push(Ident::new(format!("R{}", self.free_from_rename_index)))
        }
        Ok(out)
    }

    fn reset(&mut self) {
        self.free_select_alias_index = 0;
        self.free_from_rename_index = 0;
        self.free_from_alias_index = 0;
    }
}

impl RandomValueChooser {
    pub fn new() -> Self {
        Self {
            rng: ChaCha8Rng::seed_from_u64(0),
            free_select_alias_index: 0,
            free_from_rename_index: 0,
            free_from_alias_index: 0,
        }
    }
}

struct VecWithIndex<Tp> where Tp: Clone {
    v: Vec<Tp>,
    ind: usize
}

impl<Tp> VecWithIndex<Tp> where Tp: Clone {
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
    chosen_from_aliases: VecWithIndex<Ident>,
    chosen_from_renames: VecWithIndex<Vec<Ident>>,
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
            chosen_from_aliases: init_from_nodes!(path, Ident, FromAlias),
            chosen_from_renames: init_from_nodes!(path, Vec<Ident>, FromColumnRenames),
            chosen_columns: init_from_nodes!(path, [IdentName; 2], SelectedColumnName),
            chosen_order_by_select_refs: init_from_nodes!(path, Ident, ORDERBYSelectReference),
            chosen_aggregate_functions: init_from_nodes!(path, ObjectName, SelectedAggregateFunctions),
            chosen_qualified_wildcard_tables: init_from_nodes!(path, Ident, QualifiedWildcardSelectedRelation),
        }
    }
}

impl QueryValueChooser for DeterministicValueChooser {
    fn choose_table_name(&mut self, available_table_names: &Vec<ObjectName>) -> DecisionResult<ObjectName> {
        let name = self.chosen_tables.next();
        let search_name_identname = IdentName::from(name.0[0].clone());
        if !available_table_names.iter().any(|avail_name| {
            let avail_name_identname = IdentName::from(avail_name.0[0].clone());
            avail_name_identname == search_name_identname
        }) {
            panic!("Selected table name {name} is not present: {:?}", available_table_names)
        }
        Ok(name)
    }

    fn choose_column(&mut self, clause_context: &ClauseContext, column_types: Vec<SubgraphType>, check_accessibility: CheckAccessibility, column_retrieval_options: ColumnRetrievalOptions) -> DecisionResult<(SubgraphType, [IdentName; 2])> {
        let qualified_column_name = self.chosen_columns.next();
        let ident_components: Vec<Ident> = if check_accessibility == CheckAccessibility::ColumnName {
            vec![qualified_column_name.iter().last().cloned().unwrap().into()]
        } else {
            qualified_column_name.iter().cloned().map(IdentName::into).collect()
        };
        let (col_tp, retrieved_column_name) = clause_context.retrieve_column_by_ident_components(
            &ident_components, column_retrieval_options
        ).unwrap();
        assert!(column_types.contains_generator_of(&col_tp));
        assert!(qualified_column_name == retrieved_column_name);
        Ok((col_tp, retrieved_column_name))
    }

    fn choose_select_ident_for_order_by(&mut self, aliases: &Vec<&IdentName>) -> DecisionResult<Ident> {
        let ident = self.chosen_order_by_select_refs.next();
        if !aliases.contains(&&ident.clone().into()) {
            panic!("Cannot choose {ident} in ORDER BY: it is not present among SELECT aliases: {:?}", aliases);
        }
        Ok(ident)
    }

    fn choose_aggregate_function_name(&mut self, func_names: Vec<&String>, _dist: WeightedIndex<f64>) -> DecisionResult<ObjectName> {
        let aggr_func = self.chosen_aggregate_functions.next();
        assert!(func_names.iter().any(|func_name| **func_name == aggr_func.0[0].value.to_uppercase()), "Selected aggregate function is not available: {}", aggr_func);
        Ok(aggr_func)
    }
    
    fn choose_bigint(&mut self) -> DecisionResult<String> { Ok(self.chosen_bigints.next()) }
    
    fn choose_integer(&mut self) -> DecisionResult<String> { Ok(self.chosen_integers.next()) }
    
    fn choose_numeric(&mut self) -> DecisionResult<String> { Ok(self.chosen_numerics.next()) }
    
    fn choose_text(&mut self) -> DecisionResult<String> { Ok(self.chosen_strings.next()) }
    
    fn choose_date(&mut self) -> DecisionResult<String> { Ok(self.chosen_dates.next()) }

    fn choose_timestamp(&mut self) -> DecisionResult<String> { Ok(self.chosen_timestamps.next()) }

    fn choose_interval(&mut self, with_field: bool) -> DecisionResult<(String, Option<DateTimeField>)> {
        let (value, field) = self.chosen_intervals.next();
        if with_field ^ field.is_some() {
            panic!("with_field is {with_field} but the field is set to {:?}", field)
        }
        Ok((value, field))
    }

    fn choose_qualified_wildcard_relation<'a>(&mut self, clause_context: &'a ClauseContext, wildcard_relations: &WildcardRelationsValue) -> DecisionResult<(Ident, &'a Relation)> {
        let alias: IdentName = self.chosen_qualified_wildcard_tables.next_ref().clone().into();
        if !wildcard_relations.relation_levels_selectable_by_qualified_wildcard.iter().any(|level| level.contains(&alias)) {
            panic!("Relation cannot be selected by wildcard in this context: wildcard_relations = {:?}, rel. alias: {}", wildcard_relations, <IdentName as Into<Ident>>::into(alias))
        }
        let relation = clause_context.get_relation_by_name(&alias);
        Ok((alias.into(), relation))
    }

    fn choose_select_alias(&mut self) -> DecisionResult<Ident> { Ok(self.chosen_select_aliases.next()) }

    fn choose_from_alias(&mut self) -> DecisionResult<Ident> { Ok(self.chosen_from_aliases.next()) }

    fn choose_from_column_renames(&mut self, n_columns: usize) -> DecisionResult<Vec<Ident>> {
        let out = self.chosen_from_renames.next();
        assert!(out.len() <= n_columns);
        Ok(out)
    }

    fn reset(&mut self) {
        self.chosen_bigints.reset();
        self.chosen_integers.reset();
        self.chosen_numerics.reset();
        self.chosen_strings.reset();
        self.chosen_dates.reset();
        self.chosen_intervals.reset();
        self.chosen_tables.reset();
        self.chosen_select_aliases.reset();
        self.chosen_from_aliases.reset();
        self.chosen_from_renames.reset();
        self.chosen_columns.reset();
        self.chosen_order_by_select_refs.reset();
        self.chosen_qualified_wildcard_tables.reset();
    }
}
