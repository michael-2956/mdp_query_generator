use std::{any::Any, collections::HashMap, fmt::Display, path::PathBuf, sync::atomic::AtomicPtr, thread, time::SystemTime};

use bimap::BiHashMap;
use chrono::{Duration, NaiveDate, NaiveDateTime};
use itertools::Itertools;
use rand_distr::{num_traits::Float, Alphanumeric, Distribution, Normal};
use postgres::{types::FromSql, Client, NoTls};
use rand::{thread_rng, Rng};
use rust_decimal::Decimal;
use smol_str::SmolStr;
use sqlparser::ast::{DataType, DateTimeField, Ident, ObjectName, Query, TimezoneInfo};
use tokio::runtime::{self, Runtime};

use crate::{config::Config, query_creation::{query_generator::{call_modifiers::WildcardRelationsValue, query_info::{self, ClauseContext, DatabaseSchema, IdentName, Relation}, value_choosers::QueryValueChooser, QueryGenerator}, state_generator::{markov_chain_generator::{markov_chain::NodeParams, StackFrame}, state_choosers::MaxProbStateChooser, subgraph_type::SubgraphType, substitute_models::DeterministicModel, MarkovChainGenerator}}, training::models::{ModelPredictionResult, PathwayGraphModel}};

#[derive(Debug, Clone)]
pub enum ConstraintType {
    Point(f64),
    Range {
        from: f64,
        to: f64
    }
}

#[derive(Debug, Clone)]
pub enum ConstraintMetric {
    Cost,
    Cardinality
}

pub struct QueryConstraint {
    c_type: ConstraintType,
    metric: ConstraintMetric,
}

impl QueryConstraint {
    pub fn new(c_type: ConstraintType, metric: ConstraintMetric) -> Self {
        Self {
            c_type,
            metric,
        }
    }
}

pub struct ConstraintMeetingEnvironment {
    config: Config,
    constraint: QueryConstraint,
    query_generator: Option<QueryGenerator<MaxProbStateChooser>>,
    observation_rs: crossbeam_channel::Receiver<Option<Vec<bool>>>,
    decision_sd: crossbeam_channel::Sender<Option<usize>>,
    generator_handle: Option<thread::JoinHandle<((Box<dyn PathwayGraphModel>, sqlparser::ast::Query), QueryGenerator<MaxProbStateChooser>)>>,
    model: Option<Box<dyn PathwayGraphModel>>,
}

impl ConstraintMeetingEnvironment {
    pub fn new(constraint: QueryConstraint, config_path: PathBuf) -> Self {
        let config = Config::read_config(&config_path).unwrap();
        
        // when model sends None to the observation channel, we treat this
        // as the end, but the model and its sd are never dropped
        let (
            observation_sd, observation_rs
        ) = crossbeam_channel::bounded::<Option<Vec<bool>>>(1);
        // when decision is None, means that the model decided to interrupt generation
        let (
            decision_sd, decision_rs
        ) = crossbeam_channel::bounded::<Option<usize>>(1);

        let all_agg_function_names = config.generator_config.aggregate_functions_distribution.get_all_function_names();

        let schema = DatabaseSchema::parse_schema(&config.generator_config.table_schema_path);

        let state_generator = MarkovChainGenerator::with_config(&config.chain_config).unwrap();

        let all_chain_states = state_generator.markov_chain_ref().get_all_states();

        Self {
            constraint,
            query_generator: Some(QueryGenerator::from_state_generator_and_config(
                state_generator,
                config.generator_config.clone(),
                Box::new(DeterministicModel::empty())
            )),
            config,
            observation_rs,
            decision_sd,
            generator_handle: None,
            model: Some(Box::new(InteractiveModel::new(
                observation_sd, decision_rs,
                all_agg_function_names,
                schema,
                all_chain_states,
                100, // TODO: put these in config
                100,
                100
            ))),
        }
    }

    /// if the query generation has finished, returns the Query \
    /// and makes the model and query generator available for spawning \
    /// an another generation process
    pub fn try_join_generator(&mut self) -> Result<Option<Query>, Box<dyn Any + Send>> {
        if let Some(handle) = self.generator_handle.take() {
            if !handle.is_finished() {
                return Ok(None);
            }
            let ((model, query), qg) = match handle.join() {
                Ok(r) => r,
                Err(e) => {
                    match e.downcast::<String>() {
                        Ok(es) => {
                            if *es == "No decision (got None)" {
                                return Ok(None)
                            } else {
                                return Err(es);
                            }
                        },
                        Err(err) => return Err(err),
                    }
                },
            };
            self.query_generator = Some(qg);
            self.model = Some(model);
            Ok(Some(query))
        } else {
            Ok(None)
        }
    }

    /// starts a new generaiton thread, consuming the model and query generator
    fn spawn_generator(&mut self) {
        if self.generator_handle.is_none() {
            let mut qg = self.query_generator.take().unwrap();
            let model = self.model.take().unwrap();
            self.generator_handle = Some(thread::spawn(
                move || {
                    (qg.generate_with_model(model), qg)
                }
            ));
        }
    }

    /// restarts generation, returns original observation \
    /// and previous query string if available
    pub fn reset(&mut self) -> (Vec<bool>, Option<String>) {
        let query = self.try_join_generator().unwrap();
        
        self.model.as_mut().unwrap().as_value_chooser().unwrap().reset();
        
        self.spawn_generator();

        let initial_obs = self.observation_rs.recv().unwrap().unwrap();

        (initial_obs, query.map(|query| format!("{query}")))
    }

    /// Step the environment. Returns `(mask, anticall_reward, terminated)`.
    pub fn step(&mut self, action: Option<usize>) -> (Option<Vec<bool>>, f32, bool) {
        assert!(self.generator_handle.is_some());

        self.decision_sd.send(action).unwrap();

        let obs = self.observation_rs.recv().unwrap();

        let terminated = obs.is_none();

        (obs, 0f32, terminated)
    }

    /// returns a query if generation has finished
    pub fn pop_query(&mut self) -> Option<String> {
        self.try_join_generator().unwrap().map(|query| format!("{query}"))
    }
}

struct InteractiveModel {
    observation_sd: crossbeam_channel::Sender<Option<Vec<bool>>>,
    decision_rs: crossbeam_channel::Receiver<Option<usize>>,
    tp_sample_map: HashMap<SubgraphType, Vec<String>>,
    state_id_bimap: BiHashMap<String, usize>,
    start_node_stack: Vec<String>,
    free_select_alias_index: usize,
    free_from_alias_index: usize,
}

impl InteractiveModel {
    fn new(
        observation_sd: crossbeam_channel::Sender<Option<Vec<bool>>>,
        decision_rs: crossbeam_channel::Receiver<Option<usize>>,
        all_agg_function_names: Vec<String>,
        schema: DatabaseSchema,
        all_chain_states: Vec<String>,
        n_allocated_aliases: usize,
        n_ds_samples: usize,
        n_random_samples: usize,
    ) -> Self {
        // TODO:
        //    - do all the sampling needed for TODO 2 / TODO 3
        //    - store action space size for env

        let mut sample_value_tp_map = HashMap::new();
        let mut state_id_bimap = BiHashMap::new();
        let mut free_id = 0usize;

        // TODO: prefix keys with --SUBSECTION so that there's no overlap
        let mut add_state = |state: String| {
            state_id_bimap.insert(state, free_id);
            free_id += 1;
        };

        // add all graph states
        for state in all_chain_states {
            add_state(state);
        }

        // aggregate function names
        for agg_func_name in all_agg_function_names {
            add_state(agg_func_name);
        }

        // aliases
        for i in 0..n_allocated_aliases {
            add_state(format!("C{i}"));
            add_state(format!("T{i}"));
        }

        // WARNING: put this in config instead
        let mut client = Client::connect("host=localhost user=mykhailo dbname=tpch", NoTls).unwrap();

        // table names
        for table_decl in schema.table_defs {
            let table_name = format!("{}", table_decl.name);
            add_state(table_name.clone());
            for column in table_decl.columns {
                // column names
                let column_name = format!("{}", column.name);
                add_state(column_name.clone());

                // sample values (dataset, 100)
                // TODO:
                //       - check how the display strings are formatted
                //       - store indexes to be able to choose
                let sv_tp = SubgraphType::from_data_type(&column.data_type);
                let values = match &sv_tp {
                    SubgraphType::Timestamp => {
                        Self::sample_values_str::<NaiveDateTime>(&mut client, &table_name, &column_name, n_ds_samples)
                    },
                    SubgraphType::Numeric => {
                        Self::sample_values_str::<Decimal>(&mut client, &table_name, &column_name, n_ds_samples)
                    },
                    SubgraphType::Integer => {
                        Self::sample_values_str::<i32>(&mut client, &table_name, &column_name, n_ds_samples)
                    },
                    SubgraphType::BigInt => {
                        Self::sample_values_str::<i64>(&mut client, &table_name, &column_name, n_ds_samples)
                    },
                    SubgraphType::Text => {
                        Self::sample_values_str::<String>(&mut client, &table_name, &column_name, n_ds_samples)
                    },
                    SubgraphType::Date => {
                        Self::sample_values_str::<NaiveDate>(&mut client, &table_name, &column_name, n_ds_samples)
                    },
                    tp => unimplemented!("{tp}"),
                };
                println!("\n{table_name}.{column_name}:");
                println!("values: {:?}", values);
                for value in values {
                    add_state(value.clone());
                    sample_value_tp_map.insert(value, sv_tp.clone());
                }
            }
        }

        // sample values (normal distrib, 100)
        let mut rng = thread_rng();

        // timestamp
        let base_ts = NaiveDateTime::parse_from_str(
            "2000-01-01 00:00:00", "%Y-%m-%d %H:%M:%S"
        ).unwrap();
        let ts_dist = Normal::new(
            0., 86_400. * 365. * 20.
        ).unwrap(); // stddev = 20 years
        let timestamps: Vec<String> = (0..n_random_samples)
            .map(|_| {
                let offset = ts_dist.sample(&mut rng).round() as i64;
                let dt = base_ts + Duration::seconds(offset);
                dt.format("%Y-%m-%d %H:%M:%S").to_string()
            })
            .collect();
        for timestamp in timestamps {
            add_state(timestamp.clone());
            sample_value_tp_map.insert(timestamp, SubgraphType::Timestamp);
        }

        // interval
        let intv_dist = Normal::new(
            10., 1000.
        ).unwrap(); // mean=0s, stddev=1000s
        let intervals: Vec<String> = (0..n_random_samples)
            .map(|_| {
                let secs = intv_dist.sample(&mut rng);
                format!("{:.2} seconds", secs.max(0.))  // clamp to ≥0
            })
            .collect();
        for interval in intervals {
            add_state(interval.clone());
            sample_value_tp_map.insert(interval, SubgraphType::Interval);
        }

        // numeric
        let num_dist = Normal::new(0., 100.).unwrap();
        let numerics: Vec<String> = (0..n_random_samples)
            .map(|_| format!("{:.8}", num_dist.sample(&mut rng)))
            .collect();
        for numeric in numerics {
            add_state(numeric.clone());
            sample_value_tp_map.insert(numeric, SubgraphType::Numeric);
        }

        // integer
        let int_dist = Normal::new(0., 100.).unwrap();
        let integers: Vec<String> = (0..n_random_samples)
            .map(|_| (int_dist.sample(&mut rng).round() as i32).to_string())
            .collect();
        for integer in integers {
            add_state(integer.clone());
            sample_value_tp_map.insert(integer, SubgraphType::Integer);
        }

        // bigint
        let bigint_dist = Normal::new(0., 100_000_000.).unwrap();
        let bigints: Vec<String> = (0..n_random_samples)
            .map(|_| (bigint_dist.sample(&mut rng).round() as i64).to_string())
            .collect();
        for bigint in bigints {
            add_state(bigint.clone());
            sample_value_tp_map.insert(bigint, SubgraphType::BigInt);
        }

        // text
        let texts: Vec<String> = (0..n_random_samples)
            .map(|_| {
                rng.clone().sample_iter(&Alphanumeric)
                    .take(8)
                    .map(char::from)
                    .collect()
            })
            .collect();
        for text in texts {
            add_state(text.clone());
            sample_value_tp_map.insert(text, SubgraphType::Text);
        }

        // date
        let base_date = NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
        let date_dist = Normal::new(0., 365. * 20.).unwrap();
        let dates: Vec<String> = (0..n_random_samples)
            .map(|_| {
                let days = date_dist.sample(&mut rng).round() as i64;
                let d = base_date + Duration::days(days);
                d.format("%Y-%m-%d").to_string()
            })
            .collect();
        for date in dates {
            add_state(date.clone());
            sample_value_tp_map.insert(date, SubgraphType::Date);
        }

        let mut tp_sample_map: HashMap<SubgraphType, Vec<String>> = HashMap::new();
        for (value, tp) in sample_value_tp_map.into_iter() {
            tp_sample_map.entry(tp).or_default().push(value);
        }

        Self {
            free_select_alias_index: 0,
            free_from_alias_index: 0,
            tp_sample_map,
            state_id_bimap,
            start_node_stack: vec![],
            observation_sd,
            decision_rs,
        }
    }

    fn sample_values<T: for<'a> FromSql<'a>>(
        client: &mut Client, table_name: &String, column_name: &String, n_ds_samples: usize
    ) -> Vec<T> {
        let query = format!(
            "SELECT {column_name} FROM {table_name} ORDER BY RANDOM() LIMIT {n_ds_samples}"
        );
        let rows = client.query(&query, &[]).unwrap();
        let mut values = vec![];
        for row in rows {
            values.push(row.get::<_, T>(0));
        }
        values
    }

    fn sample_values_str<T: for<'a> FromSql<'a> + Display>(
        client: &mut Client, table_name: &String, column_name: &String, n_ds_samples: usize
    ) -> Vec<String> {
        Self::sample_values(client, table_name, column_name, n_ds_samples)
            .into_iter().map(|v: T| format!("{v}")).collect()
    }

    fn gen_mask(&self, available_states: Vec<&str>) -> Vec<bool> {
        let mut mask = vec![false; self.state_id_bimap.len()];
        for state in available_states {
            let state_id = self.state_id_bimap.get_by_left(state).unwrap();
            mask[*state_id] = true;
        }
        mask
    }
}

impl PathwayGraphModel for InteractiveModel {
    fn as_value_chooser(&mut self) -> Option<&mut dyn QueryValueChooser> { Some(self) }
    
    fn process_state(&mut self, call_stack: &Vec<StackFrame>, popped_stack_frame: Option<&StackFrame>) { unimplemented!() }

    fn write_weights(&self, file_path: &PathBuf) -> std::io::Result<()> { unimplemented!() }

    fn load_weights(&mut self, file_path: &PathBuf) -> std::io::Result<()> { unimplemented!() }

    /// send states available for decision to observation_sd, \
    /// receiving the decision from decision_rs, checking that it \
    /// is valid.
    fn predict(&mut self, call_stack: &Vec<StackFrame>, node_outgoing: Vec<NodeParams>, current_exit_node_name: &SmolStr, _current_query_ast_ptr_opt: &mut Option<AtomicPtr<Query>>) -> ModelPredictionResult {
        if call_stack.len() > self.start_node_stack.len() {
            assert!(call_stack.len() == self.start_node_stack.len() + 1);
            self.start_node_stack.push(
                call_stack.last().unwrap().function_context.call_params.func_name.to_string()
            );
            let mask = self.gen_mask(vec![self.start_node_stack.last().unwrap().as_str()]);
            self.observation_sd.send(Some(mask.clone())).unwrap();
            let decision_id = match self.decision_rs.recv().unwrap() {
                Some(decision_id) => decision_id,
                None => panic!("No decision (got None)"),
            };
            assert!(mask[decision_id]);
            // no need to return the decision
        }
        self.start_node_stack.truncate(call_stack.len());

        let available_states = node_outgoing.iter().map(
            |params| {
                params.node_common.name.as_str()
            }
        ).collect_vec();
        let mask = self.gen_mask(available_states);
        self.observation_sd.send(Some(mask.clone())).unwrap();
        let decision_id = match self.decision_rs.recv().unwrap() {
            Some(decision_id) => decision_id,
            None => panic!("No decision (got None)"),
        };
        assert!(mask[decision_id]);

        let chosen_state = self.state_id_bimap.get_by_right(&decision_id).unwrap();

        if call_stack.len() == 1 && chosen_state == current_exit_node_name.as_str() {
            // as predict() will never be invoked again, send terminating observation
            self.observation_sd.send(None).unwrap();
        }

        ModelPredictionResult::Some(node_outgoing.into_iter().map(
            |params| (
                if params.node_common.name == chosen_state { 1f64 } else { 0f64 },
                params
            )
        ).collect())
    }
}


impl QueryValueChooser for InteractiveModel {
    fn choose_table_name(&mut self, available_table_names: &Vec<ObjectName>) -> ObjectName {
        // TODO 1: when we refer to the table aliases we created previously,
        //         would it be smart to allocate neurons just for
        //         the max number of table aliases possible, if every
        //         time they would refer to different things?
        
        // available_table_names[self.rng.gen_range(0..available_table_names.len())].clone()
        
        todo!()
    }

    fn choose_column(&mut self, clause_context: &ClauseContext, column_types: Vec<SubgraphType>, check_accessibility: query_info::CheckAccessibility, column_retrieval_options: query_info::ColumnRetrievalOptions) -> (SubgraphType, [IdentName; 2]) {
        // TODO 1: when we refer to the columns we created previously,
        //         would it be smart to allocate neurons just for
        //         the max number of columns possible?
        
        // let column_levels = clause_context.get_non_empty_column_levels_by_types(column_types.clone(), check_accessibility, column_retrieval_options.clone());
        // let columns = *column_levels.choose(&mut self.rng).as_ref().unwrap();
        // let (col_tp, [rel_name, col_name]) = *columns.choose(&mut self.rng).as_ref().unwrap();
        // ((*col_tp).clone(), [(*rel_name).clone(), (*col_name).clone()])
        
        todo!()
    }

    fn choose_select_alias_order_by(&mut self, aliases: &Vec<&IdentName>) -> Ident {
        // TODO 1:   when we refer to an alias we created previously,
        //           would it be smart to allocate neurons just for
        //           the max number of select aliases possible? - Yes
        // Problem:  How would the network be able to count tokens?
        // Solution: Use LSTM embedding to attend to next token embeddings?

        // (**aliases.choose(&mut self.rng).as_ref().unwrap()).clone().into()

        todo!()
    }

    fn choose_aggregate_function_name(&mut self, func_names: Vec<&String>, dist: rand::distributions::WeightedIndex<f64>) -> ObjectName {
        // TODO 2: there's a limited number of function names available
        //         their total total number can be extracted from json
        //         then give a neuron to each one
        todo!()
    }

    fn choose_bigint(&mut self) -> String {
        // let states = self.tp_sample_map.get(&SubgraphType::BigInt).unwrap();
        // TODO 3: sample N_r=1024 random values, N_d=min(ds, 1024) values from the DS
        todo!()
    }

    fn choose_integer(&mut self) -> String {
        // TODO 3
        todo!()
    }

    fn choose_numeric(&mut self) -> String {
        // TODO 3
        todo!()
    }

    fn choose_text(&mut self) -> String {
        // TODO 3
        todo!()
    }

    fn choose_date(&mut self) -> String {
        // TODO 3
        // note: NaiveDate string looks like "2023-08-10" already
        todo!()
    }

    fn choose_timestamp(&mut self) -> String {
        // TODO 3
        // note: NaiveDateTime string looks like '2025-01-01 12:00:00' already
        todo!()
    }

    fn choose_interval(&mut self, with_field: bool) -> (String, Option<DateTimeField>) {
        // PROBLEM 1: cannot always just sample values from the DS, as intervals might not be
        //            present there.
        //          - Can just sample from a normal distribution then.
        // TODO 3:  - Or maybe for every field we should have N_r=1024 random values to sample
        //            and then N_d=min(ds, 1024) values from the DS???
        //          - Then, there are always random values present when we don't have
        //            the dataset values present
        // PROBLEM 2: also, multiple formats available,
        //          - but I guess if with_field is false just transform it into a string.
        //          - it would be simple as we can just represent all as seconds

        // if with_field {
        //     ("1".to_string(), Some(DateTimeField::Day))
        // } else { ("1 day".to_string(), None) }
        todo!()
    }

    fn choose_qualified_wildcard_relation<'a>(&mut self, clause_context: &'a ClauseContext, wildcard_relations: &WildcardRelationsValue) -> (Ident, &'a Relation) {
        // TODO 1

        // let alias = wildcard_relations.relation_levels_selectable_by_qualified_wildcard
        //     .iter().filter(|x| !x.is_empty()).collect::<Vec<_>>()
        //     .choose(&mut self.rng).unwrap()
        //     .choose(&mut self.rng).unwrap();
        // let relation = clause_context.get_relation_by_name(alias);
        // (alias.clone().into(), relation)
        
        todo!()
    }

    fn choose_select_alias(&mut self) -> Ident {
        self.free_select_alias_index += 1;
        Ident::new(format!("C{}", self.free_select_alias_index))
    }

    fn choose_from_alias(&mut self) -> Ident {
        self.free_from_alias_index += 1;
        Ident::new(format!("T{}", self.free_from_alias_index))
    }

    fn choose_from_column_renames(&mut self, _n_columns: usize) -> Vec<Ident> {
        vec![]  // do not rename anything
    }

    fn reset(&mut self) {
        self.free_select_alias_index = 0;
        self.free_from_alias_index = 0;
    }
}