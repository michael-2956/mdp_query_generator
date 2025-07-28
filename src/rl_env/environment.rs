use std::{collections::HashMap, fmt::Display, path::PathBuf, sync::atomic::AtomicPtr, thread};

use bimap::BiHashMap;
use chrono::{Duration, NaiveDate, NaiveDateTime};
use itertools::Itertools;
use rand_distr::{num_traits::Float, Alphanumeric, Distribution, Normal};
use postgres::{types::FromSql, Client, NoTls};
use rand::{thread_rng, Rng};
use rust_decimal::Decimal;
use smol_str::SmolStr;
use sqlparser::ast::{DateTimeField, Ident, ObjectName, Query};

use crate::{config::Config, query_creation::{query_generator::{call_modifiers::WildcardRelationsValue, query_info::{self, ClauseContext, DatabaseSchema, IdentName, QueryProps, Relation}, value_choosers::QueryValueChooser, QueryGenerationError, QueryGenerator}, state_generator::{markov_chain_generator::{markov_chain::NodeParams, StackFrame, StateGenerationError}, state_choosers::MaxProbStateChooser, subgraph_type::SubgraphType, substitute_models::DeterministicModel, MarkovChainGenerator}}, training::models::{DecisionError, DecisionResult, ModelPrediction, PathwayGraphModel}};

#[derive(Debug, Clone)]
pub enum ConstraintType {
    Point(f64),
    Range {
        from: f64,
        to: f64
    }
}

// #[derive(Debug, Clone)]
// pub enum ConstraintMetric {
//     Cost,
//     Cardinality
// }

// pub struct QueryConstraint {
//     c_type: ConstraintType,
//     metric: ConstraintMetric,
// }

// impl QueryConstraint {
//     pub fn new(c_type: ConstraintType, metric: ConstraintMetric) -> Self {
//         Self {
//             c_type,
//             metric,
//         }
//     }
// }

pub struct ConstraintMeetingEnvironment {
    // constraint: QueryConstraint,
    query_generator: Option<QueryGenerator<MaxProbStateChooser>>,
    observation_rs: crossbeam_channel::Receiver<Option<Vec<bool>>>,
    decision_sd: crossbeam_channel::Sender<Option<usize>>,
    generator_handle: Option<thread::JoinHandle<(Result<(Box<dyn PathwayGraphModel>, sqlparser::ast::Query), (QueryGenerationError, Box<dyn PathwayGraphModel>)>, QueryGenerator<MaxProbStateChooser>)>>,
    model: Option<Box<dyn PathwayGraphModel>>,
    truncated: bool,
    terminated: bool,
}

impl ConstraintMeetingEnvironment {
    pub fn new(config_path: PathBuf) -> Self {
        let config = Config::read_config(&config_path).unwrap();
        
        let (
            query_generator,
            model,
            observation_rs,
            decision_sd,
        ) = Self::create_model_and_generator(config.clone());

        Self {
            // constraint,
            query_generator: Some(query_generator),
            observation_rs,
            decision_sd,
            generator_handle: None,
            model: Some(model),
            truncated: false,
            terminated: false,
        }
    }

    fn create_model_and_generator(config: Config) -> (
        QueryGenerator<MaxProbStateChooser>,
        Box<dyn PathwayGraphModel>,
        crossbeam_channel::Receiver<Option<Vec<bool>>>,
        crossbeam_channel::Sender<Option<usize>>
    ) {
        // when model sends None to the observation channel, we treat this
        // as the end (termination), but the model and its sd are not dropped
        let (
            observation_sd, observation_rs
        ) = crossbeam_channel::bounded::<Option<Vec<bool>>>(1);
        // when decision is None, means that the model decided to interrupt generation (truncation)
        let (
            decision_sd, decision_rs
        ) = crossbeam_channel::bounded::<Option<usize>>(1);
        
        let all_agg_function_names = config.generator_config.aggregate_functions_distribution.get_all_function_names();

        let schema = DatabaseSchema::parse_schema(&config.generator_config.table_schema_path);

        let state_generator = MarkovChainGenerator::with_config(&config.chain_config).unwrap();

        let all_chain_states = state_generator.markov_chain_ref().get_all_states();

        return (
            QueryGenerator::from_state_generator_and_config(
                state_generator,
                config.generator_config,
                Box::new(DeterministicModel::empty())
            ),
            Box::new(InteractiveModel::new(
                observation_sd, decision_rs,
                all_agg_function_names,
                schema,
                all_chain_states,
                100, // TODO: put these in config
                100,
                100
            )),
            observation_rs,
            decision_sd,
        )
    }

    /// Returns Ok(None) when the generation is running or has failed, \
    /// returns the query otherwise. If possible, restores data for the \
    /// new generation to be started
    /// - If the generator thread is running, returns Ok(None)
    /// - If the generator has finished successfully, returns the \
    ///   generated query and restored the model and QG objects to \
    ///   start a new generation
    /// - If the generator has finished with an error, replaces the \
    ///   model and QG objects with new ones. If the error is actually \
    ///   the generation being truncated, returns Ok(None), otherwise \
    ///   returns the error
    /// - If the generator thread if abcent, returns Ok(None)
    pub fn try_join_generator(&mut self) -> Result<Option<Query>, QueryGenerationError> {
        if self.generator_handle.is_some() {
            if !self.generator_handle.as_ref().unwrap().is_finished() && !self.truncated && !self.terminated {
                return Ok(None);
            }
            let (qg, model, query) = match self.generator_handle.take().unwrap().join().unwrap() {
                (Ok((model, query)), qg) => (qg, model, query),
                (Err((qg_error, model)), qg) => match qg_error {
                    QueryGenerationError::StateGeneration(
                        StateGenerationError::ModelDecisionError(DecisionError::Truncated)
                    ) => {
                        assert!(self.truncated);  // sanity check
                        self.query_generator = Some(qg);
                        self.model = Some(model);
                        return Ok(None)
                    },
                    any => return Err(any)
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
        assert!(self.generator_handle.is_none());
        self.truncated = false;
        self.terminated = false;
        let mut qg = self.query_generator.take().unwrap();
        let model = self.model.take().unwrap();
        self.generator_handle = Some(thread::spawn(
            move || {
                (qg.generate_with_model(model), qg)
            }
        ));
    }

    /// Restarts the generation \
    /// - If successful, returns original observation \
    /// - Returns previous query string if available
    pub fn reset(&mut self) -> (Option<Vec<bool>>, Option<String>) {
        let query = self.try_join_generator().unwrap();

        // the resources were taken by another generation: cannot reset
        if self.query_generator.is_none() || self.model.is_none() {
            return (None, query.map(|query| format!("{query}")))
        }

        // restart the generation
        self.model.as_mut().unwrap().as_value_chooser().unwrap().reset();
        self.spawn_generator();
        let initial_obs = self.observation_rs.recv().unwrap().unwrap();

        (Some(initial_obs), query.map(|query| format!("{query}")))
    }

    /// Step the environment. Returns `(mask, anticall_reward, terminated)`.
    pub fn step(&mut self, action: Option<usize>) -> (Option<Vec<bool>>, f32, bool) {
        assert!(self.generator_handle.is_some(), "Generator handle is abcent. Try to reset()");
        assert!(!self.generator_handle.as_ref().unwrap().is_finished(), "Generator is not running. Try to reset()");
        assert!(!self.truncated, "Generator was truncated. Try to reset()");

        self.decision_sd.send(action).unwrap();

        if action.is_none() {  // the generation was truncated
            self.truncated = true;
            return (None, 0f32, false)
        }

        let obs = self.observation_rs.recv().unwrap();

        self.terminated = obs.is_none();  // TODO: renew query generator and model after termination

        (obs, 0f32, self.terminated)  // TODO: anticall reward
    }

    /// returns a query if generation has finished
    pub fn pop_query(&mut self) -> Option<String> {
        self.try_join_generator().unwrap().map(|query| format!("{query}"))
    }
}

#[derive(Debug)]
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
        let mut sample_value_tp_map = HashMap::new();
        let mut state_id_bimap = BiHashMap::new();
        let mut free_id = 0usize;

        let mut add_state = |state: String, section_name: &str| {
            state_id_bimap.insert(format!("{section_name}--{state}"), free_id);
            free_id += 1;
        };

        // add all graph states
        for state in all_chain_states {
            add_state(state, "GRAPH");
        }

        // aggregate function names
        for agg_func_name in all_agg_function_names {
            add_state(agg_func_name.clone(), "AGG_FUNC");
            add_state(agg_func_name, "C_NAME");  // can be a column name
        }
        
        // aliases
        for i in 0..n_allocated_aliases {
            add_state(format!("C{i}"), "C_NAME");
            add_state(format!("T{i}"), "R_NAME");
        }
        
        // WARNING: put this in config instead
        let mut client = Client::connect("host=localhost user=mykhailo dbname=tpch", NoTls).unwrap();

        // table names
        for table_decl in schema.table_defs {
            let table_name = format!("{}", table_decl.name);
            add_state(table_name.to_uppercase(), "R_NAME");
            for column in table_decl.columns {
                // column names
                let column_name = format!("{}", column.name);
                add_state(column_name.to_uppercase(), "C_NAME");

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
                for value in values {
                    add_state(value.clone(), &format!("DB_SAMPLE_{sv_tp}"));
                    sample_value_tp_map.insert(value, sv_tp.clone());
                }
            }
        }

        // sample values (normal distrib, 100)
        let mut rng = thread_rng();

        let mut add_random_sample_states = |state_list: Vec<String>, tp: SubgraphType| {
            for state in state_list {
                add_state(state.clone(), &format!("RANDOM_SAMPLE_{tp}"));
                sample_value_tp_map.insert(state, tp.clone());
            }
        };

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
        add_random_sample_states(timestamps, SubgraphType::Timestamp);

        // interval
        let intv_dist = Normal::new(
            10., 1000.
        ).unwrap(); // mean=0s, stddev=1000s
        let intervals: Vec<String> = (0..n_random_samples)
            .map(|_| {
                let secs = intv_dist.sample(&mut rng);
                format!("{:.2} seconds", secs.abs())
            })
            .collect();
        add_random_sample_states(intervals, SubgraphType::Interval);

        // numeric
        let num_dist = Normal::new(0., 100.).unwrap();
        let numerics: Vec<String> = (0..n_random_samples)
            .map(|_| format!("{:.8}", num_dist.sample(&mut rng)))
            .collect();
        add_random_sample_states(numerics, SubgraphType::Numeric);

        // integer
        let int_dist = Normal::new(0., 100.).unwrap();
        let integers: Vec<String> = (0..n_random_samples)
            .map(|_| (int_dist.sample(&mut rng).round() as i32).to_string())
            .collect();
        add_random_sample_states(integers, SubgraphType::Integer);

        // bigint
        let bigint_dist = Normal::new(0., 100_000_000.).unwrap();
        let bigints: Vec<String> = (0..n_random_samples)
            .map(|_| (bigint_dist.sample(&mut rng).round() as i64).to_string())
            .collect();
        add_random_sample_states(bigints, SubgraphType::BigInt);

        // text
        let texts: Vec<String> = (0..n_random_samples)
            .map(|_| {
                rng.clone().sample_iter(&Alphanumeric)
                    .take(8)
                    .map(char::from)
                    .collect()
            })
            .collect();
        add_random_sample_states(texts, SubgraphType::Text);

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
        add_random_sample_states(dates, SubgraphType::Date);

        // finalize tp -> sample map
        let mut tp_sample_map: HashMap<SubgraphType, Vec<String>> = HashMap::new();
        for (value, tp) in sample_value_tp_map.into_iter() {
            tp_sample_map.entry(tp).or_default().push(value);
        }
        
        // add literal auto aliases
        for auto_alias in QueryProps::all_auto_aliases() {
            add_state(auto_alias.to_uppercase(), "C_NAME");
        }

        // remove empty spaces in ids
        let mut new_state_id_bimap = BiHashMap::new();
        let mut free_id = 0usize;
        for (state, _old_id) in state_id_bimap.into_iter() {
            new_state_id_bimap.insert(state, free_id);
            free_id += 1;
        }
        let state_id_bimap = new_state_id_bimap;

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

    /// will panic if some states are unavailable
    fn gen_mask_strict(&self, available_states: &Vec<&str>, sections: &[&str]) -> Vec<bool> {
        self.gen_mask(available_states, sections, true)
    }

    /// when strict is: \
    ///   - true:  will panic if some states are unavailable \
    ///   - false: will panic if none of the states are available
    fn gen_mask(&self, available_states: &Vec<&str>, sections: &[&str], strict: bool) -> Vec<bool> {
        let mut mask = vec![false; self.state_id_bimap.len()];
        let mut is_empty = true;
        for state in available_states {
            for section in sections {
                let state_name = format!("{section}--{state}");
                let Some(state_id) = self.state_id_bimap.get_by_left(&state_name) else {
                    if strict {
                        panic!("State not found in bimap: {state_name}");
                    } else {
                        continue;
                    }
                };
                mask[*state_id] = true;
                is_empty = false;
            }
        }
        if !strict && is_empty {
            panic!("None of {:?} with sections {:?} were found", available_states, sections)
        }
        mask
    }

    /// returns the selected state minus category name
    fn get_state_by_id(&self, state_id: &usize) -> &str {
        let sp = self.state_id_bimap.get_by_right(state_id).unwrap().split("--").collect_vec();
        assert!(sp.len() == 2);
        sp[1]
    }

    /// sends the mask to the model and returns decision id 
    fn send_mask_receive_decision_id(&mut self, mask: Vec<bool>) -> DecisionResult<usize> {
        self.observation_sd.send(Some(mask.clone())).unwrap();
        let decision_id = match self.decision_rs.recv() {
            Ok(Some(decision_id)) => decision_id,
            Ok(None) => panic!("No decision (got None)"),
            Err(err) => panic!("decision_rs.recv() error: {err}")
        };
        assert!(mask[decision_id]);
        Ok(decision_id)
    }

    /// sends the mask to the model and returns the selected state minus category name
    fn send_mask_receive_state_str(&mut self, mask: Vec<bool>) -> DecisionResult<&str> {
        let decision_id = self.send_mask_receive_decision_id(mask)?;
        Ok(self.get_state_by_id(&decision_id))
    }

    fn get_sample_mask_by_type(&self, tp: &SubgraphType) -> Vec<bool> {
        self.gen_mask(
            &self.tp_sample_map
                .get(&tp).unwrap().iter()
                .map(|s| s.as_str()).collect(), 
            &[
                &format!("RANDOM_SAMPLE_{}", tp),
                &format!("DB_SAMPLE_{}", tp)
            ],
            false
        )
    }
}

impl PathwayGraphModel for InteractiveModel {
    fn as_value_chooser(&mut self) -> Option<&mut dyn QueryValueChooser> { Some(self) }
    
    fn process_state(&mut self, _call_stack: &Vec<StackFrame>, _popped_stack_frame: Option<&StackFrame>) { unimplemented!() }

    fn write_weights(&self, _file_path: &PathBuf) -> std::io::Result<()> { unimplemented!() }

    fn load_weights(&mut self, _file_path: &PathBuf) -> std::io::Result<()> { unimplemented!() }

    /// send states available for decision to observation_sd, \
    /// receiving the decision from decision_rs, checking that it \
    /// is valid.
    fn predict(&mut self, call_stack: &Vec<StackFrame>, node_outgoing: Vec<NodeParams>, current_exit_node_name: &SmolStr, _current_query_ast_ptr_opt: &mut Option<AtomicPtr<Query>>) -> DecisionResult<ModelPrediction> {
        if call_stack.len() > self.start_node_stack.len() {
            assert!(call_stack.len() == self.start_node_stack.len() + 1);
            self.start_node_stack.push(
                call_stack.last().unwrap().function_context.call_params.func_name.to_string()
            );
            let mask = self.gen_mask_strict(
                &vec![self.start_node_stack.last().unwrap().as_str()],
                &["GRAPH"]
            );
            self.send_mask_receive_decision_id(mask)?;  // no need to return the decision
        }
        self.start_node_stack.truncate(call_stack.len());

        let available_states = node_outgoing.iter().map(
            |params| {
                params.node_common.name.as_str()
            }
        ).collect_vec();
        let mask = self.gen_mask_strict(&available_states, &["GRAPH"]);
        let chosen_state = self.send_mask_receive_state_str(mask)?.to_string();

        if call_stack.len() == 1 && chosen_state == current_exit_node_name.as_str() {
            // as predict() will never be invoked again, send terminating observation
            self.observation_sd.send(None).unwrap();
        }

        Ok(ModelPrediction::Some(node_outgoing.into_iter().map(
            |params| (
                if params.node_common.name == chosen_state { 1f64 } else { 0f64 },
                params
            )
        ).collect()))
    }
}


impl QueryValueChooser for InteractiveModel {
    fn choose_table_name(&mut self, available_table_names: &Vec<ObjectName>) -> DecisionResult<ObjectName> {
        let state_to_obj = available_table_names.iter().map(
            |table_name| (format!("{table_name}").to_uppercase(), table_name)
        ).collect::<HashMap<_, _>>();
        let mask = self.gen_mask_strict(
            &state_to_obj.keys().map(|s| s.as_str()).collect_vec(),
            &["R_NAME"]
        );
        Ok((*state_to_obj.get(self.send_mask_receive_state_str(mask)?).unwrap()).clone())
    }

    fn choose_column(&mut self, clause_context: &ClauseContext, column_types: Vec<SubgraphType>, check_accessibility: query_info::CheckAccessibility, column_retrieval_options: query_info::ColumnRetrievalOptions) -> DecisionResult<(SubgraphType, [IdentName; 2])> {
        // all available columns
        let columns = clause_context.get_non_empty_column_levels_by_types(
            column_types.clone(), check_accessibility, column_retrieval_options.clone()
        ).into_iter().flat_map(|v| v.into_iter()).collect_vec();
        // relation -> column -> data
        let mut rstate_to_cstate_to_col: HashMap<String, HashMap<String, (&SubgraphType, [&IdentName; 2])>> = HashMap::new();
        for (tp, [r, c]) in columns {
            rstate_to_cstate_to_col
                .entry(format!("{r}").to_uppercase()).or_default()
                .insert(format!("{c}").to_uppercase(), (tp, [r, c]));
        }
        // send relation choice to model
        let rstate = self.send_mask_receive_state_str(
            self.gen_mask_strict(
                &rstate_to_cstate_to_col.keys().map(|s| s.as_str()).collect_vec(),
                &["R_NAME"]
            )
        )?;
        // send column choice to model
        let cstate_to_col = rstate_to_cstate_to_col.get(rstate).unwrap();
        let cstate: &str = self.send_mask_receive_state_str(
            self.gen_mask_strict(
                &cstate_to_col.keys().map(|s| s.as_str()).collect_vec(),
                &["C_NAME"]
            )
        )?;
        // extract data
        let (tp, [r, c]) = *cstate_to_col.get(cstate).unwrap();
        Ok((tp.clone(), [r.clone(), c.clone()]))
    }

    fn choose_select_ident_for_order_by(&mut self, aliases: &Vec<&IdentName>) -> DecisionResult<Ident> {
        let state_to_ident = aliases.iter().map(
            |alias| (format!("{alias}").to_uppercase(), *alias)
        ).collect::<HashMap<_, _>>();
        let mask = self.gen_mask_strict(
            &state_to_ident.keys().map(|s|s.as_str()).collect_vec(),
            &["C_NAME"]
        );
        Ok((*state_to_ident.get(self.send_mask_receive_state_str(mask)?).unwrap()).clone().into())
    }

    fn choose_aggregate_function_name(&mut self, func_names: Vec<&String>, _dist: rand::distributions::WeightedIndex<f64>) -> DecisionResult<ObjectName> {
        let mask = self.gen_mask_strict(
            &func_names.iter().map(|s| s.as_str()).collect_vec(),
            &["AGG_FUNC"]
        );
        let fname = self.send_mask_receive_state_str(mask)?.to_string();
        Ok(ObjectName(vec![Ident {
            value: fname,
            quote_style: None,
        }]))
    }

    fn choose_bigint(&mut self) -> DecisionResult<String> {
        let mask = self.get_sample_mask_by_type(&SubgraphType::BigInt);
        Ok(self.send_mask_receive_state_str(mask)?.to_string())
    }

    fn choose_integer(&mut self) -> DecisionResult<String> {
        let mask = self.get_sample_mask_by_type(&SubgraphType::Integer);
        Ok(self.send_mask_receive_state_str(mask)?.to_string())
    }

    fn choose_numeric(&mut self) -> DecisionResult<String> {
        let mask = self.get_sample_mask_by_type(&SubgraphType::Numeric);
        Ok(self.send_mask_receive_state_str(mask)?.to_string())
    }

    fn choose_text(&mut self) -> DecisionResult<String> {
        let mask = self.get_sample_mask_by_type(&SubgraphType::Text);
        Ok(self.send_mask_receive_state_str(mask)?.to_string())
    }

    fn choose_date(&mut self) -> DecisionResult<String> {
        let mask = self.get_sample_mask_by_type(&SubgraphType::Date);
        Ok(self.send_mask_receive_state_str(mask)?.to_string())
    }

    fn choose_timestamp(&mut self) -> DecisionResult<String> {
        let mask = self.get_sample_mask_by_type(&SubgraphType::Timestamp);
        Ok(self.send_mask_receive_state_str(mask)?.to_string())
    }

    fn choose_interval(&mut self, with_field: bool) -> DecisionResult<(String, Option<DateTimeField>)> {
        let mask = self.get_sample_mask_by_type(&SubgraphType::Interval);
        let interval = self.send_mask_receive_state_str(mask)?.to_string();
        Ok(if with_field {
            (
                interval.split_once(" seconds").unwrap().0.to_string(),
                Some(DateTimeField::Second)
            )
        } else { (interval, None) })
    }

    fn choose_qualified_wildcard_relation<'a>(&mut self, clause_context: &'a ClauseContext, wildcard_relations: &WildcardRelationsValue) -> DecisionResult<(Ident, &'a Relation)> {
        let aliases = wildcard_relations.relation_levels_selectable_by_qualified_wildcard
            .iter().flat_map(|v| v.into_iter()).collect_vec();
        let state_to_ident = aliases.iter().map(
            |alias| (format!("{alias}").to_uppercase(), *alias)
        ).collect::<HashMap<_, _>>();
        let mask = self.gen_mask_strict(
            &state_to_ident.keys().map(|s|s.as_str()).collect_vec(),
            &["R_NAME"]
        );
        let alias = (*state_to_ident.get(self.send_mask_receive_state_str(mask)?).unwrap()).clone();
        let relation = clause_context.get_relation_by_name(&alias);
        Ok((alias.into(), relation))
    }

    fn choose_select_alias(&mut self) -> DecisionResult<Ident> {
        self.free_select_alias_index += 1;
        let c_name = format!("C{}", self.free_select_alias_index);
        assert!(self.send_mask_receive_state_str(self.gen_mask_strict(
            &vec![c_name.as_str()],
            &["C_NAME"]
        ))? == c_name);
        Ok(Ident::new(c_name))
    }

    fn choose_from_alias(&mut self) -> DecisionResult<Ident> {
        self.free_from_alias_index += 1;
        let r_name = format!("T{}", self.free_from_alias_index);
        assert!(self.send_mask_receive_state_str(self.gen_mask_strict(
            &vec![r_name.as_str()],
            &["R_NAME"]
        ))? == r_name);
        Ok(Ident::new(r_name))
    }

    fn choose_from_column_renames(&mut self, _n_columns: usize) -> DecisionResult<Vec<Ident>> {
        Ok(vec![])  // do not rename anything
    }

    fn reset(&mut self) {
        self.free_select_alias_index = 0;
        self.free_from_alias_index = 0;
    }
}