#[macro_use]
pub mod query_info;
pub mod ast_builders;
pub mod call_modifiers;
pub mod value_choosers;
pub mod expr_precedence;
pub mod aggregate_function_settings;

use std::path::PathBuf;

use rand::SeedableRng;
use rand_chacha::ChaCha8Rng;
use smol_str::SmolStr;
use sqlparser::ast::{Expr, Ident, Query};

use crate::{config::TomlReadable, training::models::PathwayGraphModel};

use super::{
    super::unwrap_variant,
    state_generator::{markov_chain_generator::subgraph_type::SubgraphType, CallTypes}
};
use self::{
    aggregate_function_settings::AggregateFunctionDistribution, ast_builders::query::QueryBuilder,
    query_info::{ClauseContext, DatabaseSchema}, value_choosers::{QueryValueChooser, RandomValueChooser}
};

use super::state_generator::{MarkovChainGenerator, substitute_models::SubstituteModel, state_choosers::StateChooser};

#[derive(Debug, Clone)]
pub struct QueryGeneratorConfig {
    pub use_probabilistic_state_chooser: bool,
    pub use_model: bool,
    pub print_queries: bool,
    pub print_schema: bool,
    pub table_schema_path: PathBuf,
    pub substitute_model_name: String,
    pub aggregate_functions_distribution: AggregateFunctionDistribution,
}

impl TomlReadable for QueryGeneratorConfig {
    fn from_toml(toml_config: &toml::Value) -> Self {
        let section = &toml_config["query_generator"];
        Self {
            use_probabilistic_state_chooser: section["use_probabilistic_state_chooser"].as_bool().unwrap(),
            use_model: section["use_model"].as_bool().unwrap(),
            print_queries: section["print_queries"].as_bool().unwrap(),
            print_schema: section["print_schema"].as_bool().unwrap(),
            table_schema_path: PathBuf::from(section["table_schema_path"].as_str().unwrap()),
            substitute_model_name: section["substitute_model"].as_str().unwrap().to_string(),
            aggregate_functions_distribution: AggregateFunctionDistribution::parse_file(
                PathBuf::from(section["aggregate_functions_distribution_map_file"].as_str().unwrap()),
            ),
        }
    }
}

pub struct QueryGenerator<StC: StateChooser> {
    config: QueryGeneratorConfig,
    state_generator: MarkovChainGenerator<StC>,
    substitute_model: Box<dyn SubstituteModel>,
    predictor_model: Option<Box<dyn PathwayGraphModel>>,
    value_chooser: Box<dyn QueryValueChooser>,
    clause_context: ClauseContext,
    train_model: Option<Box<dyn PathwayGraphModel>>,
    rng: ChaCha8Rng,
    /// this is used to give a model a look at the whole query\
    /// while it is still being constructed (in other words,\
    /// some builder has the mutable borrow to it)
    /// 
    /// Since there's no multithreading for now, I used unsafe\
    /// code to achieve that.
    /// 
    /// Basically we have to be able to have a look at the whole\
    /// tree while also holding an &mut to some component of it.
    current_query_raw_ptr: Option<*const Query>,
}

pub trait Unnested {
    fn unnested(&self) -> &Expr;
}

impl Unnested for Expr {
    fn unnested(&self) -> &Expr {
        match self {
            Expr::Nested(expr) => expr.unnested(),
            any => any,
        }
    }
}

macro_rules! match_next_state {
    ($generator:expr, { $($state:pat $(if $guard:expr)? => $body:block),* $(,)? }) => {
        match $generator.next_state().as_str() {
            $(
                $state $(if $guard)? => $body,
            )*
            _any => $generator.panic_unexpected(_any),
        }
    };
    ($generator:expr, { $($state:pat $(if $guard:expr)? => $body:expr),* $(,)? }) => {
        match $generator.next_state().as_str() {
            $(
                $state $(if $guard)? => { $body },
            )*
            _any => $generator.panic_unexpected(_any),
        }
    };
}

pub(crate) use match_next_state;

/// Returns the value chooser of the generator, which amounts to\
/// either the .predictor_model of .value_chooser fields
/// 
/// Not a method because we don't want to borrow the whole generator
/// mutably for this, since other fields may be needed
macro_rules! value_chooser {
    ($generator:expr) => {{
        let vc = if let Some(predictor_model) = $generator.predictor_model.as_mut() {
            if let Some(predictor_model) = predictor_model.as_value_chooser() {
                predictor_model
            } else { &mut *$generator.value_chooser }
        } else { &mut *$generator.value_chooser };
        vc.set_choice_query_ast($generator.current_query_raw_ptr.map(|p| unsafe { &*p }).unwrap());
        vc
    }};
}

pub(crate) use value_chooser;

const HIGHLIGHT_STR: &str = "[?]";

pub fn highlight_str() -> String {
    HIGHLIGHT_STR.to_string()
}

pub fn highlight_ident() -> Ident {
    Ident::new(HIGHLIGHT_STR)
}

impl<StC: StateChooser> QueryGenerator<StC> {
    pub fn from_state_generator_and_config(state_generator: MarkovChainGenerator<StC>, config: QueryGeneratorConfig, substitute_model: Box<dyn SubstituteModel>) -> Self {
        Self::from_state_generator_and_config_with_schema(
            state_generator,
            DatabaseSchema::parse_schema(&config.table_schema_path),
            config,
            substitute_model
        )
    }

    pub fn from_state_generator_and_config_with_schema(state_generator: MarkovChainGenerator<StC>, schema: DatabaseSchema, config: QueryGeneratorConfig, substitute_model: Box<dyn SubstituteModel>) -> Self {
        let mut _self = QueryGenerator::<StC> {
            state_generator,
            predictor_model: None,
            substitute_model,
            value_chooser: Box::new(RandomValueChooser::new()),
            clause_context: ClauseContext::new(schema),
            config,
            train_model: None,
            rng: ChaCha8Rng::seed_from_u64(0),
            current_query_raw_ptr: None,
        };

        if _self.config.print_schema {
            eprintln!("Relations:\n{}", _self.clause_context.schema_ref());
        }

        _self
    }

    fn _perform_model_backtracking(&mut self) {
        if let Some(model) = self.predictor_model.as_mut() {
            if let Some(backtracking_checkpoint) = model.try_get_backtracking_checkpoint() {
                self.state_generator.restore_chain_state(backtracking_checkpoint);
            }
            let chain_state_checkpoint = self.state_generator.get_chain_state_checkpoint(false);
            model.add_checkpoint(chain_state_checkpoint);
        }
    }

    fn next_state_opt(&mut self) -> Option<SmolStr> {
        // self.perform_model_backtracking();
        self.state_generator.next_node_name(
            &mut self.rng,
            &mut self.clause_context,
            &mut *self.substitute_model,
            self.predictor_model.as_mut(),
            self.current_query_raw_ptr.map(|p| unsafe { &*p }),
        ).unwrap()
    }

    fn next_state(&mut self) -> SmolStr {
        let next_state = self.next_state_opt().unwrap();
        // eprintln!("{next_state}");
        if let Some(ref mut model) = self.train_model {
            model.process_state(
                self.state_generator.call_stack_ref(),
                self.state_generator.get_last_popped_stack_frame_ref(),
            );
        }
        next_state
    }

    fn panic_unexpected(&mut self, state: &str) -> ! {
        self.state_generator.print_stack();
        panic!("Unexpected state: {state}");
    }

    fn expect_state(&mut self, state: &str) {
        let new_state = self.next_state();
        if new_state.as_str() != state {
            self.state_generator.print_stack();
            panic!("Expected {state}, got {new_state}")
        }
    }

    fn expect_states(&mut self, states: &[&str]) {
        for state in states {
            self.expect_state(*state)
        }
    }

    fn assert_single_type_argument(&self) -> SubgraphType {
        let arg_types = unwrap_variant!(self.state_generator.get_fn_selected_types_unwrapped(), CallTypes::TypeList);
        if let [tp] = arg_types.as_slice() {
            tp.clone()
        } else {
            panic!("This subgraph does not accept multiple types as argument. Got: {:?}", arg_types);
        }
    }

    /// starting point; calls QueryBuilder::build for the first time.\
    /// NOTE: If you use this function without a predictor model,\
    /// it will use uniform distributions 
    pub fn generate(&mut self) -> Query {
        if let Some(model) = self.predictor_model.as_mut() {
            model.start_inference(self.clause_context.schema_ref().get_schema_string());
        }
        let mut query = QueryBuilder::nothing();

        self.current_query_raw_ptr = Some(&query);  // scary!
        QueryBuilder::build(self, &mut query);
        self.current_query_raw_ptr.take();

        self.value_chooser.reset();
        // reset the generator
        if let Some(state) = self.next_state_opt() {
            panic!("Couldn't reset state_generator: Received {state}");
        }
        self.substitute_model.reset();
        if let Some(model) = self.predictor_model.as_mut() {
            model.end_inference();
        }
        query
    }

    /// generate the next query with the provided model generating the probabilities
    pub fn generate_with_model(&mut self, model: Box<dyn PathwayGraphModel>) -> (Box<dyn PathwayGraphModel>, Query) {
        self.predictor_model = Some(model);
        let query = self.generate();
        (self.predictor_model.take().unwrap(), query)
    }

    /// generate the next query with the provided dynamic model and value choosers
    pub fn generate_with_substitute_model_and_value_chooser(&mut self, substitute_model: Box<dyn SubstituteModel>, value_chooser: Box<dyn QueryValueChooser>) -> Query {
        self.substitute_model = substitute_model;
        self.value_chooser = value_chooser;
        self.generate()
    }

    /// generate the query and feed it to the model. Useful for training.
    pub fn generate_and_feed_to_model(
        &mut self, substitute_model: Box<dyn SubstituteModel>, value_chooser: Box<dyn QueryValueChooser>, model: Box<dyn PathwayGraphModel>
    ) -> Box<dyn PathwayGraphModel> {
        self.train_model = Some(model);
        self.generate_with_substitute_model_and_value_chooser(substitute_model, value_chooser);
        self.train_model.take().unwrap()
    }
}
