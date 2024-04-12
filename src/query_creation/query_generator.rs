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
use sqlparser::ast::{
    BinaryOperator, Expr, Ident, Query, TrimWhereField, UnaryOperator
};

use crate::{config::TomlReadable, training::models::PathwayGraphModel};

use super::{
    super::unwrap_variant,
    state_generator::{markov_chain_generator::subgraph_type::SubgraphType, subgraph_type::ContainsSubgraphType, CallTypes}
};
use self::{
    aggregate_function_settings::AggregateFunctionDistribution, ast_builders::{number::NumberBuilder, query::QueryBuilder, types::TypesBuilder, val_3::Val3Builder}, query_info::{ClauseContext, DatabaseSchema}, value_choosers::QueryValueChooser
};

use super::state_generator::{MarkovChainGenerator, substitute_models::SubstituteModel, state_choosers::StateChooser};

#[derive(Debug, Clone)]
pub struct QueryGeneratorConfig {
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

pub struct QueryGenerator<DynMod: SubstituteModel, StC: StateChooser, QVC: QueryValueChooser> {
    config: QueryGeneratorConfig,
    state_generator: MarkovChainGenerator<StC>,
    substitute_model: Box<DynMod>,
    predictor_model: Option<Box<dyn PathwayGraphModel>>,
    value_chooser: Box<QVC>,
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

pub fn empty_ident() -> Ident {
    Ident::new("[?]")
}

#[derive(Debug, Clone)]
pub enum TypeAssertion<'a> {
    None,
    GeneratedBy(SubgraphType),
    CompatibleWith(SubgraphType),
    GeneratedByOneOf(&'a [SubgraphType]),
    CompatibleWithOneOf(&'a [SubgraphType]),
}

impl<'a> TypeAssertion<'a> {
    pub fn check(&'a self, selected_type: &SubgraphType, types_value: &Expr) {
        if !self.assertion_passed(selected_type) {
            panic!("types got an unexpected type: expected {:?}, got {:?}.\nExpr: {:?}", self, selected_type, types_value);
        }
    }

    pub fn check_type(&'a self, selected_type: &SubgraphType) {
        if !self.assertion_passed(selected_type) {
            panic!("types got an unexpected type: expected {:?}, got {:?}", self, selected_type);
        }
    }

    fn assertion_passed(&'a self, selected_type: &SubgraphType) -> bool {
        match self {
            TypeAssertion::None => true,
            TypeAssertion::GeneratedBy(by) => selected_type.is_same_or_more_determined_or_undetermined(by),
            TypeAssertion::CompatibleWith(with) => selected_type.is_compat_with(with),
            TypeAssertion::GeneratedByOneOf(by_one_of) => {
                by_one_of.contains_generator_of(selected_type)
            },
            TypeAssertion::CompatibleWithOneOf(with_one_of) => {
                with_one_of.iter().any(|with| selected_type.is_compat_with(with))
            },
        }
    }
}

impl<SubMod: SubstituteModel, StC: StateChooser, QVC: QueryValueChooser> QueryGenerator<SubMod, StC, QVC> {
    pub fn from_state_generator_and_schema(state_generator: MarkovChainGenerator<StC>, config: QueryGeneratorConfig) -> Self {
        let mut _self = QueryGenerator::<SubMod, StC, QVC> {
            state_generator,
            predictor_model: None,
            substitute_model: Box::new(SubMod::empty()),
            value_chooser: Box::new(QVC::new()),
            clause_context: ClauseContext::new(DatabaseSchema::parse_schema(&config.table_schema_path)),
            config,
            train_model: None,
            rng: ChaCha8Rng::seed_from_u64(1),
            current_query_raw_ptr: None,
        };

        if _self.config.print_schema {
            eprintln!("Relations:\n{}", _self.clause_context.schema_ref());
        }

        _self
    }

    fn next_state_opt(&mut self) -> Option<SmolStr> {
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

    fn handle_types(&mut self, type_assertion: TypeAssertion) -> (SubgraphType, Expr) {
        // TODO: remove the handler
        let mut expr = TypesBuilder::empty();
        let tp = TypesBuilder::build(self, &mut expr, type_assertion);
        (tp, expr)
    }

    fn handle_val_3(&mut self) -> (SubgraphType, Expr) {
        // TODO: remove the handler
        let mut l = Val3Builder::empty();
        let tp = Val3Builder::build(self, &mut l);
        (tp, l)
    }

    fn handle_number(&mut self) -> (SubgraphType, Expr) {
        // TODO: remove the handler
        let mut l = NumberBuilder::empty();
        let tp = NumberBuilder::build(self, &mut l);
        (tp, l)
    }

    /// subgraph def_text
    fn handle_text(&mut self) -> (SubgraphType, Expr) {
        self.expect_state("text");
        let string = match_next_state!(self, {
            "text_trim" => {
                let (trim_where, trim_what) = match_next_state!(self, {
                    "call6_types" => {
                        let types_value = self.handle_types(TypeAssertion::GeneratedBy(SubgraphType::Text)).1;
                        let spec_mode = match_next_state!(self, {
                            "BOTH" => TrimWhereField::Both,
                            "LEADING" => TrimWhereField::Leading,
                            "TRAILING" => TrimWhereField::Trailing,
                        });
                        self.expect_state("call5_types");
                        (Some(spec_mode), Some(Box::new(types_value)))
                    },
                    "call5_types" => (None, None),
                });
                let types_value = self.handle_types(TypeAssertion::GeneratedBy(SubgraphType::Text)).1;
                Expr::Trim {
                    expr: Box::new(types_value), trim_where, trim_what
                }
            },
            "text_concat" => {
                self.expect_state("call7_types");
                let types_value_1 = self.handle_types(TypeAssertion::GeneratedBy(SubgraphType::Text)).1;
                self.expect_state("text_concat_concat");
                self.expect_state("call8_types");
                let types_value_2 = self.handle_types(TypeAssertion::GeneratedBy(SubgraphType::Text)).1;
                Expr::BinaryOp {
                    left: Box::new(types_value_1),
                    op: BinaryOperator::StringConcat,
                    right: Box::new(types_value_2)
                }
            },
            "text_substring" => {
                self.expect_state("call9_types");
                let target_string = self.handle_types(TypeAssertion::GeneratedBy(SubgraphType::Text)).1;
                let mut substring_from = None;
                let mut substring_for = None;
                loop {
                    match_next_state!(self, {
                        "text_substring_from" => {
                            self.expect_state("call10_types");
                            substring_from = Some(Box::new(self.handle_types(TypeAssertion::GeneratedBy(SubgraphType::Integer)).1));
                        },
                        "text_substring_for" => {
                            self.expect_state("call11_types");
                            substring_for = Some(Box::new(self.handle_types(TypeAssertion::GeneratedBy(SubgraphType::Integer)).1));
                            self.expect_state("text_substring_end");
                            break;
                        },
                        "text_substring_end" => break,
                    })
                }
                Expr::Substring {
                    expr: Box::new(target_string),
                    substring_from,
                    substring_for,
                }
            },
        });
        self.expect_state("EXIT_text");
        (SubgraphType::Text, string)
    }

    /// subgarph def_date
    fn handle_date(&mut self) -> (SubgraphType, Expr) {
        self.expect_state("date");

        let expr = match_next_state!(self, {
            "date_binary" => {
                let (mut date, op, mut integer) = match_next_state!(self, {
                    "date_add_subtract" => {
                        self.expect_state("call86_types");
                        let date = self.handle_types(TypeAssertion::GeneratedBy(SubgraphType::Date)).1;
                        self.expect_state("call88_types");
                        let integer = self.handle_types(TypeAssertion::GeneratedBy(SubgraphType::Integer)).1;
                        let op = match_next_state!(self, {
                            "date_add_subtract_plus" => BinaryOperator::Plus,
                            "date_add_subtract_minus" => BinaryOperator::Minus,
                        });
                        (date, op, integer)
                    },
                });
                match_next_state!(self, {
                    "date_swap_arguments" => {
                        assert!(op == BinaryOperator::Plus);
                        std::mem::swap(&mut date, &mut integer);
                        self.expect_state("EXIT_date");
                    },
                    "EXIT_date" => { },
                });
                Expr::BinaryOp {
                    left: Box::new(date),
                    op,
                    right: Box::new(integer)
                }
            },
        });
        
        (SubgraphType::Date, expr)
    }

    /// subgraph def_timestamp
    fn handle_timestamp(&mut self) -> (SubgraphType, Expr) {
        self.expect_state("timestamp");

        let expr = match_next_state!(self, {
            "timestamp_binary" => {
                let (mut date, op, mut interval) = match_next_state!(self, {
                    "timestamp_add_subtract" => {
                        self.expect_state("call94_types");
                        let date = self.handle_types(TypeAssertion::GeneratedBy(SubgraphType::Date)).1;
                        self.expect_state("call95_types");
                        let interval = self.handle_types(TypeAssertion::GeneratedBy(SubgraphType::Interval)).1;
                        let op = match_next_state!(self, {
                            "timestamp_add_subtract_plus" => BinaryOperator::Plus,
                            "timestamp_add_subtract_minus" => BinaryOperator::Minus,
                        });
                        (date, op, interval)
                    },
                });
                match_next_state!(self, {
                    "timestamp_swap_arguments" => {
                        assert!(op == BinaryOperator::Plus);
                        std::mem::swap(&mut date, &mut interval);
                        self.expect_state("EXIT_timestamp");
                    },
                    "EXIT_timestamp" => { },
                });
                Expr::BinaryOp {
                    left: Box::new(date),
                    op,
                    right: Box::new(interval)
                }
            },
        });
        
        (SubgraphType::Timestamp, expr)
    }

    /// subgarph def_interval
    fn handle_interval(&mut self) -> (SubgraphType, Expr) {
        self.expect_state("interval");

        let expr = match_next_state!(self, {
            "interval_binary" => {
                let (left, op, right) = match_next_state!(self, {
                    "interval_add_subtract" => {
                        self.expect_state("call91_types");
                        let interval_1 = self.handle_types(TypeAssertion::GeneratedBy(SubgraphType::Interval)).1;
                        let op = match_next_state!(self, {
                            "interval_add_subtract_plus" => BinaryOperator::Plus,
                            "interval_add_subtract_minus" => BinaryOperator::Minus,
                        });
                        self.expect_state("call92_types");
                        let interval_2 = self.handle_types(TypeAssertion::GeneratedBy(SubgraphType::Interval)).1;
                        (interval_1, op, interval_2)
                    },
                });
                Expr::BinaryOp {
                    left: Box::new(left),
                    op,
                    right: Box::new(right)
                }
            },
            "interval_unary_minus" => {
                self.expect_state("call93_types");
                let interval = self.handle_types(TypeAssertion::GeneratedBy(SubgraphType::Interval)).1;
                Expr::UnaryOp {
                    op: UnaryOperator::Minus,
                    expr: Box::new(interval)
                }
            },
        });

        self.expect_state("EXIT_interval");
        
        (SubgraphType::Interval, expr)
    }

    /// starting point; calls handle_query for the first time.\
    /// NOTE: If you use this function without a predictor model,\
    /// it will use uniform distributions 
    pub fn generate(&mut self) -> Query {
        if let Some(model) = self.predictor_model.as_mut() {
            model.start_inference();
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
        self.substitute_model = Box::new(SubMod::empty());
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
    pub fn generate_with_substitute_model_and_value_chooser(&mut self, dynamic_model: Box<SubMod>, value_chooser: Box<QVC>) -> Query {
        self.substitute_model = dynamic_model;
        self.value_chooser = value_chooser;
        self.generate()
    }

    /// generate the query and feed it to the model. Useful for training.
    pub fn generate_and_feed_to_model(
            &mut self, dynamic_model: Box<SubMod>, value_chooser: Box<QVC>, model: Box<dyn PathwayGraphModel>
        ) -> Box<dyn PathwayGraphModel> {
        self.train_model = Some(model);
        self.generate_with_substitute_model_and_value_chooser(dynamic_model, value_chooser);
        self.train_model.take().unwrap()
    }
}
