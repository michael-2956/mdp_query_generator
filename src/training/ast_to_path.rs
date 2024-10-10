use std::{error::Error, fmt, path::PathBuf, str::FromStr, sync::{Arc, Mutex}};

pub mod tester;

use itertools::Itertools;
use rand::SeedableRng;
use rand_chacha::ChaCha8Rng;
use smol_str::SmolStr;
use sqlparser::ast::{self, BinaryOperator, DataType, DateTimeField, Distinct, Expr, FunctionArg, FunctionArgExpr, GroupByExpr, Ident, Interval, ObjectName, OrderByExpr, Query, Select, SelectItem, SetExpr, TableFactor, TableWithJoins, TimezoneInfo, TrimWhereField, UnaryOperator, Value};

use crate::{
    config::TomlReadable, query_creation::{
        query_generator::{
            aggregate_function_settings::{AggregateFunctionAgruments, AggregateFunctionDistribution}, ast_builders::types_value::TypeAssertion, call_modifiers::{SelectAccessibleColumnsValue, ValueSetterValue, WildcardRelationsValue}, query_info::{ClauseContext, ClauseContextCheckpoint, ColumnRetrievalOptions, DatabaseSchema, IdentName, QueryProps}
        },
        state_generator::{
            markov_chain_generator::{
                error::SyntaxError, markov_chain::{CallModifiers, MarkovChain, QueryTypes}, ChainStateCheckpoint, DynClone, StateGeneratorConfig
            },
            state_choosers::MaxProbStateChooser,
            subgraph_type::{ContainsSubgraphType, SubgraphType}, substitute_models::DeterministicModel, CallTypes, MarkovChainGenerator
        }
    },
    unwrap_pat, unwrap_variant, unwrap_variant_or_else
};

pub struct AST2PathTestingConfig {
    pub schema: PathBuf,
    pub n_tests: usize,
}

impl TomlReadable for AST2PathTestingConfig {
    fn from_toml(toml_config: &toml::Value) -> Self {
        let section = &toml_config["ast_to_path_testing"];
        Self {
            schema: PathBuf::from_str(section["testing_schema"].as_str().unwrap()).unwrap(),
            n_tests: section["n_tests"].as_integer().unwrap() as usize,
        }
    }
}

#[derive(Debug)]
pub struct ConvertionError {
    reason: String,
}

impl Error for ConvertionError { }

impl fmt::Display for ConvertionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "AST to path convertion error:\n{}", self.reason.clone().get_indentated_string())
    }
}

impl ConvertionError {
    pub fn new(reason: String) -> Self {
        Self {
            reason
        }
    }
}

#[derive(Debug)]
pub struct ConvertionErrorWithPartialSelectType {
    select_type: Vec<SubgraphType>,
    real_n_cols: Option<usize>,
    err: ConvertionError,
}

impl Error for ConvertionErrorWithPartialSelectType { }

impl fmt::Display for ConvertionErrorWithPartialSelectType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Partial Select Type: {:?}\nError:\n{}", self.select_type, self.err)
    }
}

macro_rules! add_pst {
    ($result:expr) => {
        $result.map_err(|err| ConvertionErrorWithPartialSelectType {
            real_n_cols: None, select_type: vec![], err
        })
    };
}

impl ConvertionErrorWithPartialSelectType {
    fn with_reason(reason: String) -> Self {
        Self {
            real_n_cols: None,
            select_type: vec![],
            err: ConvertionError::new(reason),
        }
    }

    fn new(reason: String, real_n_cols: Option<usize>, select_type: Vec<SubgraphType>) -> Self {
        Self {
            real_n_cols,
            select_type,
            err: ConvertionError::new(reason),
        }
    }
}

trait GetIndentated {
    fn get_indentated_string(self) -> String;
}

impl GetIndentated for ConvertionError {
    fn get_indentated_string(self) -> String {
        let mut s = format!(".  {self}");
        s = s.replace("\n", "\n.  ");
        s
    }
}

impl GetIndentated for String {
    fn get_indentated_string(self) -> String {
        let mut s = format!(".  {self}");
        s = s.replace("\n", "\n.  ");
        s
    }
}

#[derive(Debug, Clone)]
pub enum PathNode {
    State(SmolStr),
    NewSubgraph(SmolStr),
    SelectedTableName(ObjectName),
    SelectedColumnName([IdentName; 2]),
    ORDERBYSelectReference(Ident),
    SelectedAggregateFunctions(ObjectName),
    NumericValue(String),
    IntegerValue(String),
    BigIntValue(String),
    StringValue(String),
    DateValue(String),
    TimestampValue(String),
    IntervalValue((String, Option<DateTimeField>)),
    QualifiedWildcardSelectedRelation(Ident),
    SelectAlias(Ident),
    FromAlias(Ident),
    FromColumnRenames(Vec<Ident>),
}

pub struct PathGenerator {
    current_path: Vec<PathNode>,
    state_generator: MarkovChainGenerator<MaxProbStateChooser>,
    state_selector: DeterministicModel,
    clause_context: ClauseContext,
    aggregate_functions_distribution: AggregateFunctionDistribution,
    /// checkpoint to restore to initial state in case of a convertion error
    zero_checkpoint: Option<Checkpoint>,
    path_until_last_rewind: Vec<String>,
    rng: ChaCha8Rng,
    try_with_quotes: bool,
}

impl PathGenerator {
    pub fn new(
        database_schema: DatabaseSchema,
        chain_config: &StateGeneratorConfig,
        aggregate_functions_distribution: AggregateFunctionDistribution,
    ) -> Result<Self, SyntaxError> {
        let mut _self = Self {
            current_path: vec![],
            state_generator: MarkovChainGenerator::<MaxProbStateChooser>::with_config(chain_config)?,
            state_selector: DeterministicModel::empty(),
            clause_context: ClauseContext::new(database_schema),
            aggregate_functions_distribution,
            path_until_last_rewind: vec![],
            rng: ChaCha8Rng::seed_from_u64(0),
            zero_checkpoint: None,
            try_with_quotes: false,
        };
        _self.zero_checkpoint = Some(_self.get_checkpoint(false));
        Ok(_self)
    }

    fn process_query(&mut self, query: &Box<Query>) -> Result<(), ConvertionError> {
        match self.handle_query(query) {
            Ok(..) => { },
            Err(err) => {
                // reset the generator (in case of an error)
                self.restore_checkpoint_consume(self.zero_checkpoint.as_ref().unwrap().dyn_clone());
                return Err(err)
            }
        }
        // reset the generator
        if let Some(state) = self.next_state_opt().unwrap() {
            panic!("Couldn't reset state generator: Received {state}");
        }
        Ok(())
    }

    pub fn set_try_with_quotes(&mut self, try_with_quotes: bool) {
        self.try_with_quotes = try_with_quotes;
    }

    pub fn get_query_path(&mut self, query: &Box<Query>) -> Result<Vec<PathNode>, ConvertionError> {
        match self.process_query(query) {
            Ok(..) => { },
            Err(err) => {
                return Err(ConvertionError::new(format!(
                    "Error converting query:\n{query}\nError: {err}"
                )))
            },
        };
        Ok(std::mem::replace(&mut self.current_path, vec![]))
    }

    pub fn markov_chain_ref(&self) -> &MarkovChain {
        self.state_generator.markov_chain_ref()
    }
}

macro_rules! unexpected_expr {
    ($el: expr) => {{
        return Err(ConvertionError::new(format!("Unexpected expression: {:?}", $el)))
    }};
}

macro_rules! unexpected_subgraph_type {
    ($el: expr) => {{
        return Err(ConvertionError::new(format!("Unexpected subgraph type: {:?}", $el)))
    }};
}

enum Checkpoint {
    CutOff {
        clause_context_checkpoint: ClauseContextCheckpoint,
        chain_state_checkpoint: ChainStateCheckpoint,
        path_cutoff_length: usize,
    },
    Full {
        clause_context_checkpoint: ClauseContextCheckpoint,
        chain_state_checkpoint: ChainStateCheckpoint,
        path: Vec<PathNode>,
    },
}

impl DynClone for Checkpoint {
    fn dyn_clone(&self) -> Self {
        match self {
            Checkpoint::CutOff {
                clause_context_checkpoint,
                chain_state_checkpoint,
                path_cutoff_length
            } => Checkpoint::CutOff {
                clause_context_checkpoint: clause_context_checkpoint.clone(),
                chain_state_checkpoint: chain_state_checkpoint.dyn_clone(),
                path_cutoff_length: *path_cutoff_length,
            },
            Checkpoint::Full {
                clause_context_checkpoint,
                chain_state_checkpoint,
                path
            } => Checkpoint::Full {
                clause_context_checkpoint: clause_context_checkpoint.clone(),
                chain_state_checkpoint: chain_state_checkpoint.dyn_clone(),
                path: path.clone(),
            }
        }
    }
}

fn determine_n_cols(set_expr: &SetExpr) -> Option<usize> {
    match set_expr { // there's a problem when a wildcard is used. use error-based approach instead. Use this function in case there are no wildcards
                     // on error, we might use the FROM info to test how many columns there actually are and use the estimate instead
        SetExpr::Select(select) => if select.projection.iter().any(|item| {
            matches!(item, SelectItem::Wildcard(..) | SelectItem::QualifiedWildcard(..))
        }) { None } else { Some(select.projection.len()) },
        SetExpr::Query(query) => determine_n_cols(&query.body),
        SetExpr::SetOperation { op: _, set_quantifier: _, left, right: _ } => determine_n_cols(&left),
        any => unimplemented!("{any}"),
    }
}

fn determine_n_cols_with_clause_context(projection: &Vec<SelectItem>, clause_context: &ClauseContext) -> usize {
    projection.iter().map(|item| {
        if matches!(item, SelectItem::Wildcard(..) | SelectItem::QualifiedWildcard(..)) {
            match item {
                SelectItem::Wildcard(..) => clause_context.top_active_from().get_n_wildcard_columns(),
                SelectItem::QualifiedWildcard(rel_name, _) => {
                    let relation = match rel_name.0.as_slice() {
                        [ident] => {
                            let identname: IdentName = ident.clone().into();
                            clause_context.get_relation_by_name(&identname)
                        },
                        any => panic!("schema.table alias is not supported: {}", ObjectName(any.to_vec())),
                    };
                    relation.get_n_wildcard_columns()
                }
                _ => unreachable!(),
            }
        } else {
            1usize
        }
    }).sum()
}

impl PathGenerator {
    fn next_state_opt(&mut self) -> Result<Option<SmolStr>, ConvertionError> {
        match self.state_generator.next_node_name(
            &mut self.rng, &self.clause_context, &mut self.state_selector, None, None
        ) {
            Ok(state) => Ok(state),
            Err(err) => Err(ConvertionError::new(format!("{err}"))),
        }
    } 

    fn try_push_state(&mut self, state: &str) -> Result<(), ConvertionError> {
        // eprintln!("pushing state: {state}, path length: {}", self.current_path.len());
        let is_new_function_initial_state = self.state_generator.has_pending_call();
        let state = SmolStr::new(state);
        self.state_selector.set_state(state.clone());
        if let Some(actual_state) = self.next_state_opt().map_err(|err| {
            ConvertionError::new(format!("{err}\nPath until last rewind: {:#?}", self.path_until_last_rewind))
        })? {
            if actual_state != state {
                self.state_generator.print_stack();
                panic!("State generator returned {actual_state}, expected {state}");
            }
        } else {
            self.state_generator.print_stack();
            panic!("State generator stopped prematurely");
        }
        self.path_until_last_rewind.push(format!("{state}"));
        if is_new_function_initial_state {
            self.current_path.push(PathNode::NewSubgraph(state));
        } else {
            self.current_path.push(PathNode::State(state));
        }
        Ok(())
    }

    fn try_push_states(&mut self, state_names: &[&str]) -> Result<(), ConvertionError> {
        for state_name in state_names {
            self.try_push_state(state_name)?;
        }
        Ok(())
    }

    fn assert_single_type_argument(&self) -> SubgraphType {
        let arg_types = unwrap_variant!(self.state_generator.get_fn_selected_types_unwrapped(), CallTypes::TypeList);
        if let [tp] = arg_types.as_slice() {
            tp.clone()
        } else {
            panic!("This subgraph does not accept multiple types as argument. Got: {:?}", arg_types);
        }
    }

    fn push_node(&mut self, path_node: PathNode) {
        self.current_path.push(path_node);
    }

    fn get_checkpoint(&self, cutoff: bool) -> Checkpoint {
        if cutoff {
            Checkpoint::CutOff {
                clause_context_checkpoint: self.clause_context.get_checkpoint(cutoff),
                chain_state_checkpoint: self.state_generator.get_chain_state_checkpoint(cutoff),
                path_cutoff_length: self.current_path.len(),
            }
        } else {
            Checkpoint::Full {
                clause_context_checkpoint: self.clause_context.get_checkpoint(cutoff),
                chain_state_checkpoint: self.state_generator.get_chain_state_checkpoint(cutoff),
                path: self.current_path.clone(),
            }
        }
    }

    fn restore_checkpoint(&mut self, checkpoint: &Checkpoint) {
        self.restore_checkpoint_consume(checkpoint.dyn_clone());
    }

    fn restore_checkpoint_consume(&mut self, checkpoint: Checkpoint) {
        self.path_until_last_rewind.clear();
        match checkpoint {
            Checkpoint::CutOff {
                clause_context_checkpoint,
                chain_state_checkpoint,
                path_cutoff_length
            } => {
                self.clause_context.restore_checkpoint(clause_context_checkpoint);
                self.state_generator.restore_chain_state(chain_state_checkpoint);
                self.current_path.truncate(path_cutoff_length);
            },
            Checkpoint::Full {
                clause_context_checkpoint,
                chain_state_checkpoint,
                path
            } => {
                self.clause_context.restore_checkpoint(clause_context_checkpoint);
                self.state_generator.restore_chain_state(chain_state_checkpoint);
                self.current_path = path;
            },
        }
    }

    /// subgraph def_Query
    fn handle_query(&mut self, query: &Box<Query>) -> Result<Vec<(Option<IdentName>, SubgraphType)>, ConvertionError> {
        self.clause_context.on_query_begin(self.state_generator.get_fn_modifiers_opt());
        self.try_push_state("Query")?;

        self.handle_query_body(&query.body)?;

        self.try_push_state("call0_ORDER_BY")?;
        self.handle_order_by(&query.order_by)?;

        self.try_push_state("call0_LIMIT")?;
        self.handle_limit(&query.limit)?;

        self.try_push_state("EXIT_Query")?;
        let output_type = self.clause_context.query_mut().pop_output_type();
        // if output_type.iter().filter_map(|(o, _)| o.as_ref()).any(|o| o.value == "C12") {
        //     eprintln!("output: {:?}", output_type);
        // }
        self.clause_context.on_query_end();

        return Ok(output_type)
    }

    fn handle_query_body(&mut self, query_body: &SetExpr) -> Result<(), ConvertionError> {
        self.try_push_state("call1_set_expression_determine_type")?;
        
        let mut n_cols = determine_n_cols(query_body);
        let expr_str = Some(format!("{query_body}"));
        loop {
            let mut wildcards_can_cause_errors = false;
            let mut n_cols_arg = 1usize;
            match &n_cols {
                Some(n_cols) => n_cols_arg = *n_cols,
                None => wildcards_can_cause_errors = true,
            }
            let checkpoint_opt = if wildcards_can_cause_errors {
                Some(self.get_checkpoint(true))
            } else { None };
            let select_err = self.handle_set_expression_determine_type_and_try(
                expr_str.clone(),
                n_cols_arg,
                n_cols_arg,
                vec![],
                wildcards_can_cause_errors,
                |_self, column_types| {
                    let column_type_list = QueryTypes::ColumnTypeLists {
                        column_type_lists: column_types.into_iter().map(|x| vec![x]).collect_vec()
                    };
                    add_pst!(_self.try_push_state("call0_set_expression"))?;
                    _self.state_generator.set_known_query_type_list(column_type_list);
                    _self.handle_set_expression(query_body)?;
                    Ok(())
                }
            )?;
            match select_err {
                Some(err) => {
                    if !wildcards_can_cause_errors {
                        return Err(ConvertionError::new(format!("Couldn't select SELECT type. Got the error:\n{}", err.err)))
                    } else {
                        self.restore_checkpoint_consume(checkpoint_opt.unwrap());
                        // wildcard caused an error, so try another time.
                        // the number of columns actually known now
                        n_cols = err.real_n_cols;
                    }
                },
                None => break,
            }
        }
        Ok(())
    }

    /// subgraph def_set_expression_determine_type
    fn handle_set_expression_determine_type_and_try<
        F: Fn(&mut PathGenerator, Vec<SubgraphType>) -> Result<(), ConvertionErrorWithPartialSelectType> + Clone
    >(
        &mut self,
        expr_str: Option<String>,
        n_cols_remaining: usize,
        n_cols_total: usize,
        column_types: Vec<SubgraphType>,
        wildcards_can_cause_errors: bool,
        and_try: F
    ) -> Result<Option<ConvertionErrorWithPartialSelectType>, ConvertionError> {
        self.try_push_states(&["set_expression_determine_type", "call9_types_type"])?;
        let column_type_lists = unwrap_variant!(self.state_generator.get_fn_selected_types_unwrapped(), CallTypes::QueryTypes);
        let (first_column_list, remaining_columns) = column_type_lists.split_first();
        self.state_generator.set_known_list(first_column_list.clone());
        let n_cols_completed = n_cols_total - n_cols_remaining;
        let error_with_incomplete_type: Arc<Mutex<Option<ConvertionErrorWithPartialSelectType>>> = Arc::new(Mutex::new(None));
        self.handle_types_type_and_try(
            expr_str.clone(),
            TypeAssertion::GeneratedByOneOf(&first_column_list),
            |_self, tp: SubgraphType| {
                let mut column_types = column_types.clone();
                column_types.push(tp.clone());
                if n_cols_remaining == 1 {
                    // last column: attempt to finalize and proceed
                    _self.try_push_state("set_expression_determine_type_can_finish")?;
                    for _ in 0..n_cols_total {  // so exit the set_expression_determine_type subgraph
                        _self.try_push_state("EXIT_set_expression_determine_type")?;
                    }  // and try to proceed with the selected types
                    match and_try(_self, column_types.clone()) {
                        Ok(()) => { },  // all worked ok. Finish without an error.
                        Err(err) => {
                            if !wildcards_can_cause_errors && n_cols_completed == err.select_type.len() {
                                // we, the last link, are responsible for the error.
                                return Err(ConvertionError::new(format!(
                                    "Having determined {n_cols_completed}/{n_cols_total} types: {:?},\nTried {tp} for the last column, but got an error:\n{}",
                                    err.select_type, err.err
                                )))
                            } else {
                                // will make us, the last link in the chain, return Ok(err)
                                // this will in turn allow to backtrack until we meet the problem causing type,
                                // since we are not responsible for the error
                                *error_with_incomplete_type.lock().unwrap() = Some(err);
                            }
                        },
                    }
                } else {
                    // not the last column: proceed to the next column recursively
                    _self.try_push_state("call0_set_expression_determine_type")?;
                    _self.state_generator.set_known_query_type_list(remaining_columns.clone());
                    let inner_error_opt = _self.handle_set_expression_determine_type_and_try(
                        expr_str.clone(),
                        n_cols_remaining - 1,
                        n_cols_total,
                        column_types,
                        wildcards_can_cause_errors,
                        and_try.clone()
                    )?;
                    if let Some(inner_error) = inner_error_opt {
                        // inner handler finished because of an error
                        if !wildcards_can_cause_errors && n_cols_completed == inner_error.select_type.len() {
                            // we are the one responsible for the error, so try a new type:
                            return Err(ConvertionError::new(format!(
                                "Having determined {n_cols_completed}/{n_cols_total} types: {:?},\nTried {tp} for the next column, but got an error:\n{}",
                                inner_error.select_type, inner_error.err
                            )))
                        } else {
                            // we are not responsible for this error, so pass it up and return Ok(err)
                            *error_with_incomplete_type.lock().unwrap() = Some(inner_error);
                        }
                    }
                }
                Ok(())
            }
        )?;
        let ans = error_with_incomplete_type.lock().unwrap().take();
        Ok(ans)
    }

    /// subgraph def_set_expression
    fn handle_set_expression(&mut self, query_body: &SetExpr) -> Result<(), ConvertionErrorWithPartialSelectType> {
        add_pst!(self.try_push_state("set_expression"))?;

        add_pst!(self.try_push_state("call0_SELECT_query"))?;
        let select_body = &**unwrap_variant!(query_body, SetExpr::Select);
        self.handle_select_query(select_body)?;

        add_pst!(self.try_push_state("EXIT_set_expression"))?;

        Ok(())
    }

    /// subgraph def_SELECT_query
    fn handle_select_query(&mut self, select_body: &Select) -> Result<(), ConvertionErrorWithPartialSelectType> {
        add_pst!(self.try_push_state("SELECT_query"))?;

        add_pst!(self.try_push_state("call0_FROM"))?;
        add_pst!(self.handle_from(&select_body.from))?;

        if let Some(ref selection) = select_body.selection {
            add_pst!(self.try_push_state("call0_WHERE"))?;
            add_pst!(self.handle_where(selection))?;
        }

        add_pst!(self.try_push_state("WHERE_done"))?;

        if select_body.group_by != GroupByExpr::Expressions(vec![]) {
            self.handle_query_after_group_by(select_body, true)?;
        } else {
            let implicit_group_by_checkpoint = self.get_checkpoint(true);
            match self.handle_query_after_group_by(select_body, false) {
                Ok(..) => { },
                Err(err_no_grouping) => {
                    self.restore_checkpoint(&implicit_group_by_checkpoint);
                    match self.handle_query_after_group_by(select_body, true) {
                        Ok(..) => { },
                        Err(err_grouping) => {
                            self.restore_checkpoint_consume(implicit_group_by_checkpoint);
                            return Err(ConvertionErrorWithPartialSelectType::new(
                                format!(
                                    "Error converting query: {select_body}\n\
                                    Tried without implicit groping, got an error: {err_no_grouping}\n\
                                    Tried with implicit groping, got an error: {err_grouping}"
                                ),
                                err_no_grouping.real_n_cols,  // shouldn't differ in both cases
                                if err_grouping.select_type.len() > err_no_grouping.select_type.len() {
                                    err_grouping.select_type
                                } else {
                                    err_no_grouping.select_type
                                },
                            ))
                        }
                    }
                }
            }
        };

        add_pst!(self.try_push_state("EXIT_SELECT_query"))?;

        Ok(())
    }

    fn handle_query_after_group_by(&mut self, select_body: &Select, with_group_by: bool) -> Result<(), ConvertionErrorWithPartialSelectType> {
        if with_group_by {
            add_pst!(self.try_push_state("call0_GROUP_BY"))?;
            add_pst!(self.handle_group_by(&select_body.group_by))?;
        }

        if let Some(ref having) = select_body.having {
            if self.clause_context.top_group_by().is_grouping_active() {
                add_pst!(self.try_push_state("call0_HAVING"))?;
                add_pst!(self.handle_having(having))?;
            } else {
                return Err(ConvertionErrorWithPartialSelectType::with_reason(format!(
                    "HAVING was used but with_grouping was set to false"
                )))
            }
        }

        add_pst!(self.try_push_state("call0_SELECT"))?;
        self.handle_select(&select_body.distinct, &select_body.projection)?;

        Ok(())
    }

    /// subgraph def_ORDER_BY
    fn handle_order_by(&mut self, order_by: &Vec<OrderByExpr>) -> Result<(), ConvertionError> {
        self.try_push_state("ORDER_BY")?;

        let expr_state = if self.clause_context.top_group_by().is_grouping_active() {
            "call84_types"
        } else { "call85_types" };

        for order_by_expr in order_by {
            self.try_push_state("order_by_list")?;
            match &order_by_expr.expr {
                Expr::Identifier(ident) if {
                    let checkpoint = self.get_checkpoint(true);
                    match self.handle_order_by_select_reference(ident) {
                        Ok(..) => true,
                        // TODO: this error is not indicated anywhere
                        Err(_) => {
                            self.restore_checkpoint_consume(checkpoint);
                            false
                        },
                    }
                } => { },
                expr => {
                    self.try_push_states(&["order_by_expr", expr_state])?;
                    self.handle_types(expr, TypeAssertion::None)?;
                },
            }
            self.try_push_state("order_by_expr_done")?;

            self.try_push_states(match &order_by_expr.asc {
                Some(true) => &["order_by_asc", "order_by_order_selected"],
                Some(false) => &["order_by_desc", "order_by_order_selected"],
                None => &["order_by_order_selected"],
            })?;
            self.try_push_states(match &order_by_expr.nulls_first {
                Some(true) => &["order_by_nulls_first", "order_by_nulls_order_selected"],
                Some(false) => &["order_by_nulls_last", "order_by_nulls_order_selected"],
                None => &["order_by_nulls_order_selected"],
            })?;
        }

        self.try_push_state("EXIT_ORDER_BY")?;
        Ok(())
    }

    fn handle_order_by_select_reference(&mut self, ident: &Ident) -> Result<(), ConvertionError> {
        self.try_push_states(&["order_by_select_reference", "order_by_select_reference_by_alias"])?;
        let aliases = &unwrap_variant!(self.state_generator
            .get_named_value::<SelectAccessibleColumnsValue>().unwrap(),
            ValueSetterValue::SelectAccessibleColumns
        ).accessible_columns.iter().collect::<Vec<_>>();
        if aliases.contains(&&ident.clone().into()) {
            self.push_node(PathNode::ORDERBYSelectReference(ident.clone()));
            Ok(())
        } else {
            Err(ConvertionError::new(format!(
                "{} is not in {:?}", ident, aliases
            )))
        }
    }

    /// subgraph def_FROM
    fn handle_from(&mut self, from: &Vec<TableWithJoins>) -> Result<(), ConvertionError> {
        self.try_push_state("FROM")?;
        for table_with_joins in from.iter() {
            self.clause_context.top_from_mut().add_subfrom();
            self.try_push_state("call0_FROM_item")?;
            self.handle_from_item(&table_with_joins.relation)?;
            for join in &table_with_joins.joins {
                self.try_push_state("FROM_join_by")?;
                let (join_type, join_on) = match &join.join_operator {
                    ast::JoinOperator::Inner(join_on) => ("FROM_join_join", join_on),
                    ast::JoinOperator::LeftOuter(join_on) => ("FROM_left_join", join_on),
                    ast::JoinOperator::RightOuter(join_on) => ("FROM_right_join", join_on),
                    ast::JoinOperator::FullOuter(join_on) => ("FROM_full_join", join_on),
                    any => unexpected_expr!(any),
                };
                let ast::JoinConstraint::On(join_on) = join_on else {
                    return Err(ConvertionError::new(format!("Only JoinConstraint::On is accepted. Got: {:?}", join_on)))
                };
                self.try_push_state(join_type)?;
                self.try_push_states(&["FROM_join_to", "call1_FROM_item"])?;
                self.handle_from_item(&join.relation)?;
                self.clause_context.top_from_mut().activate_subfrom();
                self.try_push_states(&["FROM_join_on", "call83_types"])?;
                self.handle_types(join_on, TypeAssertion::GeneratedBy(SubgraphType::Val3))?;
                self.clause_context.top_from_mut().deactivate_subfrom();
            }
            self.try_push_state("FROM_cartesian_product")?;
            self.clause_context.top_from_mut().delete_subfrom();
        }
        self.clause_context.top_from_mut().activate_from();
        self.try_push_state("EXIT_FROM")?;
        Ok(())
    }

    /// subgraph def_FROM_item
    fn handle_from_item(&mut self, relation: &TableFactor) -> Result<(), ConvertionError> {
        self.try_push_state("FROM_item")?;

        match relation {
            TableFactor::Table { name, alias, .. } => {
                let renames = if let Some(alias) = alias.as_ref() {
                    self.try_push_state("FROM_item_alias")?;
                    self.push_node(PathNode::FromAlias(alias.name.clone()));
                    Some(alias.columns.clone())
                } else { self.try_push_state("FROM_item_no_alias")?; None };
                self.try_push_state("FROM_item_table")?;
                self.push_node(PathNode::SelectedTableName(name.clone()));
                if let Some(renames) = renames { self.push_node(PathNode::FromColumnRenames(renames)); }
                if let Err(err) = self.clause_context.add_from_table_by_name(name, alias.clone()) {
                    return Err(ConvertionError::new(format!("{err}")))
                }
            },
            TableFactor::Derived { subquery, alias, .. } => {
                let alias = match alias.as_ref() {
                    Some(alias) => alias,
                    None => return Err(ConvertionError::new(format!("No alias for subquery: {subquery}"))),
                };
                self.try_push_state("FROM_item_alias")?;
                self.push_node(PathNode::FromAlias(alias.name.clone()));
                self.try_push_state("call0_Query")?;
                let column_idents_and_graph_types = self.handle_query(subquery)?;
                self.push_node(PathNode::FromColumnRenames(alias.columns.clone()));
                self.clause_context.top_from_mut().append_query(column_idents_and_graph_types, alias.clone());
            },
            any => unexpected_expr!(any)
        }

        self.try_push_state("EXIT_FROM_item")?;

        Ok(())
    }

    /// subgraph def_WHERE
    fn handle_where(&mut self, selection: &Expr) -> Result<SubgraphType, ConvertionError> {
        self.try_push_state("WHERE")?;
        self.try_push_state("call53_types")?;
        let tp = self.handle_types(selection, TypeAssertion::GeneratedBy(SubgraphType::Val3))?;
        self.try_push_state("EXIT_WHERE")?;
        Ok(tp)
    }

    /// subgraph def_SELECT
    fn handle_select(&mut self, distinct: &Option<Distinct>, projection: &Vec<SelectItem>) -> Result<Vec<SubgraphType>, ConvertionErrorWithPartialSelectType> {
        add_pst!(self.try_push_state("SELECT"))?;
        if distinct.is_some() {
            add_pst!(self.try_push_state("SELECT_DISTINCT"))?;
            self.clause_context.query_mut().set_distinct();
        }
        add_pst!(self.try_push_state("call0_SELECT_item"))?;
        
        let select_item_state = if self.clause_context.top_group_by().is_grouping_active() {
            "call73_types"
        } else {
            "call54_types"
        };

        self.clause_context.query_mut().create_select_type();  // before the first item is called
        let select_type = match self.handle_select_item(projection.as_slice(), select_item_state) {
            Ok(..) => {
                self.clause_context.query_mut().select_type().iter().map(|(_, tp)| tp.clone()).collect_vec()
            },
            Err(err) => {
                return Err(ConvertionErrorWithPartialSelectType {
                    real_n_cols: Some(determine_n_cols_with_clause_context(projection, &self.clause_context)),
                    select_type: self.clause_context.query_mut().select_type().iter().map(|(_, tp)| tp.clone()).collect_vec(),
                    err,
                })
            },
        };

        add_pst!(self.try_push_state("EXIT_SELECT"))?;

        Ok(select_type)
    }

    /// subgraph def_SELECT_item
    fn handle_select_item(
        &mut self, projection: &[SelectItem], select_item_state: &str
    ) -> Result<(), ConvertionError> {

        self.try_push_states(&["SELECT_item", "SELECT_item_grouping_enabled"])?;

        let (select_item, remaining_projection) = projection.split_first().unwrap();
        let (
            first_column_list, remaining_column_types
        ) = unwrap_variant!(
            self.state_generator.get_fn_selected_types_unwrapped(), CallTypes::QueryTypes
        ).split_first();

        match select_item {
            SelectItem::UnnamedExpr(expr) => {
                self.try_push_states(&["SELECT_unnamed_expr", "select_expr", select_item_state])?;
                self.state_generator.set_known_list(first_column_list.clone());
                let tp = self.handle_types(expr, TypeAssertion::GeneratedByOneOf(&first_column_list))?;
                self.try_push_state("select_expr_done")?;
                let alias = QueryProps::extract_alias(&expr);
                self.clause_context.query_mut().select_type_mut().push((alias, tp));
            },
            SelectItem::ExprWithAlias { expr, alias } => {
                self.try_push_state("SELECT_expr_with_alias")?;
                self.push_node(PathNode::SelectAlias(alias.clone()));  // the order is important
                self.try_push_states(&["select_expr", select_item_state])?;
                self.state_generator.set_known_list(first_column_list.clone());
                let tp = self.handle_types(expr, TypeAssertion::GeneratedByOneOf(&first_column_list))?;
                self.try_push_state("select_expr_done")?;
                self.clause_context.query_mut().select_type_mut().push((Some(alias.clone().into()), tp));
            },
            SelectItem::QualifiedWildcard(alias, ..) => {
                self.try_push_states(&["SELECT_tables_eligible_for_wildcard", "SELECT_qualified_wildcard"])?;
                let relation = match alias.0.as_slice() {
                    [ident] => {
                        let identname: IdentName = ident.clone().into();
                        let wildcard_relations = unwrap_variant!(self.state_generator.get_named_value::<WildcardRelationsValue>().unwrap(), ValueSetterValue::WildcardRelations);
                        if !wildcard_relations.relation_levels_selectable_by_qualified_wildcard.iter().any(
                            |level| level.contains(&identname)
                        ) {
                            return Err(ConvertionError::new(format!(
                                "{ident}.* is not available: wildcard_selectable_relations = {:?}",
                                wildcard_relations.relation_levels_selectable_by_qualified_wildcard
                            )))
                        }
                        self.clause_context.get_relation_by_name(&identname)
                    },
                    any => panic!("schema.table alias is not supported: {}", ObjectName(any.to_vec())),
                };

                let new_types = relation.get_wildcard_columns_iter().collect_vec();
                self.clause_context.query_mut().select_type_mut().extend(new_types.into_iter());
                self.push_node(PathNode::QualifiedWildcardSelectedRelation(alias.0.last().unwrap().clone()));
            },
            SelectItem::Wildcard(..) => {
                self.try_push_states(&["SELECT_tables_eligible_for_wildcard", "SELECT_wildcard"])?;
                let new_types = self.clause_context.top_active_from().get_wildcard_columns_iter().collect_vec();
                self.clause_context.query_mut().select_type_mut().extend(new_types.into_iter());
            },
        }

        self.try_push_state("SELECT_item_can_add_more_columns")?;
        if remaining_projection.len() == 0 {
            self.try_push_state("SELECT_item_can_finish")?;
        } else {
            self.try_push_state("call1_SELECT_item")?;
            self.state_generator.set_known_query_type_list(remaining_column_types);
            self.handle_select_item(remaining_projection, select_item_state)?;
        }
        self.try_push_state("EXIT_SELECT_item")?;

        Ok(())
    }

    /// subgraph def_LIMIT
    fn handle_limit(&mut self, expr_opt: &Option<Expr>) -> Result<SubgraphType, ConvertionError> {
        self.try_push_state("LIMIT")?;

        let limit_type = if let Some(expr) = expr_opt {
            match self.state_generator.get_fn_modifiers() {
                CallModifiers::StaticList(list) if list.contains(&SmolStr::new("single row")) => {
                    if expr != &Expr::Value(Value::Number("1".to_string(), false)) {
                        return Err(ConvertionError::new(format!(
                            "Expected query to have \"LIMIT 1\" because of \'single row\', got {:#?}", expr
                        )))
                    }
                    self.try_push_state("single_row_true")?;
                    SubgraphType::Integer
                },
                _ => {
                    self.try_push_states(&["limit_num", "call52_types"])?;
                    self.handle_types(expr, TypeAssertion::GeneratedByOneOf(&[SubgraphType::Numeric, SubgraphType::Integer, SubgraphType::BigInt]))?
                },
            }
        } else {
            self.try_push_states(&["query_can_skip_limit_set_val", "query_can_skip_limit"])?;
            SubgraphType::Undetermined
        };

        self.try_push_state("EXIT_LIMIT")?;

        Ok(limit_type)
    }

    /// subgraph def_VAL_3
    fn handle_val_3(&mut self, expr: &Expr) -> Result<SubgraphType, ConvertionError> {
        self.try_push_state("VAL_3")?;

        match expr {
            x @ (Expr::IsNull(expr) | Expr::IsNotNull(expr)) => {
                self.try_push_state("IsNull")?;
                if matches!(x, Expr::IsNotNull(..)) { self.try_push_state("IsNull_not")?; }
                self.try_push_state("call55_types")?;
                self.handle_types(expr, TypeAssertion::None)?;
            },
            x @ (Expr::IsDistinctFrom(expr_1, expr_2) | Expr::IsNotDistinctFrom(expr_1, expr_2)) =>  {
                self.try_push_state("IsDistinctFrom")?;
                if matches!(x, Expr::IsNotDistinctFrom(..)) { self.try_push_state("IsDistinctNOT")?; }
                self.try_push_state("call0_types_type")?;
                self.handle_types_type_and_try(None, TypeAssertion::None, |_self, returned_type| {
                    _self.state_generator.set_compatible_list(returned_type.get_compat_types());
                    _self.try_push_state("call56_types")?;
                    _self.handle_types(expr_1, TypeAssertion::CompatibleWith(returned_type.clone()))?;
                    _self.try_push_state("call21_types")?;
                    _self.handle_types(expr_2, TypeAssertion::CompatibleWith(returned_type.clone()))?;
                    Ok(())
                })?;
            },
            Expr::Exists { subquery, negated } => {
                self.try_push_state("Exists")?;
                if *negated { self.try_push_state("Exists_not")?; }
                self.try_push_state("call2_Query")?;
                self.handle_query(subquery)?;
            },
            Expr::InList { expr, list, negated } => {
                self.try_push_state("InList")?;
                if *negated { self.try_push_state("InListNot")?; }
                self.try_push_state("call3_types_type")?;
                self.handle_types_type_and_try(None, TypeAssertion::None, |_self, returned_type| {
                    _self.state_generator.set_compatible_list(returned_type.get_compat_types());
                    _self.try_push_state("call57_types")?;
                    _self.handle_types(expr, TypeAssertion::CompatibleWith(returned_type.clone()))?;
                    _self.try_push_state("call1_list_expr")?;
                    _self.handle_list_expr(list)?;
                    Ok(())
                })?;
            },
            Expr::InSubquery { expr, subquery, negated } => {
                self.try_push_state("InSubquery")?;
                if *negated { self.try_push_state("InSubqueryNot")?; }
                self.try_push_state("call4_types_type")?;
                self.handle_types_type_and_try(None, TypeAssertion::None, |_self, returned_type| {
                    _self.state_generator.set_compatible_list(returned_type.get_compat_types());
                    _self.try_push_state("call58_types")?;
                    _self.handle_types(expr, TypeAssertion::CompatibleWith(returned_type.clone()))?;
                    _self.state_generator.set_compatible_query_type_list(QueryTypes::ColumnTypeLists {
                        column_type_lists: vec![returned_type.get_compat_types()]  // single column
                    });
                    _self.try_push_state("call3_Query")?;
                    _self.handle_query(subquery)?;
                    Ok(())
                })?;
            },
            Expr::Between { expr, negated, low, high } => {
                if *negated { panic!("Negated betweens are not supported"); }
                self.try_push_states(&["Between", "call5_types_type"])?;
                self.handle_types_type_and_try(None, TypeAssertion::None, |_self, returned_type| {
                    _self.state_generator.set_compatible_list(returned_type.get_compat_types());
                    _self.try_push_state("call59_types")?;
                    _self.handle_types(expr, TypeAssertion::CompatibleWith(returned_type.clone()))?;
                    _self.try_push_states(&["BetweenBetween", "call22_types"])?;
                    _self.handle_types(low, TypeAssertion::CompatibleWith(returned_type.clone()))?;
                    _self.try_push_states(&["BetweenBetweenAnd", "call23_types"])?;
                    _self.handle_types(high, TypeAssertion::CompatibleWith(returned_type))?;
                    Ok(())
                })?;
            },
            Expr::AnyOp { left, compare_op, right } |
            Expr::AllOp { left, compare_op, right } => {
                self.try_push_states(&["AnyAll", "AnyAllSelectOp"])?;
                self.try_push_state(match compare_op {
                    BinaryOperator::Eq => "AnyAllEqual",
                    BinaryOperator::NotEq => "AnyAllUnEqual",
                    BinaryOperator::Lt => "AnyAllLess",
                    BinaryOperator::LtEq => "AnyAllLessEqual",
                    BinaryOperator::Gt => "AnyAllGreater",
                    BinaryOperator::GtEq => "AnyAllGreaterEqual",
                    any => unexpected_expr!(any),
                })?;
                self.try_push_state("call2_types_type")?;
                self.handle_types_type_and_try(None, TypeAssertion::None, |_self, returned_type| {
                    _self.state_generator.set_compatible_list(returned_type.get_compat_types());
                    _self.try_push_state("call61_types")?;
                    _self.handle_types(left, TypeAssertion::CompatibleWith(returned_type.clone()))?;

                    _self.try_push_state("AnyAllAnyAll")?;
                    _self.try_push_state(match expr {
                        Expr::AllOp { .. } => "AnyAllAnyAllAll",
                        Expr::AnyOp { .. } => "AnyAllAnyAllAny",
                        any => unexpected_expr!(any),
                    })?;
                    
                    _self.try_push_state("AnyAllSelectIter")?;
                    match &**right {
                        Expr::Subquery(query) => {
                            _self.try_push_state("call4_Query")?;
                            _self.state_generator.set_compatible_query_type_list(QueryTypes::ColumnTypeLists {
                                column_type_lists: vec![returned_type.get_compat_types()]  // single column
                            });
                            _self.handle_query(query)?;
                        },
                        any => unexpected_expr!(any),
                    }
                    
                    Ok(())
                })?;
            }
            Expr::BinaryOp { left, op, right } => {
                match op {
                    BinaryOperator::And |
                    BinaryOperator::Or |
                    BinaryOperator::Xor => {
                        self.try_push_state("BinaryBooleanOpV3")?;
                        self.try_push_state(match op {
                            BinaryOperator::And => "BinaryBooleanOpV3AND",
                            BinaryOperator::Or => "BinaryBooleanOpV3OR",
                            BinaryOperator::Xor => "BinaryBooleanOpV3XOR",
                            any => unexpected_expr!(any),
                        })?;
                        self.try_push_state("call27_types")?;
                        self.handle_types(left, TypeAssertion::GeneratedBy(SubgraphType::Val3))?;
                        self.try_push_state("call28_types")?;
                        self.handle_types(right, TypeAssertion::GeneratedBy(SubgraphType::Val3))?;
                    },
                    BinaryOperator::Eq |
                    BinaryOperator::NotEq |
                    BinaryOperator::Lt |
                    BinaryOperator::LtEq |
                    BinaryOperator::Gt |
                    BinaryOperator::GtEq => {
                        self.try_push_state("BinaryComp")?;
                        self.try_push_state(match op {
                            BinaryOperator::Eq => "BinaryCompEqual",
                            BinaryOperator::NotEq => "BinaryCompUnEqual",
                            BinaryOperator::Lt => "BinaryCompLess",
                            BinaryOperator::LtEq => "BinaryCompLessEqual",
                            BinaryOperator::Gt => "BinaryCompGreater",
                            BinaryOperator::GtEq => "BinaryCompGreaterEqual",
                            any => unexpected_expr!(any),
                        })?;
                        self.try_push_state("call1_types_type")?;
                        self.handle_types_type_and_try(None, TypeAssertion::None, |_self, returned_type| {
                            _self.state_generator.set_compatible_list(returned_type.get_compat_types());
                            _self.try_push_state("call60_types")?;
                            _self.handle_types(left, TypeAssertion::CompatibleWith(returned_type.clone()))?;
                            _self.try_push_state("call24_types")?;
                            _self.handle_types(right, TypeAssertion::CompatibleWith(returned_type))?;
                            Ok(())
                        })?;
                    },
                    any => unexpected_expr!(any),
                }
            },
            Expr::Like { negated, expr, pattern, .. } => {
                self.try_push_state("BinaryStringLike")?;
                if *negated { self.try_push_state("BinaryStringLikeNot")?; }
                self.try_push_state("call25_types")?;
                self.handle_types(expr, TypeAssertion::GeneratedBy(SubgraphType::Text))?;
                self.try_push_state("call26_types")?;
                self.handle_types(pattern, TypeAssertion::GeneratedBy(SubgraphType::Text))?;
            }
            Expr::UnaryOp { op, expr } if *op == UnaryOperator::Not => {
                self.try_push_states(&["UnaryNot_VAL_3", "call30_types"])?;
                self.handle_types(
                    expr, TypeAssertion::GeneratedBy(SubgraphType::Val3)
                )?;
            }
            any => unexpected_expr!(any),
        }

        self.try_push_state("EXIT_VAL_3")?;

        Ok(SubgraphType::Val3)
    }

    /// subgraph def_numeric
    fn handle_number(&mut self, expr: &Expr) -> Result<SubgraphType, ConvertionError> {
        self.try_push_state("number")?;
        let requested_number_type = self.assert_single_type_argument();
        let number_type = match expr {
            Expr::BinaryOp { left, op, right } => {
                self.try_push_state("BinaryNumberOp")?;
                self.try_push_state(match op {
                    BinaryOperator::BitwiseAnd => "binary_number_bin_and",
                    BinaryOperator::BitwiseOr => "binary_number_bin_or",
                    BinaryOperator::PGBitwiseXor => "binary_number_bin_xor",
                    BinaryOperator::PGExp => "binary_number_exp",
                    BinaryOperator::Divide => "binary_number_div",
                    BinaryOperator::Minus => "binary_number_minus",
                    BinaryOperator::Multiply => "binary_number_mul",
                    BinaryOperator::Plus => "binary_number_plus",
                    any => unexpected_expr!(any),
                })?;
                self.state_generator.set_compatible_list(requested_number_type.get_compat_types());
                self.try_push_state("call47_types")?;
                self.handle_types(left, TypeAssertion::CompatibleWith(requested_number_type.clone()))?;
                self.try_push_state("call48_types")?;
                self.handle_types(right, TypeAssertion::CompatibleWith(requested_number_type.clone()))?;
                requested_number_type
            },
            Expr::UnaryOp { op, expr } => {
                self.try_push_state("UnaryNumberOp")?;
                self.try_push_state(match op {
                    UnaryOperator::PGAbs => "unary_number_abs",
                    UnaryOperator::PGBitwiseNot => "unary_number_bin_not",
                    UnaryOperator::PGCubeRoot => "unary_number_cub_root",
                    UnaryOperator::Minus => "unary_number_minus",
                    UnaryOperator::Plus => "unary_number_plus",
                    UnaryOperator::PGSquareRoot => "unary_number_sq_root",
                    any => unexpected_expr!(any),
                })?;
                if *op == UnaryOperator::Minus {
                    self.try_push_state("call89_types")?;
                } else {
                    self.try_push_state("call1_types")?;
                }
                self.state_generator.set_compatible_list(requested_number_type.get_compat_types());
                self.handle_types(expr, TypeAssertion::CompatibleWith(requested_number_type.clone()))?;
                requested_number_type
            },
            Expr::Position { expr, r#in } => {
                self.try_push_states(&["number_string_position", "call2_types"])?;
                self.handle_types(expr, TypeAssertion::GeneratedBy(SubgraphType::Text))?;
                self.try_push_states(&["string_position_in", "call3_types"])?;
                self.handle_types(r#in, TypeAssertion::GeneratedBy(SubgraphType::Text))?;
                SubgraphType::Integer
            },
            Expr::Extract { field, expr } => {
                self.try_push_states(&["number_extract_field_from_date", "call0_select_datetime_field"])?;
                self.handle_select_datetime_field(field)?;
                self.try_push_state("call97_types")?;
                self.state_generator.set_compatible_list([
                    SubgraphType::Interval.get_compat_types(),
                    SubgraphType::Timestamp.get_compat_types(),
                ].concat());
                self.handle_types(expr, TypeAssertion::CompatibleWithOneOf(&[SubgraphType::Interval, SubgraphType::Timestamp]))?;
                SubgraphType::Numeric
            },
            any => unexpected_expr!(any),
        };
        self.try_push_state("EXIT_number")?;
        Ok(number_type)
    }

    /// subgraph def_text
    fn handle_text(&mut self, expr: &Expr) -> Result<SubgraphType, ConvertionError> {
        self.try_push_state("text")?;
        match expr {
            Expr::Value(Value::SingleQuotedString(_)) => self.try_push_state("text_literal")?,
            Expr::Trim { expr, trim_where, trim_what , trim_characters } => {
                if trim_characters.is_some() {
                    return Err(ConvertionError::new(format!("trim_characters is not supppoted. Got: {expr}")));
                }
                self.try_push_states(&["text_trim", "call6_types"])?;
                self.handle_types(expr, TypeAssertion::GeneratedBy(SubgraphType::Text))?;

                match (trim_where, trim_what) {
                    (Some(trim_where), Some(trim_what)) => {
                        self.try_push_state(match trim_where {
                            TrimWhereField::Both => "BOTH",
                            TrimWhereField::Leading => "LEADING",
                            TrimWhereField::Trailing => "TRAILING",
                        })?;
                        self.try_push_state("call5_types")?;
                        self.handle_types(trim_what, TypeAssertion::GeneratedBy(SubgraphType::Text))?;
                    },
                    (None, None) => {},
                    any => unexpected_expr!(any),
                };
                
                self.try_push_state("text_trim_done")?;
            },
            Expr::BinaryOp { left, op, right } if *op == BinaryOperator::StringConcat => {
                self.try_push_states(&["text_concat", "call7_types"])?;
                self.handle_types(left, TypeAssertion::GeneratedBy(SubgraphType::Text))?;
                self.try_push_states(&["text_concat_concat", "call8_types"])?;
                self.handle_types(right, TypeAssertion::GeneratedBy(SubgraphType::Text))?;
            },
            Expr::Substring { expr, substring_from, substring_for, special: _ } => {
                self.try_push_states(&["text_substring", "call9_types"])?;
                self.handle_types(expr, TypeAssertion::GeneratedBy(SubgraphType::Text))?;
                if let Some(substring_from) = substring_from {
                    self.try_push_states(&["text_substring_from", "call10_types"])?;
                    self.handle_types(substring_from, TypeAssertion::GeneratedBy(SubgraphType::Integer))?;
                }
                if let Some(substring_for) = substring_for {
                    self.try_push_states(&["text_substring_for", "call11_types"])?;
                    self.handle_types(substring_for, TypeAssertion::GeneratedBy(SubgraphType::Integer))?;
                }
                self.try_push_state("text_substring_end")?;
            },
            any => unexpected_expr!(any),
        }
        self.try_push_state("EXIT_text")?;
        Ok(SubgraphType::Text)
    }

    /// subgraph def_date
    fn handle_date(&mut self, expr: &Expr) -> Result<SubgraphType, ConvertionError> {
        self.try_push_state("date")?;

        match expr {
            Expr::BinaryOp { left, op, right } => {
                self.try_push_states(&["date_binary", "date_add_subtract"])?;
                self.try_push_state(match op {
                    BinaryOperator::Plus => "date_add_subtract_plus",
                    BinaryOperator::Minus => "date_add_subtract_minus",
                    any => unexpected_expr!(any),
                })?;

                let mut err_str: String = "".to_string();
                let checkpoint = self.get_checkpoint(true);
                for swap_arguments in [false, true] {
                    if swap_arguments {
                        self.try_push_state("date_swap_arguments")?;
                    }

                    let (date, integer) = if swap_arguments {
                        (right, left)
                    } else { (left, right) };

                    self.try_push_state("call86_types")?;
                    match self.handle_types(
                        date, TypeAssertion::GeneratedBy(SubgraphType::Date)
                    ) {
                        Ok(..) => { },
                        Err(err) => {
                            self.restore_checkpoint(&checkpoint);
                            err_str += format!("Tried swap_arguments = {swap_arguments}, got: {err}\n").as_str();
                            continue;
                        },
                    };

                    self.try_push_state("call88_types")?;
                    match self.handle_types(
                        integer, TypeAssertion::GeneratedBy(SubgraphType::Integer)
                    ) {
                        Ok(..) => { },
                        Err(err) => {
                            self.restore_checkpoint(&checkpoint);
                            err_str += format!("Tried swap_arguments = {swap_arguments}, got: {err}\n").as_str();
                            continue;
                        },
                    };

                    err_str = "".to_string();
                    break;
                }

                if err_str != "" {
                    return Err(ConvertionError::new(err_str))
                }
            },
            any => unexpected_expr!(any),
        }

        self.try_push_state("EXIT_date")?;
        Ok(SubgraphType::Date)
    }

    /// subgraph def_timestamp
    fn handle_timestamp(&mut self, expr: &Expr) -> Result<SubgraphType, ConvertionError> {
        self.try_push_state("timestamp")?;

        match expr {
            Expr::BinaryOp { left, op, right } => {
                self.try_push_states(&["timestamp_binary", "timestamp_add_subtract"])?;
                self.try_push_state(match op {
                    BinaryOperator::Plus => "timestamp_add_subtract_plus",
                    BinaryOperator::Minus => "timestamp_add_subtract_minus",
                    any => unexpected_expr!(any),
                })?;

                let mut err_str: String = "".to_string();
                let checkpoint = self.get_checkpoint(true);
                for swap_arguments in [false, true] {
                    if swap_arguments {
                        self.try_push_state("timestamp_swap_arguments")?;
                    }

                    let (timestamp, interval) = if swap_arguments {
                        (right, left)
                    } else { (left, right) };

                    self.try_push_state("call94_types")?;
                    match self.handle_types(
                        timestamp, TypeAssertion::GeneratedBy(SubgraphType::Date)
                    ) {
                        Ok(..) => { },
                        Err(err) => {
                            self.restore_checkpoint(&checkpoint);
                            err_str += format!("Tried swap_arguments = {swap_arguments}, got: {err}\n").as_str();
                            continue;
                        },
                    };

                    self.try_push_state("call95_types")?;
                    match self.handle_types(
                        interval, TypeAssertion::GeneratedBy(SubgraphType::Interval)
                    ) {
                        Ok(..) => { },
                        Err(err) => {
                            self.restore_checkpoint(&checkpoint);
                            err_str += format!("Tried swap_arguments = {swap_arguments}, got: {err}\n").as_str();
                            continue;
                        },
                    };

                    err_str = "".to_string();
                    break;
                }

                if err_str != "" {
                    return Err(ConvertionError::new(err_str))
                }
            },
            any => unexpected_expr!(any),
        }

        self.try_push_state("EXIT_timestamp")?;
        Ok(SubgraphType::Timestamp)
    }

    /// subgraph def_select_datetime_field
    fn handle_select_datetime_field(&mut self, field: &DateTimeField) -> Result<(), ConvertionError> {
        self.try_push_state("select_datetime_field")?;
        self.try_push_state(match field {
            DateTimeField::Microseconds => "select_datetime_field_microseconds",
            DateTimeField::Milliseconds => "select_datetime_field_milliseconds",
            DateTimeField::Second => "select_datetime_field_second",
            DateTimeField::Minute => "select_datetime_field_minute",
            DateTimeField::Hour => "select_datetime_field_hour",
            DateTimeField::Day => "select_datetime_field_day",
            DateTimeField::Isodow => "select_datetime_field_isodow",
            DateTimeField::Week => "select_datetime_field_week",
            DateTimeField::Month => "select_datetime_field_month",
            DateTimeField::Quarter => "select_datetime_field_quarter",
            DateTimeField::Year => "select_datetime_field_year",
            DateTimeField::Isoyear => "select_datetime_field_isoyear",
            DateTimeField::Decade => "select_datetime_field_decade",
            DateTimeField::Century => "select_datetime_field_century",
            DateTimeField::Millennium => "select_datetime_field_millennium",
            any => unexpected_expr!(any),
        })?;
        self.try_push_state("EXIT_select_datetime_field")?;
        Ok(())
    }

    /// subgraph def_interval
    fn handle_interval(&mut self, expr: &Expr) -> Result<SubgraphType, ConvertionError> {
        self.try_push_state("interval")?;

        match expr {
            Expr::BinaryOp {
                left, op, right
            } => {
                self.try_push_states(&["interval_binary", "interval_add_subtract"])?;
                self.try_push_state(match op {
                    BinaryOperator::Plus => "interval_add_subtract_plus",
                    BinaryOperator::Minus => "interval_add_subtract_minus",
                    any => unexpected_expr!(any),
                })?;

                let checkpoint = self.get_checkpoint(true);
                self.try_push_state("call91_types")?;
                match self.handle_types(left, TypeAssertion::GeneratedByOneOf(&[SubgraphType::Interval])) {
                    Ok(_) => {
                        self.try_push_state("call92_types")?;
                        self.handle_types(right, TypeAssertion::GeneratedByOneOf(&[SubgraphType::Interval]))?;
                    },
                    Err(err) => {
                        self.restore_checkpoint_consume(checkpoint);
                        if *op == BinaryOperator::Plus {
                            return Err(err)  // only timestamp - timestamp
                        }
                        self.try_push_state("call98_types")?;
                        self.handle_types(left, TypeAssertion::GeneratedByOneOf(&[SubgraphType::Timestamp]))?;
                        self.try_push_state("call99_types")?;
                        self.handle_types(right, TypeAssertion::GeneratedByOneOf(&[SubgraphType::Timestamp]))?;
                    },
                }
            },
            Expr::UnaryOp {
                op, expr: interval
            } if *op == UnaryOperator::Minus => {
                self.try_push_states(&["interval_unary_minus", "call93_types"])?;
                self.handle_types(interval, TypeAssertion::GeneratedBy(SubgraphType::Interval))?; 
            },
            any => unexpected_expr!(any),
        }

        self.try_push_state("EXIT_interval")?;
        Ok(SubgraphType::Interval)
    }

    /// subgraph def_types
    fn handle_types(&mut self, expr: &Expr, type_assertion: TypeAssertion) -> Result<SubgraphType, ConvertionError> {
        self.try_push_state("types")?;
        let selected_types = unwrap_variant!(self.state_generator.get_fn_selected_types_unwrapped(), CallTypes::TypeList);
        let selected_types = SubgraphType::sort_by_compatibility(selected_types);
        let mut selected_types_iter = selected_types.iter();
        self.handle_types_continue_after(expr, type_assertion, &selected_types, &mut selected_types_iter)
    }

    fn _handle_types_and_try<F: Fn(&mut PathGenerator, SubgraphType) -> Result<(), ConvertionError>>(&mut self, expr: &Expr, type_assertion: TypeAssertion, and_try: F) -> Result<SubgraphType, ConvertionError> {
        // Use like:
        // let returned_type = self.handle_types_and_try(expr, TypeAssertion::None, |_self, returned_type| {
        //     _self.something(...);
        //     Ok(())
        // })?;
        self.try_push_state("types")?;
        let selected_types = unwrap_variant!(self.state_generator.get_fn_selected_types_unwrapped(), CallTypes::TypeList);
        let selected_types = SubgraphType::sort_by_compatibility(selected_types);
        let mut selected_types_iter = selected_types.iter();
        let mut err_msg = "".to_string();
        // can't use cutoff because restoration may be on a level that's less deep
        let checkpoint = self.get_checkpoint(false);
        loop {
            let tp = match self.handle_types_continue_after(expr, type_assertion.clone(), &selected_types, &mut selected_types_iter) {
                Ok(tp) => tp,
                Err(err) => break Err(ConvertionError::new(err_msg + format!("Finally: {err}\n").as_str())),
            };
            match and_try(self, tp.clone()) {
                Ok(..) => break Ok(tp),
                Err(err) => {
                    err_msg += format!("Tried with {tp} returned by types, but got: {err}\n").as_str();
                    self.restore_checkpoint(&checkpoint);
                },
            };
        }
    }

    /// only use in handle_types/handle_types_and_try
    fn handle_types_continue_after<'a>(
        &mut self, expr: &Expr, type_assertion: TypeAssertion,
        selected_types: &Vec<SubgraphType>, selected_types_iter: &mut impl Iterator<Item = &'a SubgraphType>
    ) -> Result<SubgraphType, ConvertionError> {
        let tp = self.handle_types_expr(expr, selected_types, selected_types_iter)?;
        self.try_push_state("EXIT_types")?;
        type_assertion.check(&tp, expr);
        return Ok(tp)
    }

    fn handle_types_expr<'a>(&mut self, expr: &Expr, selected_types: &Vec<SubgraphType>, selected_types_iter: &mut impl Iterator<Item = &'a SubgraphType>) -> Result<SubgraphType, ConvertionError> {
        let mut err_str = "".to_string();
        let types_before_state_selection = self.get_checkpoint(true);

        // try out different allowed types to find the actual one (or not)
        let tp = loop {
            let subgraph_type = match selected_types_iter.next() {
                Some(subgraph_type) => subgraph_type,
                None => return Err(ConvertionError::new(
                    format!(
                        "Types didn't find a suitable type for expression, among:\n{:#?}\n\
                        Expression:\n{:?}\nPrinted: {expr}\nErrors:\n{}\n",
                        selected_types, expr, err_str.get_indentated_string()
                    )
                )),
            };

            match self.try_push_state(match subgraph_type {
                SubgraphType::BigInt => "types_select_type_bigint",
                SubgraphType::Integer => "types_select_type_integer",
                SubgraphType::Numeric => "types_select_type_numeric",
                SubgraphType::Val3 => "types_select_type_3vl",
                SubgraphType::Text => "types_select_type_text",
                SubgraphType::Date => "types_select_type_date",
                SubgraphType::Timestamp => "types_select_type_timestamp",
                SubgraphType::Interval => "types_select_type_interval",
                any => unexpected_subgraph_type!(any),
            }) {
                Ok(_) => {},
                Err(err) => {
                    err_str += format!("{subgraph_type} => is not allowed (err: {err})\n").as_str();
                    self.restore_checkpoint(&types_before_state_selection);
                    continue;
                },
            };
            let allowed_type_list = SubgraphType::filter_by_selected(&selected_types, subgraph_type.clone());

            self.state_generator.set_known_list(allowed_type_list);

            self.try_push_state("call0_types_value")?;
            match self.handle_types_value(expr) {
                Ok(tp) => {
                    if tp.is_same_or_more_determined_or_undetermined(subgraph_type) {
                        break tp
                    } else {
                        panic!("types_value did not return the requested type. Requested: {subgraph_type} Got: {}", tp)
                    }
                },
                Err(err) => {
                    err_str += format!("{subgraph_type} =>\n{}\n", err.get_indentated_string()).as_str();
                    self.restore_checkpoint(&types_before_state_selection)
                },
            };
        };

        Ok(tp)
    }

    /// subgraph def_types_type
    fn handle_types_type_and_try<F: Fn(&mut PathGenerator, SubgraphType) -> Result<(), ConvertionError>>(
        &mut self, expr_str: Option<String>, type_assertion: TypeAssertion, and_try: F
    ) -> Result<SubgraphType, ConvertionError> {
        // Use like:
        // let returned_type = self.handle_types_type_and_try(None, TypeAssertion::None, |_self, returned_type| {
        //     _self.something(...);
        //     Ok(())
        // })?;
        self.try_push_state("types_type")?;
        let selected_types = unwrap_variant!(self.state_generator.get_fn_selected_types_unwrapped(), CallTypes::TypeList);
        let selected_types = SubgraphType::sort_by_compatibility(selected_types);
        let mut selected_types_iter = selected_types.iter();
        let mut err_msg = "".to_string();
        // can't use cutoff because restoration may be on a level that's less deep
        let checkpoint = self.get_checkpoint(false);
        loop {
            let tp = match self.handle_types_type_continue_after(expr_str.clone(), &mut selected_types_iter, &selected_types) {
                Ok(tp) => tp,
                Err(err) => break Err(ConvertionError::new(format!("{err}\n{}", err_msg.get_indentated_string()))),
            };
            type_assertion.check_type(&tp);
            match and_try(self, tp.clone()) {
                Ok(..) => break Ok(tp.clone()),
                Err(err) => {
                    err_msg += format!("Tried with {tp}, but got: {}\n", err.get_indentated_string()).as_str();
                    self.restore_checkpoint(&checkpoint);
                },
            };
        }
    }

    /// only for use in handle_types_type_and_try
    fn handle_types_type_continue_after<'a>(
        &mut self, expr_str: Option<String>, selected_types_iter: &mut impl Iterator<Item = &'a SubgraphType>, selected_types: &Vec<SubgraphType>
    ) -> Result<&'a SubgraphType, ConvertionError> {
        let subgraph_type = match selected_types_iter.next() {
            Some(subgraph_type) => subgraph_type,
            None => return Err(ConvertionError::new(format!(
                "Types type didn't find a suitable type for expression, among:\n{:#?}{}", selected_types, if let Some(expr_str) = expr_str {
                    format!("\nExpr: {expr_str}")
                } else { format!("\nExpr: not provided by caller") }
            ))),
        };

        self.try_push_state(match subgraph_type {
            SubgraphType::BigInt => "types_type_bigint",
            SubgraphType::Integer => "types_type_integer",
            SubgraphType::Numeric => "types_type_numeric",
            SubgraphType::Val3 => "types_type_3vl",
            SubgraphType::Text => "types_type_text",
            SubgraphType::Date => "types_type_date",
            SubgraphType::Interval => "types_type_interval",
            SubgraphType::Timestamp => "types_type_timestamp",
            any => unexpected_subgraph_type!(any),
        })?;

        self.try_push_state("EXIT_types_type")?;

        Ok(subgraph_type)
    }

    /// subgraph def_types_value
    /// the boolean value indicates whether the expression may be multi-type
    /// and pther types are worth exploring
    fn handle_types_value(&mut self, expr: &Expr) -> Result<SubgraphType, ConvertionError> {
        self.try_push_state("types_value")?;
        let selected_types = unwrap_variant!(self.state_generator.get_fn_selected_types_unwrapped(), CallTypes::TypeList);
        self.state_generator.set_known_list(selected_types.clone());

        let mut err_str = "".to_string();

        let checkpoint = self.get_checkpoint(true);

        let (tp_res, subgraph_key) = match expr {
            Expr::Value(Value::Null) => {
                self.try_push_state("types_value_null")?;
                (Ok(SubgraphType::Undetermined), "null")
            },
            Expr::Nested(expr) => {
                self.try_push_states(&["types_value_nested", "call1_types_value"])?;
                (self.handle_types_value(expr), "nested")
            },
            Expr::Cast { expr, data_type, format } if **expr == Expr::Value(Value::Null) => {
                if format.is_some() {
                    return Err(ConvertionError::new(format!("cast format is not supported. Got: {expr}")));
                }
                let null_type = SubgraphType::from_data_type(data_type);
                (if selected_types.contains(&null_type) {
                    self.try_push_state("types_value_typed_null")?;
                    Ok(null_type)
                } else {
                    Err(ConvertionError::new(format!("Wrong null types: {:?} for expr: {expr}::{data_type}", selected_types)))
                }, "typed null")
            },
            Expr::Case { .. } => (self.handle_types_value_expr_case(expr), "CASE"),
            Expr::Function(function) => (self.handle_types_value_expr_aggr_function(function), "Aggregate function"),
            Expr::Subquery(subquery) => (self.handle_types_value_subquery(subquery, &selected_types), "Subquery"),
            expr => {
                let handlers: Vec<(Box<dyn FnMut(&mut PathGenerator) -> Result<SubgraphType, ConvertionError>>, &str)> = vec![
                    (Box::new(|_self| _self.handle_types_value_column_expr(expr)), "column identifier"),
                    (Box::new(|_self| _self.handle_types_value_literals(expr)), "literals"),
                ];
                handlers.into_iter().find_map(|(mut handler, subgraph_key)| match handler(self) {
                    Ok(tp) => Some((Ok(tp), subgraph_key)),
                    Err(err) => {
                        err_str += format!("Tried the {subgraph_key} handler, got: {err}\n").as_str();
                        self.restore_checkpoint(&checkpoint);
                        None
                    },
                }).unwrap_or_else(|| (self.handle_types_value_expr_formula(expr), "type expression"))
            },
        };
        
        match tp_res {
            Ok(tp) => {
                self.try_push_state("EXIT_types_value")?;
                Ok(tp)
            },
            Err(err) => {
                self.restore_checkpoint_consume(checkpoint);
                err_str += format!("Tried the {subgraph_key} handler, got: {err}\n").as_str();
                Err(ConvertionError::new(err_str))
            },
        }
    }

    fn handle_types_value_expr_case(&mut self, expr: &Expr) -> Result<SubgraphType, ConvertionError> {
        self.try_push_state("call0_case")?;
        // change to explorable if we use this feature
        self.handle_case(expr)
    }

    fn handle_types_value_expr_aggr_function(&mut self, function: &ast::Function) -> Result<SubgraphType, ConvertionError> {
        self.try_push_state("call0_aggregate_function")?;
        self.handle_aggregate_function(function)
    }

    fn handle_types_value_subquery(&mut self, subquery: &Box<Query>, selected_types: &Vec<SubgraphType>) -> Result<SubgraphType, ConvertionError> {
        self.try_push_state("call1_Query")?;
        self.state_generator.set_known_query_type_list(QueryTypes::ColumnTypeLists {
            column_type_lists: vec![selected_types.clone()]  // single column
        });
        let col_type_list = self.handle_query(subquery)?;
        if let [(.., query_subgraph_type)] = col_type_list.as_slice() {
            // change to explorable if we use this feature
            Ok(query_subgraph_type.clone())
        } else {
            panic!("Query did not return a single type. Got: {:?}\nQuery: {subquery}", col_type_list);
        }
    }

    fn handle_types_value_column_expr(&mut self, expr: &Expr) -> Result<SubgraphType, ConvertionError> {
        self.try_push_states(&["column_type_available", "call0_column_spec"])?;
        self.handle_column_spec(expr)
    }

    fn handle_types_value_literals(&mut self, expr: &Expr) -> Result<SubgraphType, ConvertionError> {
        self.try_push_state("call0_literals")?;
        // change to explorable if we use this feature
        self.handle_literals(expr)
    }

    fn handle_types_value_expr_formula(&mut self, expr: &Expr) -> Result<SubgraphType, ConvertionError> {
        self.try_push_state("call0_formulas")?;
        self.handle_formulas(expr)
    }

    /// subgraph def_formulas
    fn handle_formulas(&mut self, expr: &Expr) -> Result<SubgraphType, ConvertionError> {
        self.try_push_state("formulas")?;
        self.assert_single_type_argument();
        let selected_types = unwrap_variant!(self.state_generator.get_fn_selected_types_unwrapped(), CallTypes::TypeList);
        let [subgraph_type] = selected_types.as_slice() else { unreachable!() };
        let subgraph_type = match subgraph_type {
            SubgraphType::BigInt => {
                self.try_push_state("call2_number")?;
                self.handle_number(expr)
            },
            SubgraphType::Integer => {
                self.try_push_state("call1_number")?;
                self.handle_number(expr)
            },
            SubgraphType::Numeric => {
                self.try_push_state("call0_number")?;
                self.handle_number(expr)
            },
            SubgraphType::Val3 => {
                self.try_push_state("call1_VAL_3")?;
                self.handle_val_3(expr)
            },
            SubgraphType::Text => {
                self.try_push_state("call0_text")?;
                self.handle_text(expr)
            },
            SubgraphType::Date => {
                self.try_push_state("call0_date")?;
                self.handle_date(expr)
            },
            SubgraphType::Timestamp => {
                self.try_push_state("call0_timestamp")?;
                self.handle_timestamp(expr)
            },
            SubgraphType::Interval => {
                self.try_push_state("call0_interval")?;
                self.handle_interval(expr)
            },
            any => unexpected_subgraph_type!(any),
        }?;
        self.try_push_state("EXIT_formulas")?;
        Ok(subgraph_type)
    }

    /// subgraph def_literals
    fn handle_literals(&mut self, expr: &Expr) -> Result<SubgraphType, ConvertionError> {
        self.try_push_state("literals")?;

        let tp = match expr {
            Expr::Value(Value::Boolean(true)) => { self.try_push_states(&["bool_literal", "true"])?; SubgraphType::Val3 },
            Expr::Value(Value::Boolean(false)) => { self.try_push_states(&["bool_literal", "false"])?; SubgraphType::Val3 },
            expr @ Expr::Value(Value::Number(_, false)) => self.handle_literals_determine_number_type(expr)?,
            expr @ Expr::Cast { expr: _, data_type, format } if *data_type == DataType::BigInt(None) && format.is_none() => self.handle_literals_determine_number_type(expr)?,
            expr @ Expr::UnaryOp { op, expr: _ } if *op == UnaryOperator::Minus => self.handle_literals_determine_number_type(expr)?,
            Expr::Value(Value::SingleQuotedString(v)) => {
                self.try_push_state("text_literal")?;
                self.push_node(PathNode::StringValue(v.clone()));
                SubgraphType::Text
            },
            Expr::TypedString { data_type, value } if *data_type == DataType::Date => {
                self.try_push_state("date_literal")?;
                self.push_node(PathNode::DateValue(value.clone()));
                SubgraphType::Date
            },
            Expr::TypedString { data_type, value } if matches!(
                *data_type, DataType::Timestamp(_, tz) if tz == TimezoneInfo::None
            ) => {
                self.try_push_state("timestamp_literal")?;
                self.push_node(PathNode::TimestampValue(value.clone()));
                SubgraphType::Timestamp
            },
            Expr::Interval(Interval {
                value, leading_field,
                leading_precision: _, last_field: _, fractional_seconds_precision: _,
            }) => {
                self.try_push_state("interval_literal")?;
                self.try_push_state(if leading_field.is_some() {
                    "interval_literal_with_field"
                } else { "interval_literal_format_string" })?;
                self.push_node(PathNode::IntervalValue((
                    unwrap_pat!(&**value, Expr::Value(Value::SingleQuotedString(s)), s).clone(),
                    leading_field.clone()
                )));
                SubgraphType::Interval
            },
            any => unexpected_expr!(any),
        };

        self.try_push_state("EXIT_literals")?;

        Ok(tp)
    }
    
    fn handle_literals_determine_number_type(&mut self, expr: &Expr) -> Result<SubgraphType, ConvertionError> {
        let number_str = match expr {
            Expr::Cast { expr, data_type, format } if *data_type == DataType::BigInt(None) && format.is_none() => {
                let tp = self.handle_literals_determine_number_type(expr)?;
                self.try_push_state("literals_explicit_cast")?;
                return Ok(tp)
            },
            Expr::UnaryOp { op, expr } if *op == UnaryOperator::Minus => {
                let Expr::Value(Value::Number(ref number_str, false)) = **expr else { unexpected_expr!(expr) };
                format!("-{number_str}")
            },
            Expr::Value(Value::Number(ref number_str, false)) => number_str.clone(),
            any => unexpected_expr!(any),
        };
        // assume that literals are the smallest type that they fit into
        // (this is now postgres determines them)
        let checkpoint = self.get_checkpoint(true);
        let err_int_str = match number_str.parse::<i32>() {
            Ok(..) => {
                match self.try_push_state("number_literal_integer") {
                    Ok(..) => {
                        self.push_node(PathNode::IntegerValue(number_str.clone()));
                        return Ok(SubgraphType::Integer)
                    },
                    Err(err_int) => {
                        self.restore_checkpoint(&checkpoint);
                        format!("{err_int}")
                    },
                }
            },
            Err(err_int) => {
                self.restore_checkpoint(&checkpoint);
                format!("{err_int}")
            }
        };
        let err_bigint_str = match number_str.parse::<i64>() {
            Ok(..) => {
                match self.try_push_state("number_literal_bigint") {
                    Ok(..) => {
                        self.push_node(PathNode::BigIntValue(number_str.clone()));
                        return Ok(SubgraphType::BigInt)
                    },
                    Err(err_bigint) => {
                        self.restore_checkpoint(&checkpoint);
                        format!("{err_bigint}")
                    },
                }
            },
            Err(err_bigint) => {
                self.restore_checkpoint(&checkpoint);
                format!("{err_bigint}")
            }
        };
        let err_numeric_str = match number_str.parse::<f64>() {
            Ok(..) => {
                match self.try_push_state("number_literal_numeric") {
                    Ok(..) => {
                        self.push_node(PathNode::NumericValue(number_str.clone()));
                        return Ok(SubgraphType::Numeric)
                    },
                    Err(err_numeric) => {
                        self.restore_checkpoint_consume(checkpoint);
                        format!("{err_numeric}")
                    },
                }
            },
            Err(err_numeric) => {
                self.restore_checkpoint_consume(checkpoint);
                format!("{err_numeric}")
            }
        };
        Err(ConvertionError::new(format!(
            "Error trying to find '{number_str}' literal type.\
            Tried Integer, got: {err_int_str}\
            Tried BigInt, got: {err_bigint_str}\
            Tried Numeric, got: {err_numeric_str}"
        )))
    }

    /// subgraph def_column_spec
    fn handle_column_spec(&mut self, expr: &Expr) -> Result<SubgraphType, ConvertionError> {
        self.try_push_state("column_spec")?;
        let column_types = unwrap_variant_or_else!(
            self.state_generator.get_fn_selected_types_unwrapped(), CallTypes::TypeList, || self.state_generator.print_stack()
        );
        let column_retrieval_options = ColumnRetrievalOptions::from_call_mods(self.state_generator.get_fn_modifiers());

        self.try_push_state("column_spec_mentioned_in_group_by")?;
        self.try_push_state(if column_retrieval_options.only_group_by_columns {
            "column_spec_mentioned_in_group_by_yes"
        } else { "column_spec_mentioned_in_group_by_no" })?;

        self.try_push_state("column_spec_shaded_by_select")?;
        self.try_push_state(if column_retrieval_options.shade_by_select_aliases {
            "column_spec_shaded_by_select_yes"
        } else { "column_spec_shaded_by_select_no" })?;

        self.try_push_state("column_spec_aggregatable_columns")?;
        self.try_push_state(if column_retrieval_options.only_columns_that_can_be_aggregated {
            "column_spec_aggregatable_columns_yes"
        } else { "column_spec_aggregatable_columns_no" })?;

        self.try_push_state("column_spec_choose_qualified")?;
        let ident_components = match expr {
            Expr::Identifier(ident) => {
                self.try_push_state("unqualified_column_name")?;
                vec![ident.clone()]
            },
            Expr::CompoundIdentifier(idents) if idents.len() == 2 => {
                self.try_push_state("qualified_column_name")?;
                idents.clone()
            },
            any => unexpected_expr!(any),
        };

        let (selected_type, qualified_column_name) = match self.clause_context.retrieve_column_by_ident_components(
            &ident_components, column_retrieval_options.try_with_quotes(self.try_with_quotes)
        ) {
            Ok(selected_type) => selected_type,
            Err(err) => return Err(ConvertionError::new(format!("{err}"))),
        };

        if !column_types.contains_generator_of(&selected_type) {
            return Err(ConvertionError::new(format!(
                "get_column_type_by_ident_components() selected a column ({}) with type {:?}, but expected one of {:?}",
                ObjectName(ident_components), selected_type, column_types
            )))
        }

        self.push_node(PathNode::SelectedColumnName(qualified_column_name));
        self.try_push_state("EXIT_column_spec")?;
        Ok(selected_type)
    }

    /// subgraph def_list_expr
    fn handle_list_expr(&mut self, list: &Vec<Expr>) -> Result<SubgraphType, ConvertionError> {
        self.try_push_states(&["list_expr", "call6_types_type"])?;
        let inner_type = self.handle_types_type_and_try(None, TypeAssertion::None, |_self, inner_type| {
            _self.state_generator.set_compatible_list(inner_type.get_compat_types());
            _self.try_push_state("call16_types")?;
            _self.handle_types(&list[0], TypeAssertion::CompatibleWith(inner_type.clone()))?;
            _self.try_push_state("list_expr_multiple_values")?;
            _self.state_generator.set_compatible_list(inner_type.get_compat_types());
            for expr in list.iter().skip(1) {
                _self.try_push_state("call49_types")?;
                _self.handle_types(expr, TypeAssertion::CompatibleWith(inner_type.clone()))?;
            }
            _self.try_push_state("EXIT_list_expr")?;
            Ok(())
        })?;
        Ok(SubgraphType::ListExpr(Box::new(inner_type)))
    }

    /// subgraph def_having
    fn handle_having(&mut self, selection: &Expr) -> Result<SubgraphType, ConvertionError> {
        self.try_push_state("HAVING")?;
        self.try_push_state("call45_types")?;
        let tp = self.handle_types(selection, TypeAssertion::GeneratedBy(SubgraphType::Val3))?;
        self.clause_context.query_mut().set_aggregation_indicated();
        self.try_push_state("EXIT_HAVING")?;
        Ok(tp)
    }

    /// subgraph def_case
    fn handle_case(&mut self, expr: &Expr) -> Result<SubgraphType, ConvertionError> {
        self.try_push_state("case")?;

        let (operand, conditions, results, else_result) = if let Expr::Case {
            operand, conditions, results, else_result
        } = expr {
            (operand, conditions, results, else_result)
        } else { panic!("handle_case reveived {expr}") };

        self.try_push_states(&["case_first_result", "call7_types_type"])?;
        let out_type = self.handle_types_type_and_try(None, TypeAssertion::None, |_self, out_type| {
            _self.try_push_state("call82_types")?;
            _self.state_generator.set_compatible_list(out_type.get_compat_types());
            _self.handle_types(&results[0], TypeAssertion::CompatibleWith(out_type.clone()))?;
            if let Some(operand_expr) = operand {
                _self.try_push_states(&["simple_case", "simple_case_operand", "call8_types_type"])?;
                _self.handle_types_type_and_try(None, TypeAssertion::None, |_self, operand_type| {
                    _self.state_generator.set_compatible_list(operand_type.get_compat_types());
                    _self.try_push_state("call78_types")?;
                    _self.handle_types(operand_expr, TypeAssertion::CompatibleWith(operand_type.clone()))?;
                    for i in 0..conditions.len() {
                        _self.try_push_states(&["simple_case_condition", "call79_types"])?;
                        _self.state_generator.set_compatible_list(operand_type.get_compat_types());
                        _self.handle_types(&conditions[i], TypeAssertion::CompatibleWith(operand_type.clone()))?;
        
                        if i+1 < results.len() {
                            _self.try_push_states(&["simple_case_result", "call80_types"])?;
                            _self.state_generator.set_compatible_list(out_type.get_compat_types());
                            _self.handle_types(&results[i+1], TypeAssertion::CompatibleWith(out_type.clone()))?;
                        }
                    }
                    Ok(())
                })?;
            } else {
                _self.try_push_state("searched_case")?;
    
                for i in 0..conditions.len() {
                    _self.try_push_states(&["searched_case_condition", "call76_types"])?;
                    _self.handle_types(&conditions[i], TypeAssertion::GeneratedBy(SubgraphType::Val3))?;
    
                    if i+1 < results.len() {
                        _self.try_push_states(&["searched_case_result", "call77_types"])?;
                        _self.state_generator.set_compatible_list(out_type.get_compat_types());
                        _self.handle_types(&results[i+1], TypeAssertion::CompatibleWith(out_type.clone()))?;
                    }
                }
            }
    
            _self.try_push_state("case_else")?;
            if let Some(else_expr) = else_result {
                _self.try_push_state("call81_types")?;
                _self.state_generator.set_compatible_list(out_type.get_compat_types());
                _self.handle_types(else_expr, TypeAssertion::CompatibleWith(out_type.clone()))?;
            }
            Ok(())
        })?;

        self.try_push_state("EXIT_case")?;

        Ok(out_type)
    }

    // subgraph def_aggregater_function
    fn handle_aggregate_function(&mut self, function: &ast::Function) -> Result<SubgraphType, ConvertionError> {
        self.try_push_state("aggregate_function")?;

        let selected_types = unwrap_variant!(self.state_generator.get_fn_selected_types_unwrapped(), CallTypes::TypeList);

        let return_type = match selected_types.as_slice() {
            &[ref tp] => tp,
            any => panic!("Aggregate function can only have one selected type. Got: {:?}", any),
        };

        let (aggr_name, aggr_arg_expr_v, distinct) = {
            let ast::Function {
                name: aggr_name,
                args: aggr_arg_expr_v,
                over,
                distinct,
                special,
                filter,
                null_treatment,
                order_by,
            } = function;
            let mut not_supported = vec![];
            if over.is_some() { not_supported.push("over"); }
            if *special { not_supported.push("special"); }
            if filter.is_some() { not_supported.push("filter"); }
            if null_treatment.is_some() { not_supported.push("null_treatment"); }
            if !order_by.is_empty() { not_supported.push("order_by"); }
            if !not_supported.is_empty() {
                return Err(ConvertionError::new(format!(
                    "Aggregate function fields: {} are not supported. Got: {function}", not_supported.into_iter().join(", ")
                )))
            }
            (aggr_name, aggr_arg_expr_v, distinct)
        };

        if *distinct {
            self.try_push_state("aggregate_distinct")?;
        } else {
            self.try_push_state("aggregate_not_distinct")?;
        }

        self.try_push_state("aggregate_select_return_type")?;

        match (return_type, aggr_arg_expr_v.as_slice()) {
            // * AS AN ARGUMENT: BIGINT
            (SubgraphType::BigInt, &[FunctionArg::Unnamed(FunctionArgExpr::Wildcard)])
            if self.aggregate_functions_distribution.func_names_include(
                &AggregateFunctionAgruments::Wildcard,
                &return_type, aggr_name
            ) => {
                self.try_push_states(&["aggregate_select_type_bigint", "arg_star"])?;
            },

            // SINGLE SAME TYPE ARGUMENT: VAL3, DATE, INTEGER, NUMERIC, TEXT, BIGINT
            (
                SubgraphType::Val3 | SubgraphType::Date | SubgraphType::Integer |
                SubgraphType::Numeric | SubgraphType::Text | SubgraphType::BigInt |
                SubgraphType::Timestamp | SubgraphType::Interval,
                &[FunctionArg::Unnamed(FunctionArgExpr::Expr(ref arg_expr))]
            ) if self.aggregate_functions_distribution.func_names_include(
                &AggregateFunctionAgruments::TypeList(vec![return_type.clone()]),
                &return_type, aggr_name
            ) => {
                self.try_push_states(match return_type {
                    SubgraphType::Val3 => &["aggregate_select_type_bool", "arg_single_3vl", "call64_types"],
                    SubgraphType::Date => &["aggregate_select_type_date", "arg_date", "call72_types"],
                    SubgraphType::Timestamp => &["aggregate_select_type_timestamp", "arg_timestamp", "call96_types"],
                    SubgraphType::Interval => &["aggregate_select_type_interval", "arg_interval", "call90_types"],
                    SubgraphType::Integer => &["aggregate_select_type_integer", "arg_integer", "call71_types"],
                    SubgraphType::Text => &["aggregate_select_type_text", "arg_single_text", "call63_types"],
                    SubgraphType::Numeric => &["aggregate_select_type_numeric", "arg_single_numeric", "call66_types"],
                    SubgraphType::BigInt => &["aggregate_select_type_bigint", "arg_bigint", "call75_types"],
                    _ => panic!("Something wrong with match arm"),
                })?;
                self.state_generator.set_compatible_list(return_type.get_compat_types());
                self.handle_types(arg_expr, TypeAssertion::CompatibleWith(return_type.clone()))?;
            },

            // ANY SINGLE ARGUMENT: BIGINT
            (SubgraphType::BigInt, &[FunctionArg::Unnamed(FunctionArgExpr::Expr(ref arg_expr))])
            if self.aggregate_functions_distribution.func_names_include(
                &AggregateFunctionAgruments::AnyType,
                &return_type, aggr_name
            ) => {
                self.try_push_states(&["aggregate_select_type_bigint", "arg_bigint_any", "call65_types"])?;
                self.handle_types(arg_expr, TypeAssertion::None)?;
            },

            // DOUBLE ARGUMENT: NUMERIC, TEXT
            (
                SubgraphType::Numeric | SubgraphType::Text,
                &[
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(ref arg_expr_1)),
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(ref arg_expr_2))
                ]
            ) if self.aggregate_functions_distribution.func_names_include(
                &AggregateFunctionAgruments::TypeList(vec![return_type.clone(), return_type.clone()]),
                &return_type, aggr_name
            ) => {
                let states = match return_type {
                    SubgraphType::Text => &["aggregate_select_type_text", "arg_double_text", "call74_types", "call63_types"],
                    SubgraphType::Numeric => &["aggregate_select_type_numeric", "arg_double_numeric", "call68_types", "call66_types"],
                    _ => panic!("Something wrong with match arm"),
                };
                self.try_push_states(&states[..3])?;
                self.state_generator.set_compatible_list(return_type.get_compat_types());
                self.handle_types(arg_expr_1, TypeAssertion::CompatibleWith(return_type.clone()))?;
                self.try_push_state(states[3])?;
                self.handle_types(arg_expr_2, TypeAssertion::CompatibleWith(return_type.clone()))?;
            },

            _ => {
                unexpected_expr!(function);
            },
        }

        self.push_node(PathNode::SelectedAggregateFunctions(aggr_name.clone()));

        self.try_push_state("EXIT_aggregate_function")?;

        Ok(return_type.clone())
    }

    /// subgraph def_group_by
    fn handle_group_by(&mut self, group_by: &GroupByExpr) -> Result<(), ConvertionError> {
        self.try_push_state("GROUP_BY")?;
        let grouping_exprs = unwrap_variant!(group_by, GroupByExpr::Expressions);
        if grouping_exprs.len() == 0 || grouping_exprs == &[Expr::Value(Value::Boolean(true))] {
            self.clause_context.top_group_by_mut().set_single_group_grouping();
            self.clause_context.top_group_by_mut().set_single_row_grouping();
            self.try_push_states(&["group_by_single_group", "EXIT_GROUP_BY"])?;
            return Ok(())
        } else {
            self.try_push_state("has_accessible_columns")?;
        }
        for grouping_expr in grouping_exprs {
            self.try_push_state("grouping_column_list")?;
            match grouping_expr {
                special_grouping @ (
                    Expr::GroupingSets(..) |
                    Expr::Rollup(..) |
                    Expr::Cube(..)
                ) => {
                    self.try_push_state("special_grouping")?;
                    let (set_list, groupping_type_str) = match special_grouping {
                        Expr::GroupingSets(set_list) => (set_list, "grouping_set"),
                        Expr::Rollup(set_list) => (set_list, "grouping_rollup"),
                        Expr::Cube(set_list) => (set_list, "grouping_cube"),
                        any => unexpected_expr!(any)
                    };
                    self.try_push_state(groupping_type_str)?;
                    for current_set in set_list {
                        self.try_push_state("set_list")?;
                        if current_set.len() == 0 {
                            self.try_push_state("set_list_empty_allowed")?;
                        } else {
                            self.try_push_state("call1_set_item")?;
                            self.handle_set_item(current_set, 0)?;
                        }
                    }
                },
                column_expr @ (
                    Expr::Identifier(..) |
                    Expr::CompoundIdentifier(..)
                ) => {
                    self.try_push_state("call1_column_spec")?;
                    let column_type = self.handle_column_spec(column_expr)?;
                    let column_name = self.clause_context.retrieve_column_by_column_expr(
                        &column_expr, ColumnRetrievalOptions::new(false, false, false).try_with_quotes(self.try_with_quotes)
                    ).unwrap().1;
                    self.clause_context.top_group_by_mut().append_column(column_name, column_type);
                },
                any => unexpected_expr!(any)
            }
        }

        // For cases such as: GROUPING SETS ( (), (), () )
        if !self.clause_context.top_group_by().contains_columns() {
            self.clause_context.top_group_by_mut().set_single_group_grouping();
            // Check is GROUPING SETS ( () )
            if let &[Expr::GroupingSets(ref set_list)] = grouping_exprs.as_slice() {
                if set_list.len() == 1 {
                    self.clause_context.top_group_by_mut().set_single_row_grouping();
                }
            }
        }

        self.clause_context.query_mut().set_aggregation_indicated();

        self.try_push_state("EXIT_GROUP_BY")?;
        Ok(())
    }

    /// subgraph def_set_item
    fn handle_set_item(&mut self, current_set: &Vec<Expr>, i: usize) ->  Result<(), ConvertionError>{
        self.try_push_states(&["set_item", "call2_column_spec"])?;

        let column_expr = &current_set[i];
        let column_type = self.handle_column_spec(column_expr)?;
        let column_name = self.clause_context.retrieve_column_by_column_expr(
            &column_expr, ColumnRetrievalOptions::new(false, false, false).try_with_quotes(self.try_with_quotes)
        ).unwrap().1;
        self.clause_context.top_group_by_mut().append_column(column_name, column_type);

        if (i + 1) < current_set.len() {
            self.try_push_state("call0_set_item")?;
            self.handle_set_item(current_set, i + 1)?;
        }

        self.try_push_state("EXIT_set_item")?;
        Ok(())
    }
}
