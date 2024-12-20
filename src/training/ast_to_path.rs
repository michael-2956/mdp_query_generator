use std::{collections::HashSet, error::Error, fmt, iter, sync::{Arc, Mutex}};

pub mod tester;

use itertools::Itertools;
use rand::SeedableRng;
use rand_chacha::ChaCha8Rng;
use smol_str::SmolStr;
use sqlparser::ast::{self, BinaryOperator, DataType, DateTimeField, Distinct, Expr, FunctionArg, FunctionArgExpr, GroupByExpr, Ident, Interval, ObjectName, OrderByExpr, Query, Select, SelectItem, SetExpr, SetOperator, SetQuantifier, TableFactor, TableWithJoins, TimezoneInfo, TrimWhereField, UnaryOperator, Value};

use crate::{
    query_creation::{
        query_generator::{
            aggregate_function_settings::{AggregateFunctionAgruments, AggregateFunctionDistribution}, ast_builders::types_value::TypeAssertion, call_modifiers::{SelectAccessibleColumnsValue, ValueSetterValue, WildcardRelationsValue}, query_info::{ClauseContext, ClauseContextCheckpoint, ClauseContextFrame, ColumnRetrievalOptions, DatabaseSchema, IdentName, QueryProps}
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

#[derive(Debug, Clone)]
pub struct SelectTypeInfo {
    /// partial select type
    select_type: Vec<SubgraphType>,
    /// real number of columns
    real_n_cols: Option<usize>,
    /// the type of the wildcard responsible
    responsible_wildcard_type: Option<Vec<SubgraphType>>,
}

impl SelectTypeInfo {
    fn new(real_n_cols: Option<usize>, responsible_wildcard_type: Option<Vec<SubgraphType>>, select_type: Vec<SubgraphType>) -> Self {
        Self {
            select_type,
            real_n_cols,
            responsible_wildcard_type,
        }
    }

    fn empty() -> Self {
        Self {
            select_type: vec![],
            real_n_cols: None,
            responsible_wildcard_type: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ConvertionError {
    reason: String,
    select_type_info: Option<SelectTypeInfo>,
}

impl Error for ConvertionError { }

impl fmt::Display for ConvertionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(sti) = self.select_type_info.as_ref() {
            writeln!(f, "Note: Select type info is present:")?;
            writeln!(f, "  Partial select type: {:?}", sti.select_type)?;
            writeln!(f, "  Real number of columns: {:?}", sti.real_n_cols)?;
            writeln!(f, "  Responsible wildcard type: {:?}", sti.responsible_wildcard_type)?;
        } else {
            writeln!(f, "Note: Select type info is not present")?;
        }
        write!(f, "AST to path convertion error:\n{}", self.reason.clone().get_indentated_string())
    }
}

impl ConvertionError {
    pub fn new(reason: String) -> Self {
        Self {
            reason,
            select_type_info: None
        }
    }

    pub fn with_type_info_opt(reason: String, select_type_info: Option<SelectTypeInfo>) -> Self {
        Self {
            reason,
            select_type_info
        }
    }

    pub fn with_type_info(reason: String, select_type_info: SelectTypeInfo) -> Self {
        Self {
            reason,
            select_type_info: Some(select_type_info)
        }
    }

    pub fn add_type_info(&mut self, select_type_info: SelectTypeInfo) {
        self.select_type_info = Some(select_type_info)
    }

    /// removes select type info from the error.\
    /// if none present, returns empty selecttypeinfo
    pub fn take_select_type_info_or_empty(&mut self) -> SelectTypeInfo {
        self.select_type_info.take().unwrap_or(SelectTypeInfo::empty())
    }

    pub fn take_select_type_info(&mut self) -> Option<SelectTypeInfo> {
        self.select_type_info.take()
    }

    pub fn select_type_info_ref(&self) -> Option<&SelectTypeInfo> {
        self.select_type_info.as_ref()
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

#[derive(Debug, Clone, PartialEq)]
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
    path_after_last_rewind: Vec<String>,
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
            path_after_last_rewind: vec![],
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

/// fails if detects wildcards
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

/// Determines n_cols in select and responsible_wildcard the type of wildcard responsible for the error.\
/// None for responsible_wildcard if all types were determined.
fn determine_n_cols_and_responsible_wildcard_with_clause_context(projection: &Vec<SelectItem>, clause_context: &ClauseContext, n_select_types: usize) -> (usize, Option<Vec<SubgraphType>>) {
    let mut n_cols = 0usize;
    let mut responsible_wildcard_type = None;
    let mut was_set = false;
    for item in projection.iter() {
        let (n_new_cols, wildcard_type) = if matches!(item, SelectItem::Wildcard(..) | SelectItem::QualifiedWildcard(..)) {
            match item {
                SelectItem::Wildcard(..) => {
                    let from = clause_context.top_active_from();
                    (from.get_n_wildcard_columns(), Some(from.get_wildcard_type()))
                },
                SelectItem::QualifiedWildcard(rel_name, _) => {
                    let relation = match rel_name.0.as_slice() {
                        [ident] => {
                            let identname: IdentName = ident.clone().into();
                            clause_context.get_relation_by_name(&identname)
                        },
                        any => panic!("schema.table alias is not supported: {}", ObjectName(any.to_vec())),
                    };
                    (relation.get_n_wildcard_columns(), Some(relation.get_wildcard_type()))
                },
                _ => unreachable!(),
            }
        } else {
            (1usize, None)
        };
        n_cols += n_new_cols;
        if n_cols > n_select_types && !was_set {
            responsible_wildcard_type = wildcard_type;
            was_set = true;
        }
    }
    (n_cols, responsible_wildcard_type)
}

/// - returns error if we are responsible, making caller try another type
/// - returns Ok(true) if we are responsible, but the type of the next column is now known,\
///   making the caller use the newly acquired type info
/// - returns Ok(false) if we are not responsible, making caller pass the error up the chain
fn check_responsible(
        column_count_can_cause_errors: bool,
        n_cols_completed: usize,
        n_cols_total: usize,
        tp: SubgraphType,
        err: &ConvertionError
    ) -> Result<bool, ConvertionError> {
    let esti = SelectTypeInfo::empty();
    let select_info = err.select_type_info_ref().unwrap_or(&esti);
    // We are not responsible in two cases:
    // -> If the column count can cause errors, the real_n_cols info should
    //    be passed up the chain to be implemented, after which the entire
    //    type selection should be restarted.
    // -> If all of the previously selected types are correct, we are responsible
    //    and should try to choose some different type
    let type_is_responsible = !column_count_can_cause_errors && n_cols_completed == select_info.select_type.len();
    // The info about the actual type of our column was given
    let type_info_present = select_info.responsible_wildcard_type.is_some();
    if type_is_responsible && !type_info_present {
        // We are responsible for the error, but did not get type info about following columns
        Err(ConvertionError::with_type_info(
            format!(
                "Having determined {n_cols_completed}/{n_cols_total} types: {:?},\nTried {tp} for the next column, but got an error:\n{}",
                select_info.select_type, err
            ), select_info.clone()
        ))
    } else {
        // This covers two cases:
        // - We either got type info about new columns, in which case
        //   type_is_responsible is true and type_info_present is true, or
        // - We are not responsible, in which case type_is_responsible is false
        //   and type_info_present doesn't matter since we just pass the error up
        Ok(type_is_responsible)
    }
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
            ConvertionError::new(format!("{err}\nPath after last rewind: {:#?}", self.path_after_last_rewind))
        })? {
            if actual_state != state {
                self.state_generator.print_stack();
                panic!("State generator returned {actual_state}, expected {state}");
            }
        } else {
            self.state_generator.print_stack();
            panic!("State generator stopped prematurely");
        }
        self.path_after_last_rewind.push(format!("{state}"));
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
        // eprintln!("Path after last rewind: {:?}", self.path_after_last_rewind);
        self.path_after_last_rewind.clear();
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
    fn handle_query(&mut self, query: &Box<Query>) -> Result<ClauseContextFrame, ConvertionError> {
        self.clause_context.on_query_begin(self.state_generator.get_fn_modifiers_opt());
        self.try_push_state("Query")?;

        self.handle_query_body(&query.body)?;

        self.try_push_state("call0_ORDER_BY")?;
        self.handle_order_by(&query.order_by)?;

        self.try_push_state("call0_LIMIT")?;
        self.handle_limit(&query.limit)?;

        self.try_push_state("EXIT_Query")?;

        Ok(self.clause_context.on_query_end())
    }

    fn handle_query_body(&mut self, query_body: &SetExpr) -> Result<(), ConvertionError> {
        self.try_push_state("call1_set_expression_determine_type")?;

        let column_type_lists = unwrap_variant!(self.state_generator.get_fn_selected_types_unwrapped(), CallTypes::QueryTypes);
        // the number of columns the caller expectes from us. We are not responsible
        // for this, we will pass real_n_cols to the caller if they mismatch.
        let expected_n_cols_opt = match column_type_lists {
            QueryTypes::ColumnTypeLists { column_type_lists } => Some(column_type_lists.len()),
            QueryTypes::TypeList { type_list: _ } => None,
        };

        let mut n_cols = determine_n_cols(query_body).or(expected_n_cols_opt.clone());
        let expr_str = Some(format!("{query_body}"));
        let mut expected_column_count_matched = false;
        loop {
            let (column_count_can_cause_errors, n_cols_arg) = match &n_cols {
                Some(n_cols) => (
                    // by default, columns can cause errors if the expected n cols restricts us
                    // (for example, is 1 when actually we have 3 columns)
                    // however, not if they matched
                    if expected_column_count_matched { false } else { expected_n_cols_opt.is_some()},
                    *n_cols
                ),
                // if expected n cols is None, columns can cause errors if determine_n_cols returned None
                // for example, if a wildcard was present
                None => (true, 1usize),
            };

            // if column count can cause errors, we will restore and use real_n_cols to rerun with the correct column count
            let checkpoint_opt = if column_count_can_cause_errors {
                Some(self.get_checkpoint(true))
            } else { None };

            // if the next call returns Ok(Some(err)), we have to check that error for the real number of columns
            let select_err = self.handle_set_expression_determine_type_and_try(
                expr_str.clone(),
                n_cols_arg,
                n_cols_arg,
                vec![],
                column_count_can_cause_errors,
                None::<iter::Empty<SubgraphType>>,
                |_self, column_types| {
                    let column_type_list = QueryTypes::ColumnTypeLists {
                        column_type_lists: column_types.into_iter().map(|x| vec![x]).collect_vec()
                    };
                    _self.try_push_state("call0_set_expression")?;
                    _self.state_generator.set_known_query_type_list(column_type_list);
                    _self.handle_set_expression(query_body)?;
                    Ok(())
                }
            )?;

            // Check if the method wants us to check the error for real_n_cols
            match select_err {
                Some(mut err) => {
                    if !column_count_can_cause_errors {
                        return Err(ConvertionError::with_type_info(
                            format!("Couldn't select SELECT type. Got the error:\n{}", err),
                            err.take_select_type_info_or_empty()
                        ))
                    } else {
                        self.restore_checkpoint_consume(checkpoint_opt.unwrap());
                        // column count caused an error, so try another time with real_n_cols
                        let select_type_info = err.take_select_type_info_or_empty();
                        match (&select_type_info.real_n_cols, &expected_n_cols_opt) {
                            // 1) real column number received and no expected column number by caller
                            //    => test again with real_n_cols. 
                            (Some(real_n_cols), None) => n_cols = Some(*real_n_cols),
                            // 2) we got the expected number of columns, but it matches.
                            //    => test again with column_count_can_cause_errors = false.
                            (
                                Some(real_n_cols), Some(expected_n_cols)
                            ) if real_n_cols == expected_n_cols => {
                                expected_column_count_matched = true;
                                n_cols = Some(*real_n_cols);
                            },
                            // 1) no real column number received
                            //    => return error because we can't do anything in this case
                            // 2) real column number was received but does not match with that of the caller.
                            //    => return error to pass real_n_cols to the caller
                            _ => {
                                // the problem is not us, so exit with error
                                return Err(ConvertionError::with_type_info(
                                    format!("real_n_cols estimation is empty! Error:\n{}", err),
                                    select_type_info,
                                ))
                            },
                        }
                    }
                },
                None => break,
            }
        }
        Ok(())
    }

    /// subgraph def_set_expression_determine_type
    /// 
    /// - returns Ok(None) if everything went ok
    /// - returns Ok(Some(err)) if the error bears some important info, like the real number of columns
    /// - returns Err(err) if there is not important info
    fn handle_set_expression_determine_type_and_try<
        F: Fn(&mut PathGenerator, Vec<SubgraphType>) -> Result<(), ConvertionError> + Clone
    >(
        &mut self,
        expr_str: Option<String>,
        n_cols_remaining: usize,
        n_cols_total: usize,
        column_types: Vec<SubgraphType>,
        column_count_can_cause_errors: bool,
        mut item_type_iter: Option<impl Iterator<Item = SubgraphType> + Clone>,
        and_try: F
    ) -> Result<Option<ConvertionError>, ConvertionError> {
        // needed if we get the type info about our column and need to re-run
        let before_us_checkpoint = self.get_checkpoint(false);
        self.try_push_states(&["set_expression_determine_type", "call9_types_type"])?;

        // obtain the list of types that are allowed for the current column by the caller
        let column_type_lists = unwrap_variant!(self.state_generator.get_fn_selected_types_unwrapped(), CallTypes::QueryTypes);
        let (first_column_list, remaining_columns) = column_type_lists.split_first();
        // pass it to the types_type subgraph that will be called
        self.state_generator.set_known_list(first_column_list.clone());

        // the number of columns before our column
        let n_cols_completed = n_cols_total - n_cols_remaining;

        // -> This will be populated by an error if and_try finishes with an error, but
        //    the type that we selected came after the type that was responsible for it,
        //    or in a rare case that the type in item_type_iter was wrong.
        let error_to_pass_up: Arc<Mutex<Option<ConvertionError>>> = Arc::new(Mutex::new(None));
        // -> This will be populated by an error if we get info about the type of our column
        //    and we should return to before_us_checkpoint and try this type.
        // -> In case we got info about multiple consequtive types, this will be Some(...) if
        //    we are the first type in the sequence. We will then launch a chain of type selections
        //    with the now known types.
        let error_with_new_cols_type_info: Arc<Mutex<Option<ConvertionError>>> = Arc::new(Mutex::new(None));

        // This will be Some(tp) if the type of our column is known
        let tp_opt = item_type_iter.as_mut().and_then(|it| it.next());

        let continue_with_type = |_self: &mut PathGenerator, tp: SubgraphType| {
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
                        let use_new_type_info = check_responsible(column_count_can_cause_errors, n_cols_completed, n_cols_total, tp, &err)?;
                        if use_new_type_info {
                            // finish and simulate a bunch of selections with the now known types
                            *error_with_new_cols_type_info.lock().unwrap() = Some(err);
                        } else {
                            // we are not responsible: finish and pass the error up
                            *error_to_pass_up.lock().unwrap() = Some(err);
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
                    column_count_can_cause_errors,
                    item_type_iter.clone(),
                    and_try.clone()
                )?;
                if let Some(inner_error) = inner_error_opt {
                    let use_new_type_info = check_responsible(
                        column_count_can_cause_errors, n_cols_completed, n_cols_total, tp, &inner_error
                    )?;
                    if use_new_type_info {
                        // finish and simulate a bunch of selections with the now known types
                        *error_with_new_cols_type_info.lock().unwrap() = Some(inner_error);
                    } else {
                        // we are not responsible: finish and pass the error up
                        *error_to_pass_up.lock().unwrap() = Some(inner_error);
                    }
                }
            }
            Ok(())
        };

        if let Some(tp) = tp_opt.as_ref() {
            let before_type_checkpoint = self.get_checkpoint(false);
            // -> Type is known: run with pre-determined type instead of testing it out
            // -> If the next call errors, the caller didn't allow the actual column type,
            //    in which case this method returns an error
            // -> The error will be caught by the instance that gave us the type of our column
            //    and will be returned to caller
            self.handle_types_type(tp)?;
            // -> Should not return an error if the type information was right
            // -> The type information can be wrong, for example if the left hand of a
            //    set operation has an [Integer, Integer] wildcard, but the right hand side
            //    type has a [Integer, Number] wildcard.
            // -> In case of incorrect type information, either error_with_new_cols_type_info
            //    or error_with_incomplete_type can be set, and we should pass up/use them now.
            match continue_with_type(self, tp.clone()) {
                Ok(()) => {
                    if let Some(esti) = error_with_new_cols_type_info.lock().unwrap().clone() {
                        let sti = esti.select_type_info_ref().unwrap();
                        let new_cols_type_info = sti.responsible_wildcard_type.as_ref().unwrap();
                        if tp.get_compat_types().contains(new_cols_type_info.first().unwrap()) {
                            return Err(ConvertionError::new(format!(
                                "Tried the supplied type {tp}, but got error_with_new_cols_type_info with a smaller type: {esti}"
                            )));
                        }
                    }
                },
                Err(err) => {
                    self.restore_checkpoint_consume(before_type_checkpoint);
                    // -> Type didn't work out, so try the types that go after it,
                    //    as types type will arrange them by compatibility.
                    match self.handle_types_type_and_try(
                        expr_str.clone(),
                        Some(tp.clone()),
                        TypeAssertion::GeneratedByOneOf(&first_column_list),
                        continue_with_type
                    ) {
                        Ok(..) => {
                            if let Some(esti) = error_with_new_cols_type_info.lock().unwrap().clone() {
                                let sti = esti.select_type_info_ref().unwrap();
                                let new_cols_type_info = sti.responsible_wildcard_type.as_ref().unwrap();
                                if tp.get_compat_types().contains(new_cols_type_info.first().unwrap()) {
                                    return Err(ConvertionError::new(format!(
                                        "Tried the supplied type {tp}, but got: {err}\nTried to find types after {tp}, but got error_with_new_cols_type_info with a smaller type: {esti}"
                                    )));
                                }
                            }
                        },
                        Err(err_other_types) => {
                            return Err(ConvertionError::new(format!(
                                "Tried the supplied type {tp}, but got: {err}\nTried other types, but got: {err_other_types}"
                            )))
                        },
                    }
                },
            }
        } else {
            // Type is not known: test out types from first_column_list.
            // -> If this returns an error, the type is not among
            //    the types supplied by caller in first_column_list
            self.handle_types_type_and_try(
                expr_str.clone(),
                None,
                TypeAssertion::GeneratedByOneOf(&first_column_list),
                continue_with_type
            )?;
        }

        // -> Check if the select handler detected the types of the
        //    following columns and wants us to use them.
        if let Some(mut err) = error_with_new_cols_type_info.lock().unwrap().take() {
            assert!(error_to_pass_up.lock().unwrap().is_none());
            // new_cols_type_info should be present in error_with_new_cols_type_info
            // if the code works correctly.
            let select_type_info = err.take_select_type_info_or_empty();
            if let Some(new_cols_type_info) = select_type_info.responsible_wildcard_type.clone() {
                self.restore_checkpoint_consume(before_us_checkpoint);
                match self.handle_set_expression_determine_type_and_try(
                    expr_str.clone(),
                    n_cols_remaining,
                    n_cols_total,
                    column_types,
                    column_count_can_cause_errors,
                    Some(new_cols_type_info.clone().into_iter()),
                    and_try.clone()
                ) {
                    // We may get an error, for example, if type info is correct for the left hand side
                    // of a set operation, but too small for some types on right had side before the
                    // current type. If we got an Ok, we are not responsible for the error, so pass it up.
                    Ok(err) => {
                        *error_to_pass_up.lock().unwrap() = err;
                    },
                    // The error can be returned without type info if the caller does not allow us to have
                    // the type we should. If the type info is present, it we already put it there.
                    Err(mut err) => {
                        if err.select_type_info_ref().is_none() {
                            err.add_type_info(select_type_info);
                        } else {
                            assert!(err.select_type_info_ref().unwrap().responsible_wildcard_type.is_some());
                        }
                        return Err(err)
                    },
                }
            } else {
                // This is unexpected, and check_responsible is probably at fault
                return Err(ConvertionError::new(format!(
                    "Got an error about a wildcard column, but it does not have type info about following columns.\nselect_type: {:?}\nreal_n_cols: {:?}\nresponsible_wildcard_type: {:?}\nError:\n{err}",
                    select_type_info.select_type, select_type_info.real_n_cols, select_type_info.responsible_wildcard_type
                )))
            }
        }

        let ans = error_to_pass_up.lock().unwrap().take();
        Ok(ans)
    }

    /// subgraph def_set_expression
    fn handle_set_expression(&mut self, expr: &SetExpr) -> Result<(), ConvertionError> {
        self.try_push_state("set_expression")?;

        match expr {
            SetExpr::Select(select_body) => {
                self.try_push_state("call0_SELECT_query")?;
                self.handle_select_query(&**select_body)?;
            },
            SetExpr::Query(query) => {
                self.try_push_state("call7_Query")?;
                // if error occurs, select type info will be present
                let subquery_frame = self.handle_query(query)?;
                self.clause_context.replace_with_frame(subquery_frame);
            },
            SetExpr::SetOperation {
                op,
                set_quantifier,
                left,
                right,
            } => {
                self.try_push_state("set_expression_set_operation")?;
                if *set_quantifier != SetQuantifier::None {
                    return Err(ConvertionError::new(format!("In set expression: {expr},\nError: Set quentifiers are not supported")))
                }

                self.try_push_state(match op {
                    SetOperator::Intersect => "query_set_op_intersect",
                    SetOperator::Union => "query_set_op_union",
                    SetOperator::Except => "query_set_op_except",
                })?;
                // Determine first set operation and second set operation call nodes.
                let (left_setop_state, right_setop_state) = match op {
                    SetOperator::Union | SetOperator::Except => ("call2_set_expression", "call5_set_expression"),
                    SetOperator::Intersect => ("call3_set_expression", "call4_set_expression"),
                };

                let empty_frame = self.clause_context.clone_frame();
                self.try_push_state(left_setop_state)?;
                // if error occurs, select type info will be present here, and it will represent the leftmost select
                self.handle_set_expression(&**left)?;
                let left_frame = self.clause_context.replace_with_frame(empty_frame);

                self.try_push_state(right_setop_state)?;
                // In some cases, the righthand type may inform us that a bigger type is needed,
                // so we shoudn't drop type info in the error
                self.handle_set_expression(&**right)?;
                self.clause_context.merge_right_frame_into_setop_parent(left_frame);
            },
            any => return Err(ConvertionError::new(format!("Unexpected query body: {:?}", any))),
        }

        self.try_push_state("EXIT_set_expression")?;

        Ok(())
    }

    /// subgraph def_SELECT_query
    fn handle_select_query(&mut self, select_body: &Select) -> Result<(), ConvertionError> {
        self.try_push_state("SELECT_query")?;

        self.try_push_state("call0_FROM")?;
        self.handle_from(&select_body.from)?;

        if let Some(ref selection) = select_body.selection {
            self.try_push_state("call0_WHERE")?;
            self.handle_where(selection)?;
        }

        self.try_push_state("WHERE_done")?;

        if select_body.group_by != GroupByExpr::Expressions(vec![]) {
            self.handle_query_after_group_by(select_body, true)?;
        } else {
            let implicit_group_by_checkpoint = self.get_checkpoint(true);
            match self.handle_query_after_group_by(select_body, false) {
                Ok(..) => { },
                Err(mut err_no_grouping) => {
                    self.restore_checkpoint(&implicit_group_by_checkpoint);
                    match self.handle_query_after_group_by(select_body, true) {
                        Ok(..) => { },
                        Err(mut err_grouping) => {
                            self.restore_checkpoint_consume(implicit_group_by_checkpoint);
                            let no_grouping_info = err_no_grouping.take_select_type_info_or_empty();
                            let grouping_info = err_grouping.take_select_type_info_or_empty();
                            return Err(ConvertionError::with_type_info(
                                format!(
                                    "Error converting query: {select_body}\n\
                                    Tried without implicit grouping, got an error: {err_no_grouping}\n\
                                    Tried with implicit grouping, got an error: {err_grouping}"
                                ),
                                SelectTypeInfo::new(
                                    no_grouping_info.real_n_cols.or(grouping_info.real_n_cols),
                                    if grouping_info.select_type.len() > no_grouping_info.select_type.len() {
                                        grouping_info.responsible_wildcard_type  // the bigger one is the one that has correct assumptions
                                    } else {
                                        no_grouping_info.responsible_wildcard_type
                                    },
                                    if grouping_info.select_type.len() > no_grouping_info.select_type.len() {
                                        grouping_info.select_type
                                    } else {
                                        no_grouping_info.select_type
                                    }
                                )
                            ))
                        }
                    }
                }
            }
        };

        self.try_push_state("EXIT_SELECT_query")?;

        Ok(())
    }

    fn handle_query_after_group_by(&mut self, select_body: &Select, with_group_by: bool) -> Result<(), ConvertionError> {
        if with_group_by {
            self.try_push_state("call0_GROUP_BY")?;
            self.handle_group_by(&select_body.group_by)?;
        }

        if let Some(ref having) = select_body.having {
            if self.clause_context.top_group_by().is_grouping_active() {
                self.try_push_state("call0_HAVING")?;
                self.handle_having(having)?;
            } else {
                return Err(ConvertionError::new(format!(
                    "HAVING was used but with_grouping was set to false"
                )))
            }
        }

        self.try_push_state("call0_SELECT")?;
        self.handle_select(&select_body.distinct, &select_body.projection)?;

        Ok(())
    }

    /// subgraph def_ORDER_BY
    fn handle_order_by(&mut self, order_by: &Vec<OrderByExpr>) -> Result<(), ConvertionError> {
        self.try_push_state("ORDER_BY")?;

        let expr_state = if self.clause_context.top_group_by().is_grouping_active() {
            "call84_types"
        } else { "call85_types" };

        if !order_by.is_empty() {
            self.try_push_state("order_by_check_order_by_present")?;
            self.clause_context.query_mut().set_order_by_present();
        }

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
                let column_idents_and_graph_types = self.handle_query(subquery)?.into_query_props().into_select_type();
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
    fn handle_select(&mut self, distinct: &Option<Distinct>, projection: &Vec<SelectItem>) -> Result<Vec<SubgraphType>, ConvertionError> {
        self.try_push_state("SELECT")?;
        if distinct.is_some() {
            self.try_push_state("SELECT_DISTINCT")?;
            self.clause_context.query_mut().set_distinct();
        }
        self.try_push_state("call0_SELECT_item")?;
        
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
            Err(mut err) => {
                let select_type = self.clause_context.query_mut().select_type().iter().map(|(_, tp)| tp.clone()).collect_vec();
                let (real_n_cols, responsible_wildcard_type) = determine_n_cols_and_responsible_wildcard_with_clause_context(
                    projection, &self.clause_context, select_type.len()
                );
                err.add_type_info(SelectTypeInfo::new(
                    Some(real_n_cols), responsible_wildcard_type, select_type
                ));
                return Err(err)
            },
        };

        self.try_push_state("EXIT_SELECT")?;

        Ok(select_type)
    }

    /// subgraph def_SELECT_item
    fn handle_select_item(
        &mut self, projection: &[SelectItem], select_item_state: &str
    ) -> Result<(), ConvertionError> {

        self.try_push_state("SELECT_item")?;

        if projection.len() == 0 {
            self.try_push_states(&["SELECT_item_can_finish", "EXIT_SELECT_item"])?;
            return Ok(())
        }

        self.try_push_state("SELECT_item_grouping_enabled")?;

        let (select_item, remaining_projection) = projection.split_first().unwrap();

        let arg = unwrap_variant!(
            self.state_generator.get_fn_selected_types_unwrapped(), CallTypes::QueryTypes
        );

        let remaining_column_types = match select_item {
            select_item @ (SelectItem::UnnamedExpr(..) | SelectItem::ExprWithAlias { .. }) => {
                let (
                    first_column_list, remaining_column_types
                ) = arg.split_first();
                let (mut alias, expr) = match select_item {
                    SelectItem::UnnamedExpr(expr) => {
                        self.try_push_states(&["SELECT_unnamed_expr", "select_expr", select_item_state])?;
                        let alias = QueryProps::extract_alias(&expr);
                        (alias, expr)
                    },
                    SelectItem::ExprWithAlias { expr, alias } => {
                        self.try_push_state("SELECT_expr_with_alias")?;
                        self.push_node(PathNode::SelectAlias(alias.clone()));  // the order is important
                        self.try_push_states(&["select_expr", select_item_state])?;
                        (Some(alias.clone().into()), expr)
                    },
                    _ => unreachable!(),
                };
                self.state_generator.set_compatible_list(first_column_list.iter().flat_map(
                    |tp| tp.get_compat_types().into_iter()  // vec![tp.clone()].into_iter()
                ).collect::<HashSet<SubgraphType>>().into_iter().collect_vec());
                let (cn, tp) = self.handle_types_with_column_name(expr, TypeAssertion::CompatibleWithOneOf(&first_column_list))?;
                alias = alias.or(cn);
                self.try_push_state("select_expr_done")?;
                self.clause_context.query_mut().select_type_mut().push((alias, tp));
                remaining_column_types
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

                let new_types: Vec<(Option<IdentName>, SubgraphType)> = relation.get_wildcard_columns_iter().collect_vec();
                let remaining_column_types = arg.after_prefix(new_types.iter().map(|(_, tp)| tp));
                self.clause_context.query_mut().select_type_mut().extend(new_types.into_iter());
                self.push_node(PathNode::QualifiedWildcardSelectedRelation(alias.0.last().unwrap().clone()));
                remaining_column_types
            },
            SelectItem::Wildcard(..) => {
                self.try_push_states(&["SELECT_tables_eligible_for_wildcard", "SELECT_wildcard"])?;
                let new_types = self.clause_context.top_active_from().get_wildcard_columns_iter().collect_vec();
                let remaining_column_types = arg.after_prefix(new_types.iter().map(|(_, tp)| tp));
                self.clause_context.query_mut().select_type_mut().extend(new_types.into_iter());
                remaining_column_types
            },
        };

        self.try_push_state("call1_SELECT_item")?;
        self.state_generator.set_known_query_type_list(remaining_column_types);
        self.handle_select_item(remaining_projection, select_item_state)?;
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
                    self.try_push_states(&["is_limit_present", "limit_not_present", "single_row_true"])?;
                    self.clause_context.query_mut().set_limit_present();
                    SubgraphType::Integer
                },
                _ => {
                    self.try_push_states(&["is_limit_present", "limit_not_present", "limit_num", "call52_types"])?;
                    self.state_generator.set_compatible_list(SubgraphType::Numeric.get_compat_types());
                    let tp = self.handle_types(expr, TypeAssertion::CompatibleWith(SubgraphType::Numeric))?;
                    self.clause_context.query_mut().set_limit_present();
                    tp
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
                self.handle_types_type_and_try(None, None, TypeAssertion::None, |_self, returned_type| {
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
                self.handle_types_type_and_try(None, None, TypeAssertion::None, |_self, returned_type| {
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
                self.handle_types_type_and_try(None, None, TypeAssertion::None, |_self, returned_type| {
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
                self.handle_types_type_and_try(None, None, TypeAssertion::None, |_self, returned_type| {
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
                self.handle_types_type_and_try(None, None, TypeAssertion::None, |_self, returned_type| {
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
                        self.handle_types_type_and_try(None, None, TypeAssertion::None, |_self, returned_type| {
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
                match self.handle_types(left, TypeAssertion::GeneratedBy(SubgraphType::Interval)) {
                    Ok(_) => {
                        self.try_push_state("call92_types")?;
                        self.handle_types(right, TypeAssertion::GeneratedBy(SubgraphType::Interval))?;
                    },
                    Err(err) => {
                        self.restore_checkpoint_consume(checkpoint);
                        if *op == BinaryOperator::Plus {
                            return Err(err)  // only timestamp - timestamp
                        }
                        self.try_push_state("call98_types")?;
                        self.handle_types(left, TypeAssertion::GeneratedBy(SubgraphType::Timestamp))?;
                        self.try_push_state("call99_types")?;
                        self.handle_types(right, TypeAssertion::GeneratedBy(SubgraphType::Timestamp))?;
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
    /// 
    /// Returns the value name in addition to its type, useful when preserving
    /// column names of subqueries is important.
    fn handle_types_with_column_name(&mut self, expr: &Expr, type_assertion: TypeAssertion) -> Result<(Option<IdentName>, SubgraphType), ConvertionError> {
        self.try_push_state("types")?;
        let selected_types = unwrap_variant!(self.state_generator.get_fn_selected_types_unwrapped(), CallTypes::TypeList);
        let selected_types = SubgraphType::sort_by_compatibility(selected_types);
        let mut selected_types_iter = selected_types.iter();
        self.handle_types_continue_after(expr, type_assertion, &selected_types, &mut selected_types_iter)
    }

    /// subgraph def_types
    fn handle_types(&mut self, expr: &Expr, type_assertion: TypeAssertion) -> Result<SubgraphType, ConvertionError> {
        self.handle_types_with_column_name(expr, type_assertion).map(|(.., tp)| tp)
    }

    /// only use in handle_types
    fn handle_types_continue_after<'a>(
        &mut self, expr: &Expr, type_assertion: TypeAssertion,
        selected_types: &Vec<SubgraphType>, selected_types_iter: &mut impl Iterator<Item = &'a SubgraphType>
    ) -> Result<(Option<IdentName>, SubgraphType), ConvertionError> {
        let (cn, tp) = self.handle_types_expr(expr, selected_types, selected_types_iter)?;
        self.try_push_state("EXIT_types")?;
        type_assertion.check(&tp, expr);
        return Ok((cn, tp))
    }

    fn handle_types_expr<'a>(&mut self, expr: &Expr, selected_types: &Vec<SubgraphType>, selected_types_iter: &mut impl Iterator<Item = &'a SubgraphType>) -> Result<(Option<IdentName>, SubgraphType), ConvertionError> {
        let mut err_str = "".to_string();
        let types_before_state_selection = self.get_checkpoint(true);

        // try out different allowed types to find the actual one (or not)
        let (cn, tp) = loop {
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
                Ok((cn, tp)) => {
                    if tp.is_same_or_more_determined_or_undetermined(subgraph_type) {
                        break (cn, tp)
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

        Ok((cn, tp))
    }

    /// subgraph def_types_type
    /// 
    /// If after_type is Some, will skip the types that go before it and the after_type itself
    fn handle_types_type_and_try<F: Fn(&mut PathGenerator, SubgraphType) -> Result<(), ConvertionError>>(
        &mut self, expr_str: Option<String>, after_type: Option<SubgraphType>, type_assertion: TypeAssertion, and_try: F
    ) -> Result<SubgraphType, ConvertionError> {
        // Use like:
        // let returned_type = self.handle_types_type_and_try(None, None, TypeAssertion::None, |_self, returned_type| {
        //     _self.something(...);
        //     Ok(())
        // })?;
        self.handle_types_type_entry()?;
        let selected_types = unwrap_variant!(self.state_generator.get_fn_selected_types_unwrapped(), CallTypes::TypeList);
        let selected_types = SubgraphType::sort_by_compatibility(selected_types);
        let mut selected_types_iter = selected_types.iter();
        if let Some(after_type) = after_type {
            let mut found = false;
            while let Some(tp) = selected_types_iter.next() {
                if *tp == after_type {
                    found = true;
                    break
                }
            }
            // if not found, try all of the types that are allowed
            if !found {
                selected_types_iter = selected_types.iter();
            }
        }
        let mut err_msg = "".to_string();
        let mut select_type_info_opt = None;
        // can't use cutoff because restoration may be on a level that's less deep
        let checkpoint = self.get_checkpoint(false);
        loop {
            let tp = match selected_types_iter.next() {
                Some(subgraph_type) => subgraph_type,
                None => break Err(ConvertionError::with_type_info_opt(format!(
                    "Types type didn't find a suitable type for expression, among:\n{:#?}\nExpr: {}\n{}",
                    selected_types,
                    if let Some(expr_str) = expr_str { format!("{expr_str}") } else { format!("not provided by caller") },
                    err_msg.get_indentated_string()
                ), select_type_info_opt))
            };
            if let Err(err) = self.handle_types_type_type(tp) {
                break Err(ConvertionError::with_type_info_opt(format!("{err}\n{}", err_msg.get_indentated_string()), select_type_info_opt))
            }
            type_assertion.check_type(&tp);
            match and_try(self, tp.clone()) {
                Ok(..) => {
                    break Ok(tp.clone())
                },
                Err(mut err) => {
                    select_type_info_opt = err.take_select_type_info();
                    err_msg += format!("Tried with {tp}, but got:\n{}\n", err.get_indentated_string()).as_str();
                    self.restore_checkpoint(&checkpoint);
                },
            };
        }
    }

    /// subgraph types_type
    fn handle_types_type(&mut self, subgraph_type: &SubgraphType) -> Result<(), ConvertionError> {
        self.handle_types_type_entry()?;
        self.handle_types_type_type(subgraph_type)
    }

    /// use handle_types_type_type afterwards
    fn handle_types_type_entry(&mut self) -> Result<(), ConvertionError> {
        self.try_push_state("types_type")?;
        Ok(())
    }

    /// only for use after handle_types_type_entry
    fn handle_types_type_type(
        &mut self, subgraph_type: &SubgraphType
    ) -> Result<(), ConvertionError> {
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

        Ok(())
    }

    /// subgraph def_types_value
    ///
    /// value may have a name if it comes from a subquery
    fn handle_types_value(&mut self, expr: &Expr) -> Result<(Option<IdentName>, SubgraphType), ConvertionError> {
        let mut column_name = None;
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
                (match self.handle_types_value(expr) {
                    Ok((cn, tp)) => {
                        column_name = cn; Ok(tp)
                    },
                    Err(err) => Err(err),
                }, "nested")
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
            Expr::Subquery(subquery) => {
                (match self.handle_types_value_subquery(subquery, &selected_types) {
                    Ok((cn, tp)) => {
                        column_name = cn; Ok(tp)
                    },
                    Err(err) => Err(err),
                }, "Subquery")
            },
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
                Ok((column_name, tp))
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

    fn handle_types_value_subquery(&mut self, subquery: &Box<Query>, selected_types: &Vec<SubgraphType>) -> Result<(Option<IdentName>, SubgraphType), ConvertionError> {
        self.try_push_state("call1_Query")?;
        self.state_generator.set_known_query_type_list(QueryTypes::ColumnTypeLists {
            column_type_lists: vec![selected_types.clone()]  // single column
        });
        let col_type_list = self.handle_query(subquery)?.into_query_props().into_select_type();
        if let [(column_name, query_subgraph_type)] = col_type_list.as_slice() {
            // change to explorable if we use this feature
            Ok((column_name.clone(), query_subgraph_type.clone()))
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
        let inner_type = self.handle_types_type_and_try(None, None, TypeAssertion::None, |_self, inner_type| {
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
        let out_type = self.handle_types_type_and_try(None, None, TypeAssertion::None, |_self, out_type| {
            _self.try_push_state("call82_types")?;
            _self.state_generator.set_compatible_list(out_type.get_compat_types());
            _self.handle_types(&results[0], TypeAssertion::CompatibleWith(out_type.clone()))?;
            if let Some(operand_expr) = operand {
                _self.try_push_states(&["simple_case", "simple_case_operand", "call8_types_type"])?;
                _self.handle_types_type_and_try(None, None, TypeAssertion::None, |_self, operand_type| {
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
