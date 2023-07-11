#[macro_use]
mod query_info;

use rand::SeedableRng;
use rand_chacha::ChaCha8Rng;
use smol_str::SmolStr;
use sqlparser::ast::{
    Expr, Ident, Query, Select, SetExpr, TableFactor,
    TableWithJoins, Value, BinaryOperator, UnaryOperator, TrimWhereField, Array, SelectItem, WildcardAdditionalOptions, ObjectName,
};

use super::{super::{unwrap_variant, unwrap_variant_and_else}, state_generators::{SubgraphType, CallTypes}};
use self::query_info::{FromContents, DatabaseSchema};

use super::state_generators::{MarkovChainGenerator, dynamic_models::DynamicModel, state_choosers::StateChooser};

pub struct QueryGenerator<DynMod: DynamicModel, StC: StateChooser> {
    state_generator: MarkovChainGenerator<StC>,
    dynamic_model: Box<DynMod>,
    database_schema: DatabaseSchema,
    from_contents_stack: Vec<FromContents>,
    free_projection_alias_index: u32,
    rng: ChaCha8Rng,
}

trait ExpressionNesting {
    /// nest l-value if needed
    fn p_nest_l(self, parent_priority: i32) -> Self;
    /// nest r-value if needed
    fn p_nest_r(self, parent_priority: i32) -> Self;
}

trait ExpressionPriority: ExpressionNesting {
    fn get_priority(&self) -> i32;
    fn nest_children_if_needed(self) -> Expr;
}

impl ExpressionNesting for Vec<Expr> {
    fn p_nest_l(self, parent_priority: i32) -> Vec<Expr> {
        self.into_iter().map(|expr| expr.p_nest_l(parent_priority)).collect()
    }

    fn p_nest_r(self, parent_priority: i32) -> Vec<Expr> {
        self.into_iter().map(|expr| expr.p_nest_r(parent_priority)).collect()
    }
}

impl ExpressionNesting for Vec<Vec<Expr>> {
    fn p_nest_l(self, parent_priority: i32) -> Vec<Vec<Expr>> {
        self.into_iter().map(|expr| expr.p_nest_l(parent_priority)).collect()
    }

    fn p_nest_r(self, parent_priority: i32) -> Vec<Vec<Expr>> {
        self.into_iter().map(|expr| expr.p_nest_r(parent_priority)).collect()
    }
}

impl ExpressionNesting for Box<Expr> {
    fn p_nest_l(self, parent_priority: i32) -> Box<Expr> {
        Box::new((*self).p_nest_l(parent_priority))
    }

    fn p_nest_r(self, parent_priority: i32) -> Box<Expr> {
        Box::new((*self).p_nest_r(parent_priority))
    }
}

impl ExpressionNesting for Option<Box<Expr>> {
    fn p_nest_l(self, parent_priority: i32) -> Option<Box<Expr>> {
        self.map(|expr| expr.p_nest_l(parent_priority))
    }

    fn p_nest_r(self, parent_priority: i32) -> Option<Box<Expr>> {
        self.map(|expr| expr.p_nest_r(parent_priority))
    }
}

impl ExpressionNesting for Expr {
    fn p_nest_l(self, parent_priority: i32) -> Expr {
        if self.get_priority() > parent_priority {
            Expr::Nested(Box::new(self))
        } else {
            self
        }
    }

    fn p_nest_r(self, parent_priority: i32) -> Expr {
        if self.get_priority() >= parent_priority {
            Expr::Nested(Box::new(self))
        } else {
            self
        }
    }
}

impl ExpressionPriority for Expr {
    fn get_priority(&self) -> i32 {
        match self {
            // no nesting, not of children nor of ourselves is needed
            Expr::Function(..) => -1,
            Expr::Nested(..) => -1,
            Expr::Value(..) => -1,
            Expr::Identifier(..) => -1,
            Expr::AnyOp(..) => -1,
            Expr::AllOp(..) => -1,
            Expr::Extract { .. } => -1,
            Expr::Position { .. } => -1,
            Expr::Substring { .. } => -1,
            Expr::Trim { .. } => -1,
            Expr::TryCast { .. } => -1,
            Expr::SafeCast { .. } => -1,
            Expr::Subquery(..) => -1,
            Expr::ListAgg(..) => -1,
            Expr::Tuple(..) => -1,
            Expr::Array(..) => -1,
            Expr::Ceil { .. } => -1,
            Expr::Floor { .. } => -1,
            Expr::Overlay { .. } => -1,
            Expr::ArrayAgg(..) => -1,
            Expr::ArraySubquery(..) => -1,
            Expr::MatchAgainst { .. } => -1,
            Expr::Case { .. } => -1,
            Expr::GroupingSets(..) => -1,
            Expr::Cube(..) => -1,
            Expr::Rollup(..) => -1,
            Expr::AggregateExpressionWithFilter { .. } => -1,

            // normal operations
            Expr::CompoundIdentifier(..) => 0,
            Expr::CompositeAccess { .. } => 0,
            Expr::Cast { .. } => 1,  // can be a ::
            Expr::ArrayIndex { .. } => 2,
            Expr::MapAccess { .. } => 2,
            Expr::UnaryOp { op, .. } => {
                match op {
                    UnaryOperator::Plus => 3,
                    UnaryOperator::Minus => 3,
                    UnaryOperator::PGBitwiseNot => 7,
                    UnaryOperator::PGSquareRoot => 7,
                    UnaryOperator::PGCubeRoot => 7,
                    UnaryOperator::PGPostfixFactorial => 7,
                    UnaryOperator::PGPrefixFactorial => 7,
                    UnaryOperator::PGAbs => 7,
                    UnaryOperator::Not => 11,
                }
            },
            Expr::BinaryOp { op, .. } => {
                match op {
                    BinaryOperator::PGExp => 4,
                    BinaryOperator::Multiply => 5,
                    BinaryOperator::Divide => 5,
                    BinaryOperator::Modulo => 5,
                    BinaryOperator::Plus => 6,
                    BinaryOperator::Minus => 6,
                    BinaryOperator::StringConcat => 7,
                    BinaryOperator::Spaceship => 7,
                    BinaryOperator::Xor => 7,
                    BinaryOperator::BitwiseOr => 7,
                    BinaryOperator::BitwiseAnd => 7,
                    BinaryOperator::BitwiseXor => 7,
                    BinaryOperator::PGBitwiseXor => 7,
                    BinaryOperator::PGBitwiseShiftLeft => 7,
                    BinaryOperator::PGBitwiseShiftRight => 7,
                    BinaryOperator::PGRegexMatch => 7,
                    BinaryOperator::PGRegexIMatch => 7,
                    BinaryOperator::PGRegexNotMatch => 7,
                    BinaryOperator::PGRegexNotIMatch => 7,
                    BinaryOperator::PGCustomBinaryOperator(..) => 7,
                    BinaryOperator::Gt => 9,
                    BinaryOperator::Lt => 9,
                    BinaryOperator::GtEq => 9,
                    BinaryOperator::LtEq => 9,
                    BinaryOperator::Eq => 9,
                    BinaryOperator::NotEq => 9,
                    BinaryOperator::And => 12,
                    BinaryOperator::Or => 13,
                }
            },
            Expr::Like { .. } => 8,
            Expr::ILike { .. } => 8,
            Expr::Between { .. } => 8,
            Expr::InList { .. } => 8,
            Expr::InSubquery { .. } => 8,
            Expr::InUnnest { .. } => 8,
            Expr::SimilarTo { .. } => 8,
            Expr::IsFalse(..) => 10,
            Expr::IsTrue(..) => 10,
            Expr::IsNull(..) => 10,
            Expr::IsNotNull(..) => 10,
            Expr::IsDistinctFrom(_, _) => 10,
            Expr::IsNotDistinctFrom(_, _) => 10,
            Expr::IsNotFalse(..) => 10,
            Expr::IsNotTrue(..) => 10,
            Expr::IsUnknown(..) => 10,
            Expr::IsNotUnknown(..) => 10,
            // EXISTS needs nesting possibly because of NOT, thus inherits its priority
            Expr::Exists { negated, .. } => if *negated {11} else {-1},
            Expr::JsonAccess { .. } => 7,
            Expr::Collate { .. } => 7,
            Expr::TypedString { .. } => 7,
            Expr::AtTimeZone { .. } => 7,
            Expr::IntroducedString { .. } => 7,
            Expr::Interval { .. } => 7,
        }
    }

    /// adds nesting to child if needed
    fn nest_children_if_needed(self) -> Expr {
        let parent_priority = self.get_priority();
        if parent_priority == -1 {
            return self
        }
        match self {
            Expr::CompoundIdentifier(ident_vec) => Expr::CompoundIdentifier(ident_vec),
            Expr::CompositeAccess { expr, key } => Expr::CompositeAccess { expr: expr.p_nest_l(parent_priority), key },
            Expr::Cast { expr, data_type} => Expr::Cast { expr: expr.p_nest_l(parent_priority), data_type},
            Expr::ArrayIndex { obj, indexes } => Expr::ArrayIndex { obj: obj.p_nest_l(parent_priority), indexes },
            Expr::MapAccess { column, keys } => Expr::MapAccess { column: column.p_nest_l(parent_priority), keys },
            Expr::UnaryOp { op, expr } => {
                if op == UnaryOperator::Not {
                    if let Expr::Exists { subquery: _, negated: _ } = *expr {
                        // exists will just be negated by the parser otherwise
                        return Expr::UnaryOp { op, expr: Box::new(Expr::Nested(expr)) }
                    }
                }
                if op == UnaryOperator::Minus {
                    if let Expr::UnaryOp { op: op2, expr: _ } = *expr {
                        if op2 == UnaryOperator::Minus {
                            // -- is a comment in SQL.
                            return Expr::UnaryOp { op, expr: Box::new(Expr::Nested(expr)) }
                        }
                    }
                }
                Expr::UnaryOp { op, expr: expr.p_nest_r(parent_priority) }  // p_nest_r is because unary operations are prefix ones.
            },
            Expr::BinaryOp { left, op, right } => Expr::BinaryOp { left: left.p_nest_l(parent_priority), op, right: right.p_nest_r(parent_priority) },
            Expr::Like { negated, expr, pattern, escape_char } => Expr::Like { negated, expr: expr.p_nest_l(parent_priority), pattern: pattern.p_nest_r(parent_priority), escape_char },
            Expr::ILike { negated, expr, pattern, escape_char } => Expr::ILike { negated, expr: expr.p_nest_l(parent_priority), pattern: pattern.p_nest_r(parent_priority), escape_char },
            Expr::Between { expr, negated, low, high } => Expr::Between { expr: expr.p_nest_l(parent_priority), negated, low: low.p_nest_r(parent_priority), high: high.p_nest_r(parent_priority) },
            Expr::InList { expr, list, negated } => Expr::InList { expr: expr.p_nest_l(parent_priority), list, negated },
            Expr::InSubquery { expr, subquery, negated } => Expr::InSubquery { expr: expr.p_nest_l(parent_priority), subquery, negated },
            Expr::InUnnest { expr, array_expr, negated } => Expr::InUnnest { expr: expr.p_nest_l(parent_priority), array_expr: array_expr, negated },
            Expr::SimilarTo { negated, expr, pattern, escape_char } => Expr::SimilarTo { negated, expr: expr.p_nest_l(parent_priority), pattern: pattern.p_nest_r(parent_priority), escape_char },
            Expr::IsFalse(expr) => Expr::IsFalse(expr.p_nest_l(parent_priority)),
            Expr::IsTrue(expr) => Expr::IsTrue(expr.p_nest_l(parent_priority)),
            Expr::IsNull(expr) => Expr::IsNull(expr.p_nest_l(parent_priority)),
            Expr::IsNotNull(expr) => Expr::IsNotNull(expr.p_nest_l(parent_priority)),
            Expr::IsDistinctFrom(expr1, expr2) => Expr::IsDistinctFrom(expr1.p_nest_l(parent_priority), expr2.p_nest_r(parent_priority)),
            Expr::IsNotDistinctFrom(expr1, expr2) => Expr::IsNotDistinctFrom(expr1.p_nest_l(parent_priority), expr2.p_nest_r(parent_priority)),
            Expr::IsNotFalse(expr) => Expr::IsNotFalse(expr.p_nest_l(parent_priority)),
            Expr::IsNotTrue(expr) => Expr::IsNotTrue(expr.p_nest_l(parent_priority)),
            Expr::IsUnknown(expr) => Expr::IsUnknown(expr.p_nest_l(parent_priority)),
            Expr::IsNotUnknown(expr) => Expr::IsNotUnknown(expr.p_nest_l(parent_priority)),
            Expr::JsonAccess { left, operator, right } => Expr::JsonAccess { left: left.p_nest_l(parent_priority), operator, right: right.p_nest_r(parent_priority) },
            Expr::Collate { expr, collation } => Expr::Collate { expr: expr.p_nest_l(parent_priority), collation },
            Expr::TypedString { data_type, value } => Expr::TypedString { data_type, value },
            Expr::AtTimeZone { timestamp, time_zone } => Expr::AtTimeZone { timestamp: timestamp.p_nest_l(parent_priority), time_zone },
            Expr::IntroducedString { introducer, value } => Expr::IntroducedString { introducer, value },
            Expr::Interval { value, leading_field, leading_precision, last_field, fractional_seconds_precision } => Expr::Interval { value: value.p_nest_l(parent_priority), leading_field, leading_precision, last_field, fractional_seconds_precision },
            any => any
        }
    }
}

impl<DynMod: DynamicModel, StC: StateChooser> QueryGenerator<DynMod, StC> {
    pub fn from_state_generator_and_schema(state_generator: MarkovChainGenerator<StC>, schema_source: &str) -> Self {
        QueryGenerator::<DynMod, StC> {
            state_generator,
            dynamic_model: Box::new(DynMod::new()),
            database_schema: DatabaseSchema::parse_schema(schema_source),
            from_contents_stack: vec![],
            free_projection_alias_index: 1,
            rng: ChaCha8Rng::seed_from_u64(1),
        }
    }

    fn next_state_opt(&mut self) -> Option<SmolStr> {
        self.state_generator.next(&mut *self.dynamic_model)
    }

    fn next_state(&mut self) -> SmolStr {
        self.next_state_opt().unwrap()
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

    fn expect_compat(&self, target: &SubgraphType, compat_with: &SubgraphType) {
        if !target.is_compat_with(compat_with) {
            self.state_generator.print_stack();
            panic!("Incompatible types: expected compatible with {:?}, got {:?}", compat_with, target);
        }
    }

    pub fn gen_select_alias(&mut self) -> ObjectName {
        let name = format!("C{}", self.free_projection_alias_index);
        self.free_projection_alias_index += 1;
        ObjectName(vec![Ident { value: name.clone(), quote_style: None }])
    }

    /// subgraph def_Query
    fn handle_query(&mut self) -> (Query, Vec<(Option<ObjectName>, SubgraphType)>) {
        self.dynamic_model.notify_subquery_creation_begin();
        self.from_contents_stack.push(FromContents::new());
        self.expect_state("Query");

        let select_limit = match self.next_state().as_str() {
            "single_value_true" => {
                self.expect_state("FROM");
                Some(Expr::Value(Value::Number("1".to_string(), false)))
            },
            "single_value_false" => {
                match self.next_state().as_str() {
                    "limit" => {
                        self.expect_state("call52_types");
                        let num = self.handle_types(Some(SubgraphType::Numeric), None).1;
                        self.expect_state("FROM");
                        Some(num)
                    },
                    "FROM" => None,
                    any => self.panic_unexpected(any)
                }
            },
            any => self.panic_unexpected(any)
        };

        let mut select_body = Select {
            distinct: false,
            top: None,
            projection: vec![],
            into: None,
            from: vec![],
            lateral_views: vec![],
            selection: None,
            group_by: vec![],
            cluster_by: vec![],
            distribute_by: vec![],
            sort_by: vec![],
            having: None,
            qualify: None,
        };

        loop {
            select_body.from.push(TableWithJoins { relation: match self.next_state().as_str() {
                "Table" => {
                    let create_table_st = self.database_schema.get_random_table_def(&mut self.rng);
                    let alias = self.from_contents_stack.last_mut().unwrap().append_table(create_table_st);
                    TableFactor::Table {
                        name: create_table_st.name.clone(),
                        alias: Some(alias),
                        args: None,
                        with_hints: vec![],
                        columns_definition: None,
                    }
                },
                "call0_Query" => {
                    let (query, column_idents_and_graph_types) = self.handle_query();
                    let alias = self.from_contents_stack.last_mut().unwrap().append_query(column_idents_and_graph_types);
                    TableFactor::Derived {
                        lateral: false,
                        subquery: Box::new(query),
                        alias: Some(alias)
                    }
                },
                "EXIT_FROM" => break,
                any => self.panic_unexpected(any)
            }, joins: vec![] });
            self.expect_state("FROM_multiple_relations");
        }

        match self.next_state().as_str() {
            "WHERE" => {
                self.expect_state("call53_types");
                select_body.selection = Some(self.handle_types(Some(SubgraphType::Val3), None).1);
                self.expect_state("EXIT_WHERE");
            },
            "EXIT_WHERE" => {},
            any => self.panic_unexpected(any)
        }

        self.expect_state("SELECT");
        select_body.distinct = match self.next_state().as_str() {
            "SELECT_DISTINCT" => {
                self.expect_state("SELECT_distinct_end");
                true
            },
            "SELECT_distinct_end" => false,
            any => self.panic_unexpected(any)
        };

        let mut column_idents_and_graph_types = vec![];

        self.expect_state("SELECT_projection");
        while match self.next_state().as_str() {
            "SELECT_list" => true,
            "SELECT_list_multiple_values_single_value_false" => {
                self.expect_state("SELECT_list");
                true
            },
            "EXIT_SELECT" => false,
            any => self.panic_unexpected(any)
        } {
            match self.next_state().as_str() {
                "SELECT_wildcard" => {
                    column_idents_and_graph_types = [
                        column_idents_and_graph_types,
                        self.from_contents_stack
                            .last()
                            .unwrap()
                            .get_wildcard_columns()
                            .into_iter()
                            .map(|x| (Some(x.0), x.1))
                            .collect::<Vec<_>>()
                    ].concat();
                    select_body.projection.push(SelectItem::Wildcard(WildcardAdditionalOptions {
                        opt_exclude: None, opt_except: None, opt_rename: None,
                    }));
                    continue;
                },
                "SELECT_qualified_wildcard" => {
                    let from_contents = self.from_contents_stack.last().unwrap();
                    let (alias, relation) = from_contents.get_random_relation(&mut self.rng);
                    column_idents_and_graph_types = [
                        column_idents_and_graph_types,
                        relation
                            .get_columns_with_types()
                            .into_iter()
                            .map(|x| (Some(x.0), x.1))
                            .collect::<Vec<_>>()
                    ].concat();
                    select_body.projection.push(SelectItem::QualifiedWildcard(
                        alias.to_owned(),
                        WildcardAdditionalOptions {
                            opt_exclude: None, opt_except: None, opt_rename: None,
                        }
                    ));
                },
                arm @ ("SELECT_unnamed_expr" | "SELECT_expr_with_alias") => {
                    self.expect_state("call54_types");
                    let (subgraph_type, expr) = self.handle_types(None, None);
                    let (alias, select_item) = match arm {
                        "SELECT_unnamed_expr" => (
                            None, SelectItem::UnnamedExpr(expr)
                        ),
                        "SELECT_expr_with_alias" => {
                            let select_alias = self.gen_select_alias();
                            (Some(select_alias.clone()), SelectItem::ExprWithAlias {
                                expr, alias: select_alias.0[0].clone(),
                            })
                        },
                        any => self.panic_unexpected(any)
                    };
                    select_body.projection.push(select_item);
                    column_idents_and_graph_types.push((alias, subgraph_type));
                },
                any => self.panic_unexpected(any)
            };
            self.expect_state("SELECT_list_multiple_values");
        }

        self.expect_state("EXIT_Query");
        self.dynamic_model.notify_subquery_creation_end();
        self.from_contents_stack.pop();
        (Query {
            with: None,
            body: Box::new(SetExpr::Select(Box::new(select_body))),
            order_by: vec![],
            limit: select_limit,
            offset: None,
            fetch: None,
            locks: vec![],
        }, column_idents_and_graph_types)
    }

    /// subgraph def_VAL_3
    fn handle_val_3(&mut self) -> Expr {
        self.expect_state("VAL_3");
        let val3 = match self.next_state().as_str() {
            "IsNull" => {
                let is_null_not_flag = match self.next_state().as_str() {
                    "IsNull_not" => {
                        self.expect_state("call0_types_all");
                        true
                    }
                    "call0_types_all" => false,
                    any => self.panic_unexpected(any)
                };
                let types_value = Box::new(self.handle_types_all().1);
                if is_null_not_flag {
                    Expr::IsNotNull(types_value)
                } else {
                    Expr::IsNull(types_value)
                }
            },
            "IsDistinctFrom" => {
                self.expect_state("call1_types_all");
                let (types_selected_type, types_value_1) = self.handle_types_all();
                self.state_generator.push_compatible_list(types_selected_type.get_compat_types());
                let is_distinct_not_flag = match self.next_state().as_str() {
                    "IsDistinctNOT" => {
                        self.expect_state("DISTINCT");
                        true
                    }
                    "DISTINCT" => false,
                    any => self.panic_unexpected(any)
                };
                self.expect_state("call21_types");
                let types_value_2 = self.handle_types(None, Some(types_selected_type)).1;
                if is_distinct_not_flag {
                    Expr::IsNotDistinctFrom(Box::new(types_value_1), Box::new(types_value_2))
                } else {
                    Expr::IsDistinctFrom(Box::new(types_value_1), Box::new(types_value_2))
                }
            },
            "Exists" => {
                let exists_not_flag = match self.next_state().as_str() {
                    "Exists_not" => {
                        self.expect_state("call2_Query");
                        true
                    },
                    "call2_Query" => false,
                    any => self.panic_unexpected(any)
                };
                Expr::Exists {
                    subquery: Box::new(self.handle_query().0),
                    negated: exists_not_flag
                }
            },
            "InList" => {
                self.expect_state("call2_types_all");
                let (types_selected_type, types_value) = self.handle_types_all();
                self.state_generator.push_compatible_list(types_selected_type.get_compat_types());
                let in_list_not_flag = match self.next_state().as_str() {
                    "InListNot" => {
                        self.expect_state("InListIn");
                        true
                    },
                    "InListIn" => false,
                    any => self.panic_unexpected(any)
                };
                self.expect_state("call1_list_expr");
                Expr::InList {
                    expr: Box::new(types_value),
                    list: unwrap_variant!(self.handle_list_expr(), Expr::Tuple),
                    negated: in_list_not_flag
                }
            },
            "InSubquery" => {
                self.expect_state("call3_types_all");
                let (types_selected_type, types_value) = self.handle_types_all();
                self.state_generator.push_compatible_list(types_selected_type.get_compat_types());
                let in_subquery_not_flag = match self.next_state().as_str() {
                    "InSubqueryNot" => {
                        self.expect_state("InSubqueryIn");
                        true
                    },
                    "InSubqueryIn" => false,
                    any => self.panic_unexpected(any)
                };
                self.expect_state("call3_Query");
                let query = self.handle_query().0;
                Expr::InSubquery {
                    expr: Box::new(types_value),
                    subquery: Box::new(query),
                    negated: in_subquery_not_flag
                }
            },
            "Between" => {
                self.expect_state("call4_types_all");
                let (types_selected_type, types_value_1) = self.handle_types_all();
                let type_names = types_selected_type.get_compat_types();
                let between_not_flag = match self.next_state().as_str() {
                    "BetweenBetweenNot" => {
                        self.expect_state("BetweenBetween");
                        true
                    },
                    "BetweenBetween" => false,
                    any => self.panic_unexpected(any)
                };
                self.state_generator.push_compatible_list(type_names.clone());
                self.expect_state("call22_types");
                let types_value_2 = self.handle_types(None, Some(types_selected_type.clone())).1;
                self.expect_state("BetweenBetweenAnd");
                self.state_generator.push_compatible_list(type_names);
                self.expect_state("call23_types");
                let types_value_3 = self.handle_types(None, Some(types_selected_type)).1;
                Expr::Between {
                    expr: Box::new(types_value_1),
                    negated: between_not_flag,
                    low: Box::new(types_value_2),
                    high: Box::new(types_value_3)
                }
            },
            "BinaryComp" => {
                self.expect_state("call5_types_all");
                let (types_selected_type, types_value_1) = self.handle_types_all();
                self.state_generator.push_compatible_list(types_selected_type.get_compat_types());
                let binary_comp_op = match self.next_state().as_str() {
                    "BinaryCompEqual" => BinaryOperator::Eq,
                    "BinaryCompLess" => BinaryOperator::Lt,
                    "BinaryCompLessEqual" => BinaryOperator::LtEq,
                    "BinaryCompUnEqual" => BinaryOperator::NotEq,
                    any => self.panic_unexpected(any)
                };
                self.expect_state("call24_types");
                let types_value_2 = self.handle_types(None, Some(types_selected_type)).1;
                Expr::BinaryOp {
                    left: Box::new(types_value_1),
                    op: binary_comp_op,
                    right: Box::new(types_value_2)
                }
            },
            "AnyAll" => {
                self.expect_state("call6_types_all");
                let (types_selected_type, types_value) = self.handle_types_all();
                self.expect_state("AnyAllSelectOp");
                let any_all_op = match self.next_state().as_str() {
                    "AnyAllEqual" => BinaryOperator::Eq,
                    "AnyAllLess" => BinaryOperator::Lt,
                    "AnyAllLessEqual" => BinaryOperator::LtEq,
                    "AnyAllUnEqual" => BinaryOperator::NotEq,
                    any => self.panic_unexpected(any)
                };
                self.expect_state("AnyAllSelectIter");
                self.state_generator.push_compatible_list(types_selected_type.get_compat_types());
                let iterable = Box::new(match self.next_state().as_str() {
                    "call4_Query" => Expr::Subquery(Box::new(self.handle_query().0)),
                    "call1_array" => self.handle_array(),
                    any => self.panic_unexpected(any)
                });
                self.expect_state("AnyAllAnyAll");
                let iterable = Box::new(match self.next_state().as_str() {
                    "AnyAllAnyAllAll" => Expr::AllOp(iterable),
                    "AnyAllAnyAllAny" => Expr::AnyOp(iterable),
                    any => self.panic_unexpected(any),
                });
                Expr::BinaryOp {
                    left: Box::new(types_value),
                    op: any_all_op,
                    right: iterable,
                }
            },
            "BinaryStringLike" => {
                self.expect_state("call25_types");
                let types_value_1 = self.handle_types(Some(SubgraphType::String), None).1;
                let string_like_not_flag = match self.next_state().as_str() {
                    "BinaryStringLikeNot" => {
                        self.expect_state("BinaryStringLikeIn");
                        true
                    }
                    "BinaryStringLikeIn" => false,
                    any => self.panic_unexpected(any)
                };
                self.expect_state("call26_types");
                let types_value_2 = self.handle_types(Some(SubgraphType::String), None).1;
                Expr::Like {
                    negated: string_like_not_flag,
                    expr: Box::new(types_value_1),
                    pattern: Box::new(types_value_2),
                    escape_char: None
                }
            },
            "BinaryBooleanOpV3" => {
                self.expect_state("call27_types");
                let types_value_1 = self.handle_types(Some(SubgraphType::Val3), None).1;
                let binary_bool_op = match self.next_state().as_str() {
                    "BinaryBooleanOpV3AND" => BinaryOperator::And,
                    "BinaryBooleanOpV3OR" => BinaryOperator::Or,
                    "BinaryBooleanOpV3XOR" => BinaryOperator::Xor,
                    any => self.panic_unexpected(any)
                };
                self.expect_state("call28_types");
                let types_value_2 = self.handle_types(Some(SubgraphType::Val3), None).1;
                Expr::BinaryOp {
                    left: Box::new(types_value_1),
                    op: binary_bool_op,
                    right: Box::new(types_value_2)
                }
            },
            "true" => Expr::Value(Value::Boolean(true)),
            "false" => Expr::Value(Value::Boolean(false)),
            "Nested_VAL_3" => {
                self.expect_state("call29_types");
                Expr::Nested(Box::new(self.handle_types(Some(SubgraphType::Val3), None).1))
            },
            "UnaryNot_VAL_3" => {
                self.expect_state("call30_types");
                Expr::UnaryOp { op: UnaryOperator::Not, expr: Box::new( self.handle_types(
                    Some(SubgraphType::Val3), None
                ).1) }
            },
            any => self.panic_unexpected(any)
        };
        self.expect_state("EXIT_VAL_3");
        val3
    }

    /// subgraph def_numeric
    fn handle_numeric(&mut self) -> Expr {
        self.expect_state("numeric");
        let numeric = match self.next_state().as_str() {
            "numeric_literal" => {
                Expr::Value(Value::Number(match self.next_state().as_str() {
                    "numeric_literal_float" => {
                        "3.1415"  // TODO: hardcode
                    },
                    "numeric_literal_int" => {
                        "3"       // TODO: hardcode
                    },
                    any => self.panic_unexpected(any)
                }.to_string(), false))
            },
            "BinaryNumericOp" => {
                self.expect_state("call48_types");
                let types_value_1 = self.handle_types(Some(SubgraphType::Numeric), None).1;
                let numeric_binary_op = match self.next_state().as_str() {
                    "binary_numeric_bin_and" => BinaryOperator::BitwiseAnd,
                    "binary_numeric_bin_or" => BinaryOperator::BitwiseOr,
                    "binary_numeric_bin_xor" => BinaryOperator::PGBitwiseXor,  // BitwiseXor is exponentiation
                    "binary_numeric_div" => BinaryOperator::Divide,
                    "binary_numeric_minus" => BinaryOperator::Minus,
                    "binary_numeric_mul" => BinaryOperator::Multiply,
                    "binary_numeric_plus" => BinaryOperator::Plus,
                    any => self.panic_unexpected(any),
                };
                self.expect_state("call47_types");
                let types_value_2 = self.handle_types(Some(SubgraphType::Numeric), None).1;
                Expr::BinaryOp {
                    left: Box::new(types_value_1),
                    op: numeric_binary_op,
                    right: Box::new(types_value_2)
                }
            },
            "UnaryNumericOp" => {
                let numeric_unary_op = match self.next_state().as_str() {
                    "unary_numeric_abs" => UnaryOperator::PGAbs,
                    "unary_numeric_bin_not" => UnaryOperator::PGBitwiseNot,
                    "unary_numeric_cub_root" => UnaryOperator::PGCubeRoot,
                    "unary_numeric_minus" => UnaryOperator::Minus,
                    "unary_numeric_plus" => UnaryOperator::Plus,
                    // "unary_numeric_postfix_fact" => UnaryOperator::PGPostfixFactorial,
                    // "unary_numeric_prefix_fact" => UnaryOperator::PGPrefixFactorial,  // THESE 2 WERE REMOVED FROM POSTGRESQL
                    "unary_numeric_sq_root" => UnaryOperator::PGSquareRoot,
                    any => self.panic_unexpected(any),
                };
                self.expect_state("call1_types");
                let types_value = self.handle_types(Some(SubgraphType::Numeric), None).1;
                Expr::UnaryOp {
                    op: numeric_unary_op,
                    expr: Box::new(types_value)
                }
            },
            "numeric_string_Position" => {
                self.expect_state("call2_types");
                let types_value_1 = self.handle_types(Some(SubgraphType::String), None).1;
                self.expect_state("string_position_in");
                self.expect_state("call3_types");
                let types_value_2 = self.handle_types(Some(SubgraphType::String), None).1;
                Expr::Position {
                    expr: Box::new(types_value_1),
                    r#in: Box::new(types_value_2)
                }
            },
            "Nested_numeric" => {
                self.expect_state("call4_types");
                let types_value = self.handle_types(Some(SubgraphType::Numeric), None).1;
                Expr::Nested(Box::new(types_value))
            },
            any => self.panic_unexpected(any)
        };
        self.expect_state("EXIT_numeric");
        numeric
    }

    /// subgraph def_string
    fn handle_string(&mut self) -> Expr {
        self.expect_state("string");
        let string = match self.next_state().as_str() {
            "string_literal" => Expr::Value(Value::SingleQuotedString("HJeihfbwei".to_string())),  // TODO: hardcoded
            "string_trim" => {
                let (trim_where, trim_what) = match self.next_state().as_str() {
                    "call6_types" => {
                        let types_value = self.handle_types(Some(SubgraphType::String), None).1;
                        let spec_mode = match self.next_state().as_str() {
                            "BOTH" => TrimWhereField::Both,
                            "LEADING" => TrimWhereField::Leading,
                            "TRAILING" => TrimWhereField::Trailing,
                            any => self.panic_unexpected(any)
                        };
                        self.expect_state("call5_types");
                        (Some(spec_mode), Some(Box::new(types_value)))
                    },
                    "call5_types" => (None, None),
                    any => self.panic_unexpected(any)
                };
                let types_value = self.handle_types(Some(SubgraphType::String), None).1;
                Expr::Trim {
                    expr: Box::new(types_value), trim_where, trim_what
                }
            },
            "string_concat" => {
                self.expect_state("call7_types");
                let types_value_1 = self.handle_types(Some(SubgraphType::String), None).1;
                self.expect_state("string_concat_concat");
                self.expect_state("call8_types");
                let types_value_2 = self.handle_types(Some(SubgraphType::String), None).1;
                Expr::BinaryOp {
                    left: Box::new(types_value_1),
                    op: BinaryOperator::StringConcat,
                    right: Box::new(types_value_2)
                }
            },
            any => self.panic_unexpected(any)
        };
        self.expect_state("EXIT_string");
        string
    }

    /// subgraph def_types
    fn handle_types(
        &mut self, equal_to: Option<SubgraphType>, compatible_with: Option<SubgraphType>
    ) -> (SubgraphType, Expr) {
        self.expect_state("types");
        let selected_type = match self.next_state().as_str() {
            "types_select_type_3vl" => SubgraphType::Val3,
            "types_select_type_array" => SubgraphType::Array,
            "types_select_type_list_expr" => SubgraphType::ListExpr,
            "types_select_type_numeric" => SubgraphType::Numeric,
            "types_select_type_string" => SubgraphType::String,
            "types_null" => {
                self.expect_state("EXIT_types");
                return (SubgraphType::Null, Expr::Value(Value::Null))
            },
            any => self.panic_unexpected(any),
        };
        let types_value = match self.next_state().as_str() {
            "types_select_type_noexpr" => {
                match self.next_state().as_str() {
                    "call0_column_spec" => {
                        self.state_generator.push_known(selected_type.clone());
                        self.handle_column_spec()
                    },
                    "call1_Query" => {
                        self.state_generator.push_known_list(vec![selected_type.clone()]);
                        Expr::Subquery(Box::new(self.handle_query().0))
                    },
                    any => self.panic_unexpected(any)
                }
            },
            "call0_numeric" if selected_type == SubgraphType::Numeric => self.handle_numeric(),
            "call1_VAL_3" if selected_type == SubgraphType::Val3 => self.handle_val_3(),
            "call0_string" if selected_type == SubgraphType::String => self.handle_string(),
            "call0_list_expr" if selected_type == SubgraphType::ListExpr => self.handle_list_expr(),
            "call0_array" if selected_type == SubgraphType::Array => self.handle_array(),
            any => self.panic_unexpected(any)
        };
        self.expect_state("EXIT_types");
        if let Some(to) = equal_to {
            if selected_type != to {
                panic!("Unexpected type: expected {:?}, got {:?}", to, selected_type);
            }
        }
        if let Some(with) = compatible_with {
            self.expect_compat(&selected_type, &with);
        }
        (selected_type, types_value.nest_children_if_needed())
    }

    /// subgraph def_types_all
    fn handle_types_all(&mut self) -> (SubgraphType, Expr) {
        self.expect_state("types_all");
        self.expect_state("call0_types");
        let ret = self.handle_types(None, None);
        self.expect_state("EXIT_types_all");
        ret
    }

    /// subgraph def_column_spec
    fn handle_column_spec(&mut self) -> Expr {
        self.expect_state("column_spec");
        self.expect_state("typed_column_name");
        let ident_components = {
            let column_type = unwrap_variant_and_else!(
                self.state_generator.get_fn_selected_types(), CallTypes::Type, || self.state_generator.print_stack()
            );
            // TODO: this has 2 cases: a new column for a relation or a constrained column selection
            // Question: Should (ObejectName, vec of cols) be a separate struct that would have a
            // method performing the following action in dep of the type of Relation (rel/query)?
            self.from_contents_stack.last().unwrap().get_random_column_with_type(&mut self.rng, &column_type)
        };
        self.expect_state("EXIT_column_spec");
        Expr::CompoundIdentifier(ident_components)
    }

    /// subgraph def_array
    fn handle_array(&mut self) -> Expr {
        self.expect_state("array");
        let array_compat_type = match self.next_state().as_str() {
            "call12_types" => SubgraphType::Numeric,
            "call13_types" => SubgraphType::Val3,
            "call31_types" => SubgraphType::String,
            "call51_types" => SubgraphType::ListExpr,
            "call14_types" => SubgraphType::Array,
            any => self.panic_unexpected(any)
        };
        let types_value = self.handle_types(Some(array_compat_type.clone()), None).1;
        let mut array: Vec<Expr> = vec![types_value];
        loop {
            match self.next_state().as_str() {
                "call50_types" => {
                    self.state_generator.push_compatible_list(array_compat_type.get_compat_types());
                    let types_value = self.handle_types(None, Some(array_compat_type.clone())).1;
                    array.push(types_value);
                },
                "EXIT_array" => break,
                any => self.panic_unexpected(any)
            }
        }
        Expr::Array(Array {
            elem: array,
            named: true
        })
    }

    /// subgraph def_list_expr
    fn handle_list_expr(&mut self) -> Expr {
        self.expect_state("list_expr");
        let list_compat_type = match self.next_state().as_str() {
            "call16_types" => SubgraphType::Numeric,
            "call17_types" => SubgraphType::Val3,
            "call18_types" => SubgraphType::String,
            "call19_types" => SubgraphType::ListExpr,
            "call20_types" => SubgraphType::Array,
            any => self.panic_unexpected(any)
        };
        let types_value = self.handle_types(Some(list_compat_type.clone()), None).1;
        let mut list_expr: Vec<Expr> = vec![types_value];
        loop {
            match self.next_state().as_str() {
                "call49_types" => {
                    self.state_generator.push_compatible_list(list_compat_type.get_compat_types());
                    let types_value = self.handle_types(None, Some(list_compat_type.clone())).1;
                    list_expr.push(types_value);
                },
                "EXIT_list_expr" => break,
                any => self.panic_unexpected(any)
            }
        }
        Expr::Tuple(list_expr)
    }

    /// starting point; calls handle_query for the first time
    fn generate(&mut self) -> Query {
        let query = self.handle_query().0;
        println!("Relations:\n{}", self.database_schema);
        println!("Query:\n{}\n", query);
        // reset the generator
        if let Some(state) = self.next_state_opt() {
            panic!("Couldn't reset state_generator: Received {state}");
        }
        self.dynamic_model = Box::new(DynMod::new());
        query
    }
}

impl<DynMod: DynamicModel, StC: StateChooser> Iterator for QueryGenerator<DynMod, StC> {
    type Item = Query;

    fn next(&mut self) -> Option<Self::Item> {
        Some(self.generate())
    }
}