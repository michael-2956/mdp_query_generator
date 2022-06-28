pub struct QueryGeneratorParams {
    /// max number of tables (counting repetitions) mentioned
    /// in a well-defined SELECT-FROM-WHERE block, including
    /// nested subqueries
    pub max_total_tables: u32,
    /// max level of nested queries in FROM and WHERE
    pub max_nest_level: u32,
    /// max number of attributes in a SELECT clause
    pub max_select_attrs: u32,
    /// max number of atomic conditions in WHERE
    pub max_where_conds: u32,
    /// probability of generating the (n+1)th table after nth
    /// table in the FROM clause
    pub from_next_table_prob: f64,
    /// probability of generating an alias for a table in the
    /// FROM clause.
    pub from_table_alias_prob: f64,
    /// probability of nesting a query in the FROM clause.
    pub from_nest_prob: f64,
    /// probability of DISTINCT appearing in a SELECT clause
    pub distinct_prob: f64,
    /// probability of generating * in SELECT
    pub select_asterisk_prob: f64,
    /// probability of generating (n+1)th attribute after
    /// generating the nth one in SELECT
    pub select_next_attr_prob: f64,
    /// probability of generating a subcondition after
    /// generating a condition on WHERE
    pub where_subcond_prob: f64,
    /// probability of nesting a subquery instead of an atomic
    /// condition when generating a subcondition in WHERE
    pub where_nest_prob: f64,
    /// probability of generating an ISNULL around a condition
    /// in WHERE
    pub where_isull_prob: f64,
    /// probability of negating a condition in WHERE
    pub where_negation_prob: f64,
}

impl Default for QueryGeneratorParams {
    fn default() -> Self {
        Self {
            max_total_tables: 6,
            max_nest_level: 3,
            max_select_attrs: 3,
            max_where_conds: 8,
            from_next_table_prob: 0.1,
            from_table_alias_prob: 0.5,
            from_nest_prob: 0.5,
            distinct_prob: 0.4,
            select_asterisk_prob: 0.1,
            select_next_attr_prob: 0.2,
            where_subcond_prob: 0.3,
            where_nest_prob: 0.5,
            where_isull_prob: 0.2,
            where_negation_prob: 0.5,
        }
    }
}

pub struct QueryGenerator {
    params: QueryGeneratorParams,
}

impl QueryGenerator {
    fn new(params: QueryGeneratorParams) -> Self {
        QueryGenerator { params: params }
    }
}
