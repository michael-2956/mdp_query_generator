use crate::query_creation::state_generator::subgraph_type::SubgraphType;

#[derive(Clone, Debug, PartialEq)]
pub enum TypesContinueAfter {
    None,
    ExprType(SubgraphType),
}
