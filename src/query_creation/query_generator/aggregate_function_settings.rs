use std::path::PathBuf;
use std::collections::{HashMap, BTreeMap};
use crate::query_creation::state_generator::markov_chain_generator::subgraph_type::SubgraphType;
use sqlparser::ast::ObjectName;
use rand::distributions::WeightedIndex;
use std::fs;

#[derive(Debug, Clone, PartialEq, Eq, Ord, PartialOrd, Hash)]
pub enum AggregateFunctionAgruments {
    AnyType,
    Wildcard,
    TypeList(Vec<SubgraphType>),
}

#[derive(Debug, Clone)]
pub struct AggregateFunctionDistribution {
    /// return -> domain -> aggr name -> weight
    func_map: HashMap<SubgraphType, HashMap<AggregateFunctionAgruments, BTreeMap<String, f64>>>,
}

fn parse_type_list(mut types_str: String) -> AggregateFunctionAgruments {
    types_str = String::from(types_str.trim());
    if types_str.starts_with("[") && types_str.ends_with("]") {
        types_str = String::from(&types_str[1..types_str.len()-1]);
        AggregateFunctionAgruments::TypeList(types_str.split(",").map(
            |x| SubgraphType::from_type_name(x.trim()).unwrap()
        ).collect())
    } else {
        match types_str.as_str() {
            "any" => AggregateFunctionAgruments::AnyType,
            "*" => AggregateFunctionAgruments::Wildcard,
            any => panic!("Error parsing aggregate function JSON file!\nTypes must conform to format: \"[type1, type2, type3]\", \"*\" or \"any\" Got: {any}")
        }
    }
}

impl AggregateFunctionDistribution {
    pub fn parse_file(file_path: PathBuf) -> AggregateFunctionDistribution {
        let content: HashMap<String, HashMap<String, HashMap<String, f64>>> = serde_json::from_str(
            &fs::read_to_string(file_path).expect("Unable to read file")
        ).unwrap();
        
        let mut func_map: HashMap<SubgraphType, HashMap<AggregateFunctionAgruments, BTreeMap<String, f64>>> = HashMap::new();
        for (return_types_str, domain_map) in content {
            let return_types = SubgraphType::from_type_name(return_types_str.as_str()).unwrap();
            let mut domain_type_map: HashMap<AggregateFunctionAgruments, BTreeMap<String, f64>> = HashMap::new();
            for (domain_type_str, name_weight_map) in domain_map {
                let domain_types = parse_type_list(domain_type_str);
                domain_type_map.insert(domain_types, BTreeMap::from_iter(name_weight_map.into_iter().map(
                    |(f_name, w)| (f_name.to_uppercase(), w)
                )));
            }
            func_map.insert(return_types, domain_type_map);
        }

        AggregateFunctionDistribution {
            func_map,
        }
    }

    pub fn get_functions_and_dist(&self, arguments: &AggregateFunctionAgruments, return_type: &SubgraphType) -> (Vec<&String>, WeightedIndex<f64>) {
        let aggr_weight_map = &self.func_map[return_type][arguments];
        let dist = WeightedIndex::new(
            aggr_weight_map.iter().map(|item| *item.1)
        ).unwrap();
        (aggr_weight_map.keys().collect(), dist)
    }

    pub fn func_names_include(&mut self, arguments: &AggregateFunctionAgruments, return_type: &SubgraphType, aggr_name: &ObjectName) -> bool {
        self.func_map[return_type][arguments].contains_key(&aggr_name.0[0].value.to_uppercase())
    }
}