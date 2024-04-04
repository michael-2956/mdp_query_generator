use std::fmt::{Debug, Display};
use std::fs::File;
use std::collections::{BTreeMap, HashMap};
use std::io::{self, Write, Read};
use std::path::PathBuf;

use smol_str::SmolStr;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarkovWeights<WeightsType> {
    /// function name [+ params] -> from -> [(to, weight), ...]
    pub weights: WeightsType,
}

impl<FuncType> MarkovWeights<HashMap<FuncType, HashMap<SmolStr, HashMap<SmolStr, f64>>>>
where
    FuncType: Debug + Eq + std::hash::Hash + Clone + Serialize + for<'a> Deserialize<'a> + Display + Ord + PartialOrd
{
    pub fn new() -> Self {
        Self {
            weights: HashMap::new(),
        }
    }

    /// Makes all the weight sums equal to one,
    /// which is nessesary for them to be a probability distribution
    pub fn normalize(&mut self) {
        for (_, chain) in self.weights.iter_mut() {
            for (_, out) in chain {
                let prob_sum: f64 = out.into_iter().map(|(_, p)| *p).sum();
                for (_, weight) in out {
                    *weight = *weight / prob_sum;
                }
            }
        }
    }

    /// adds 1 to the specified edge, creates it if it is absent
    pub fn insert_edge(&mut self, func_name: FuncType, from: &SmolStr, to: &SmolStr) {
        let assign_to = self.weights
            .entry(func_name).or_insert(HashMap::new())
            .entry(from.clone()).or_insert(HashMap::new())
            .entry(to.clone()).or_insert(0f64);
        *assign_to += 1f64;
    }

    pub fn get_outgoing_weights_opt(&self, func_name: &FuncType, from: &SmolStr) -> Option<&HashMap<SmolStr, f64>> {
        self.weights
            .get(func_name).map(
                |f_w| f_w.get(from)
            )
            .filter(
                |o_w| o_w.is_some()
            )
            .map(
                |o_w| o_w.unwrap()
            )
    }

    pub fn write_to_file(&self, file_path: &PathBuf) -> io::Result<()> {
        let encoded: Vec<u8> = bincode::serialize(&self).unwrap();
        let mut file = File::create(file_path)?;
        file.write_all(&encoded)?;
        Ok(())
    }

    pub fn load(file_path: &PathBuf) -> io::Result<Self> {
        let mut file = File::open(file_path)?;
        let mut encoded = Vec::new();
        file.read_to_end(&mut encoded)?;
        let decoded: MarkovWeights<HashMap<FuncType, HashMap<SmolStr, HashMap<SmolStr, f64>>>> = bincode::deserialize(&encoded[..]).unwrap();
        Ok(decoded)
    }

    pub fn print_outgoing_weights(&self, func_name: &FuncType, from: &SmolStr) {
        for (to, weight) in self.weights
            .get(func_name).unwrap()
            .get(from).unwrap()
            .iter()
        {
            eprintln!("{to} -> {weight}");
        }
    }

    pub fn print_function_weights(&self, func_name: &FuncType) {
        for (from, out) in self.weights
            .get(func_name).unwrap()
        {
            eprintln!("{from}: ");
            for (to, weight) in out {
                eprintln!("    {weight} -> {to}");
            }
        }
    }

    pub fn write_to_dot(&self, dot_file_path: &PathBuf) -> io::Result<()> {
        let mut file = File::create(dot_file_path)?;
        writeln!(file, "digraph G {{")?;
        let weights_ordered = BTreeMap::from_iter(self.weights.iter());
        for (func_name, chain) in weights_ordered {
            writeln!(file, "    subgraph {func_name} {{")?;
            let chain_ordered = BTreeMap::from_iter(chain.iter());
            for (from, out) in chain_ordered {
                let out_ordered = BTreeMap::from_iter(out.iter());
                for (to, weight) in out_ordered {
                    writeln!(file, "        {func_name}_{from} -> {func_name}_{to} [label=\"  {weight:.4}\"]")?;
                }
            }
            writeln!(file, "    }}")?;
        }
        writeln!(file, "}}")?;
        Ok(())
    }

    pub fn print(&self) {
        for (func_name, chain) in self.weights.iter() {
            eprintln!("\n=====================================================\n{func_name}: ");
            for (from, out) in chain {
                eprintln!("    {from}: ");
                for (to, weight) in out {
                    eprintln!("        {weight} -> {to}");
                }
            }
        }
    }

}
