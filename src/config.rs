use std::path::PathBuf;

use structopt::StructOpt;
use toml::Value;

use crate::{programs::{semantic_correctness::SemanticCorrectnessConfig, syntax_coverage::SyntaxCoverageConfig}, query_creation::{query_generator::QueryGeneratorConfig, state_generator::{markov_chain_generator::StateGeneratorConfig, substitute_models::AntiCallModelConfig}}, training::{ast_to_path::AST2PathTestingConfig, models::ModelConfig, trainer::TrainingConfig}};

#[derive(StructOpt, Debug)]
#[structopt(name = "basic")]
pub struct ProgramArgs {
    /// config file
    #[structopt(parse(from_os_str), default_value = "config.toml")]
    pub config_path: PathBuf,
    /// Number of queries to generate/test (if provided, otherwise sourced from config)
    #[structopt(short = "n", long = "num_queries")]
    pub num_queries: Option<usize>,
    /// Print generated queries, overriding config file settings
    #[structopt(short = "p", long = "print")]
    pub print: bool,
    /// Anticall model setting
    #[structopt(short = "asl", long = "anticall_stir_level")]
    pub anticall_stir_level: Option<usize>,
}

pub trait TomlReadable {
    fn from_toml(toml_config: &Value) -> Self;
}

#[derive(Debug, Clone)]
pub struct MainConfig {
    pub mode: String,
    pub num_generate: usize,
    pub count_equivalence: bool,
    pub measure_generation_time: bool,
    pub assert_parcing_equivalence: bool,
    pub assert_runs_on_schema: bool,
    pub print_progress: bool,
}

impl TomlReadable for MainConfig {
    fn from_toml(toml_config: &toml::Value) -> Self {
        let section = &toml_config["main"];
        Self {
            mode: section["mode"].as_str().unwrap().to_string(),
            num_generate: section["num_generate"].as_integer().unwrap() as usize,
            count_equivalence: section["count_equivalence"].as_bool().unwrap(),
            measure_generation_time: section["measure_generation_time"].as_bool().unwrap(),
            assert_parcing_equivalence: section["assert_parcing_equivalence"].as_bool().unwrap(),
            assert_runs_on_schema: section["assert_runs_on_schema"].as_bool().unwrap(),
            print_progress: section["print_progress"].as_bool().unwrap(),
        }
    }
}

pub struct Config {
    pub main_config: MainConfig,
    pub syntax_coverage_config: SyntaxCoverageConfig,
    pub semantic_correctness_config: SemanticCorrectnessConfig,
    pub generator_config: QueryGeneratorConfig,
    pub chain_config: StateGeneratorConfig,
    pub training_config: TrainingConfig,
    pub ast2path_testing_config: AST2PathTestingConfig,
    pub model_config: ModelConfig,
    pub anticall_model_config: AntiCallModelConfig,
}

fn read_toml_config(config_path: &PathBuf) -> Result<Value, Box<dyn std::error::Error>> {
    let config_source = std::fs::read_to_string(config_path)?;
    Ok(toml::from_str(&config_source)?)
}

impl Config {
    pub fn read_config(config_path: &PathBuf) -> Result<Self, Box<dyn std::error::Error>> {
        let toml_config = read_toml_config(config_path)?;
        Ok(Self {
            main_config: MainConfig::from_toml(&toml_config),
            syntax_coverage_config: SyntaxCoverageConfig::from_toml(&toml_config),
            semantic_correctness_config: SemanticCorrectnessConfig::from_toml(&toml_config),
            generator_config: QueryGeneratorConfig::from_toml(&toml_config),
            chain_config: StateGeneratorConfig::from_toml(&toml_config),
            training_config: TrainingConfig::from_toml(&toml_config),
            ast2path_testing_config: AST2PathTestingConfig::from_toml(&toml_config),
            model_config: ModelConfig::from_toml(&toml_config),
            anticall_model_config: AntiCallModelConfig::from_toml(&toml_config),
        })
    }

    pub fn update_from_args(&mut self, program_args: &ProgramArgs) {
        if let Some(num_queries) = program_args.num_queries {
            self.main_config.num_generate = num_queries;
            self.ast2path_testing_config.n_tests = num_queries;
            self.semantic_correctness_config.n_tests = num_queries;
        }
        if let Some(anticall_stir_level) = program_args.anticall_stir_level {
            self.anticall_model_config.stir_level = anticall_stir_level;
        }
        if program_args.print {
            self.generator_config.print_queries = true;
        }
    }
}
