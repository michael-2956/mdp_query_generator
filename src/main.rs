use std::{io::Write, time::Instant};

use equivalence_testing::{query_creation::{
    query_generator::{QueryGenerator, QueryGeneratorConfig, value_choosers::RandomValueChooser},
    state_generator::{
        MarkovChainGenerator,
        state_choosers::{ProbabilisticStateChooser, StateChooser},
        dynamic_models::{DynamicModel, MarkovModel, AntiCallModel},
    },
}, equivalence_testing_function::{
    check_query, string_to_query
}, config::{Config, ProgramArgs, MainConfig}, training::{ast_to_path::TestAST2Path, trainer::SQLTrainer, models::SubgraphMarkovModel}};

use structopt::StructOpt;

fn run_generation<DynMod: DynamicModel, StC: StateChooser>(markov_generator: MarkovChainGenerator<StC>, generator_config: QueryGeneratorConfig, main_config: MainConfig) {
    let mut generator = QueryGenerator::<DynMod, StC, RandomValueChooser>::from_state_generator_and_schema(markov_generator, generator_config);

    let mut accumulated_time_ns = 0;
    let mut num_equivalent = 0;
    for i in 0..main_config.num_generate {
        let start_time = Instant::now();
        let query_ast = Box::new(generator.next().unwrap());
        accumulated_time_ns += (Instant::now() - start_time).as_nanos();
        if main_config.assert_parcing_equivalence {
            let query_string = query_ast.to_string();
            if let Some(parsed_ast) = string_to_query(&query_string) {
                if parsed_ast != query_ast {
                    println!("AST mismatch! For query: {query_string}");
                    let mut f_g = std::fs::File::create(format!("{i}-g")).unwrap();
                    write!(f_g, "{:#?}", query_ast).unwrap();
                    let mut f_p = std::fs::File::create(format!("{i}-p")).unwrap();
                    write!(f_p, "{:#?}", parsed_ast).unwrap();
                }
            }
        }
        if main_config.count_equivalence {
            num_equivalent += check_query(query_ast) as usize;
        }
        if i % 100 == 0 {
            if main_config.print_progress {
                print!("{}/{}      \r", i, main_config.num_generate);
            }
            std::io::stdout().flush().unwrap();
        }
    }
    if main_config.measure_generation_time {
        println!("Average generation time: {} secs", accumulated_time_ns as f64 / 1_000_000_000f64 / main_config.num_generate as f64);
    }
    if main_config.count_equivalence {
        println!("Equivalence: {} / {}", num_equivalent, main_config.num_generate);
    }
}

fn select_model_and_run_generation<StC: StateChooser>(config: Config) {
    let markov_generator = match MarkovChainGenerator::<StC>::with_config(&config.chain_config) {
        Ok(generator) => generator,
        Err(err) => {
            println!("{}", err);
            return;
        }
    };

    if config.generator_config.dynamic_model_name == "anticall" {
        run_generation::<AntiCallModel, _>(markov_generator, config.generator_config, config.main_config);
    } else {
        run_generation::<MarkovModel, _>(markov_generator, config.generator_config, config.main_config);
    };
}

fn run_training(config: Config) {
    let mut sql_trainer = match SQLTrainer::with_config(config) {
        Ok(trainer) => trainer,
        Err(err) => {
            println!("{}", err);
            return;
        },
    };
    let model = SubgraphMarkovModel::new(
        &sql_trainer.markov_chain_ref().functions
    );
    let model = match sql_trainer.train(Box::new(model)) {
        Ok(model) => model,
        Err(err) => {
            println!("Training error!\n{}", err);
            return;
        },
    };
    match model.write_weights("weights/untrained_db.mw") {
        Ok(_) => {},
        Err(err) => {
            println!("Failed writing model!\n{}", err);
            return;
        },
    }
    model.print_weights();
}

fn test_ast_to_path(config: Config) {
    let mut tester = match TestAST2Path::with_config(config) {
        Ok(trainer) => trainer,
        Err(err) => {
            println!("{}", err);
            return;
        },
    };
    if let Err(err) = tester.test() {
        println!("\n{err}");
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let program_args = ProgramArgs::from_args();
    let mut config = Config::read_config(&program_args.config_path)?;
    config.update_from_args(&program_args);
    if config.main_config.mode == "train" {
        run_training(config);
    } else if config.main_config.mode == "generate" {
        select_model_and_run_generation::<ProbabilisticStateChooser>(config);
    } else if config.main_config.mode == "test_ast_to_path" {
        test_ast_to_path(config);
    }
    Ok(())
}
