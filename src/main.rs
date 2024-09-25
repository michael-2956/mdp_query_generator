use std::{io::Write, time::Instant};

use equivalence_testing::{config::{Config, ProgramArgs}, equivalence_testing_function::{
    check_query, string_to_query
}, programs::{semantic_correctness::test_semantic_correctness, syntax_coverage::test_syntax_coverage}, query_creation::{
    query_generator::QueryGenerator,
    state_generator::{
        state_choosers::{MaxProbStateChooser, ProbabilisticStateChooser, StateChooser},
        substitute_models::{AntiCallModel, MarkovModel, SubstituteModel}, MarkovChainGenerator
    },
}, training::{ast_to_path::tester::TestAST2Path, trainer::SQLTrainer}};

use structopt::StructOpt;

fn run_generation<StC: StateChooser>(
        markov_generator: MarkovChainGenerator<StC>, config: Config, sub_model: Box<dyn SubstituteModel>
    ) {
    let mut predictor_model = if config.generator_config.use_model {
        Some(config.model_config.create_model().expect("Error creating model for inference"))
    } else { None };

    let print_queries = config.generator_config.print_queries;

    let mut generator = QueryGenerator::<StC>::from_state_generator_and_config(markov_generator, config.generator_config, sub_model);

    let mut accumulated_time_ns = 0;
    let mut num_equivalent = 0;
    let mut i = 0;
    loop {
        let start_time = Instant::now();
        let query_ast;
        (predictor_model, query_ast) = if let Some(model) = predictor_model {
            let (model, query) = generator.generate_with_model(model);
            (Some(model), query)
        } else {
            (None, generator.generate())
        };
        accumulated_time_ns += (Instant::now() - start_time).as_nanos();

        let query_string = query_ast.to_string();

        if let Some(parsed_ast) = string_to_query(&query_string) {
            if config.main_config.assert_parcing_equivalence {
                if *parsed_ast != query_ast {
                    eprintln!("AST mismatch! For query: {query_string}");
                    let mut f_g = std::fs::File::create(format!("{i}-g")).unwrap();
                    write!(f_g, "{:#?}", query_ast).unwrap();
                    let mut f_p = std::fs::File::create(format!("{i}-p")).unwrap();
                    write!(f_p, "{:#?}", parsed_ast).unwrap();
                }
            }

            // only print & count parceable queries
            i += 1;
            if print_queries {
                println!("\n{};\n", query_string);
            }
        }

        if config.main_config.count_equivalence {
            num_equivalent += check_query(Box::new(query_ast)) as usize;
        }
        if i % 100 == 0 {
            if config.main_config.print_progress {
                eprint!("{}/{}      \r", i, config.main_config.num_generate);
            }
            std::io::stderr().flush().unwrap();
        }
        if i >= config.main_config.num_generate {
            break
        }
    }
    if config.main_config.measure_generation_time {
        eprintln!("Average generation time: {} secs", accumulated_time_ns as f64 / 1_000_000_000f64 / config.main_config.num_generate as f64);
    }
    if config.main_config.count_equivalence {
        eprintln!("Equivalence: {} / {}", num_equivalent, config.main_config.num_generate);
    }
}

fn select_sub_model_and_run_generation<StC: StateChooser>(config: Config) {
    let markov_generator = MarkovChainGenerator::<StC>::with_config(&config.chain_config).expect("Could not create generator!");

    let sub_model: Box<dyn SubstituteModel> = match config.generator_config.substitute_model_name.as_str() {
        "anticall" => Box::new(AntiCallModel::new(config.anticall_model_config.clone())),
        "markov" => Box::new(MarkovModel::new()),
        any => panic!("Unexpected dynamic model name in config: {any}"),
    };

    run_generation::<_>(markov_generator, config, sub_model);
}

fn run_training(config: Config) {
    let mut sql_trainer = SQLTrainer::with_config(&config).expect("Could not create trainer!");
    let model = config.model_config.create_model().expect("Coudn't create model!");
    let model = sql_trainer.train(model).unwrap_or_else(|err| panic!("Training error!\n{err}"));
    model.write_weights(&sql_trainer.config.save_weights_to).expect("Failed writing model!");
    if config.model_config.print_weights_after_training {
        model.print_weights();
    }
}

fn test_ast_to_path(config: Config) {
    let mut tester = match TestAST2Path::with_config(config) {
        Ok(tester) => tester,
        Err(err) => {
            eprintln!("{}", err);
            return;
        },
    };
    if let Err(err) = tester.test() {
        eprintln!("\n{err}");
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let program_args = ProgramArgs::from_args();
    let mut config = Config::read_config(&program_args.config_path)?;
    config.update_from_args(&program_args);
    if config.main_config.mode == "train" {
        run_training(config);
    } else if config.main_config.mode == "generate" {
        if config.generator_config.use_probabilistic_state_chooser {
            select_sub_model_and_run_generation::<ProbabilisticStateChooser>(config);
        } else {
            select_sub_model_and_run_generation::<MaxProbStateChooser>(config);
        }
    } else if config.main_config.mode == "test_ast_to_path" {
        test_ast_to_path(config);
    } else if config.main_config.mode == "syntax_coverage" {
        test_syntax_coverage(config);
    } else if config.main_config.mode == "semantic_correctness" {
        test_semantic_correctness(config);
    }
    Ok(())
}
