use std::{fs, io::Write, path::PathBuf, str::FromStr, sync::{atomic::{AtomicBool, Ordering}, mpsc::{self, Receiver, Sender}, Arc, Mutex}, thread, time::Instant};

use itertools::Itertools;
use sqlparser::ast::Query;

use crate::{config::{Config, TomlReadable}, query_creation::{query_generator::{query_info::DatabaseSchema, value_choosers::DeterministicValueChooser, QueryGenerator}, state_generator::{markov_chain_generator::error::SyntaxError, state_choosers::{MaxProbStateChooser, ProbabilisticStateChooser}, substitute_models::{AntiCallModel, PathModel}, MarkovChainGenerator}}};

use super::{ConvertionError, PathGenerator};

#[derive(Debug, Clone)]
pub struct AST2PathTestingConfig {
    pub schema: PathBuf,
    pub n_tests: usize,
    pub parallel: bool,
    pub start_testing_from: Option<usize>,
}

impl TomlReadable for AST2PathTestingConfig {
    fn from_toml(toml_config: &toml::Value) -> Self {
        let section = &toml_config["ast_to_path_testing"];
        Self {
            schema: PathBuf::from_str(section["testing_schema"].as_str().unwrap()).unwrap(),
            n_tests: section["n_tests"].as_integer().unwrap() as usize,
            parallel: section["parallel"].as_bool().unwrap(),
            start_testing_from: None
        }
    }
}

pub struct TestAST2Path {
    config: Config,
    path_generator: PathGenerator,
    random_query_generator: QueryGenerator<ProbabilisticStateChooser>,
    path_query_generator: QueryGenerator<MaxProbStateChooser>,
}

impl TestAST2Path {
    pub fn with_config(config: Config) -> Result<Self, SyntaxError> {
        Ok(Self {
            path_generator: PathGenerator::new(
                DatabaseSchema::parse_schema(&config.ast2path_testing_config.schema),
                &config.chain_config,
                config.generator_config.aggregate_functions_distribution.clone(),
            )?,
            random_query_generator: QueryGenerator::from_state_generator_and_config(
                MarkovChainGenerator::with_config(&config.chain_config).unwrap(),
                config.generator_config.clone(),
                Box::new(AntiCallModel::new(config.anticall_model_config.clone())),
            ),
            path_query_generator: QueryGenerator::from_state_generator_and_config(
                MarkovChainGenerator::with_config(&config.chain_config).unwrap(),
                config.generator_config.clone(),
                Box::new(PathModel::empty())
            ),
            config: config,
        })
    }

    pub fn test(&mut self) -> Result<(), ConvertionError> {
        let mut path_length_time = vec![];
        let start_testing_from = self.config.ast2path_testing_config.start_testing_from.clone();
        for i in 0..self.config.ast2path_testing_config.n_tests {
            let query = Box::new(self.random_query_generator.generate());
            if i % 1 == 0 {
                if self.config.main_config.print_progress {
                    print!("{}/{}      \r", i, self.config.ast2path_testing_config.n_tests);
                    std::io::stdout().flush().unwrap();
                }
            }
            if let Some(start_testing_from) = start_testing_from {
                if i < start_testing_from { continue; }
                eprintln!("\nTested query: {query}\n");
            }
            let path_gen_start = Instant::now();
            let path = self.path_generator.get_query_path(&query)?;
            path_length_time.push((path.len(), path_gen_start.elapsed().as_secs_f64()));
            let generated_query = self.path_query_generator.generate_with_substitute_model_and_value_chooser(
                Box::new(PathModel::from_path_nodes(&path)),
                Box::new(DeterministicValueChooser::from_path_nodes(&path))
            );
            if *query != generated_query {
                eprintln!("\nAST -> path -> AST mismatch!\nOriginal  query: {}\nGenerated query: {}", query, generated_query);
                eprintln!("Path: {:?}", path);
            }
        }
        fs::write("ast2path_time.txt", format!(
            "[\n{}\n]", path_length_time.into_iter().map(
                |(l, t)| format!("({l}, {t})")
            ).join(",\n")
        )).unwrap();
        Ok(())
    }

    pub fn test_parallel(&mut self) -> Result<(), ConvertionError> {
        let n_tests = self.config.ast2path_testing_config.n_tests;
        let n_threads = 12;

        let (tx, rx): (
            Sender<(usize, Box<Query>)>,
            Receiver<(usize, Box<Query>)>
        ) = mpsc::channel();
        let (result_tx, result_rx): (
            Sender<(usize, f64)>,
            Receiver<(usize, f64)>
        ) = mpsc::channel();

        let rx = Arc::new(Mutex::new(rx));
        let error_flag = Arc::new(AtomicBool::new(false));
        let convertion_error = Arc::new(Mutex::new(None));

        let mut handles = Vec::new();
        for _ in 0..n_threads {
            let rx = Arc::clone(&rx);
            let result_tx = result_tx.clone();
            let error_flag = Arc::clone(&error_flag);
            let convertion_error = Arc::clone(&convertion_error);
            let config = self.config.clone();

            let handle = thread::spawn(move || {
                let mut path_generator = PathGenerator::new(
                    DatabaseSchema::parse_schema(&config.ast2path_testing_config.schema),
                    &config.chain_config,
                    config.generator_config.aggregate_functions_distribution.clone(),
                ).unwrap();
                let mut path_query_generator = QueryGenerator::<ProbabilisticStateChooser>::from_state_generator_and_config(
                    MarkovChainGenerator::with_config(&config.chain_config).unwrap(),
                    config.generator_config.clone(),
                    Box::new(PathModel::empty()),
                );

                loop {
                    if error_flag.load(Ordering::SeqCst) {
                        break;
                    }

                    let (i, query) = match rx.lock().unwrap().recv() {
                        Ok(x) => x,
                        Err(_) => break,
                    };

                    let path_gen_start = Instant::now();
                    let path = match path_generator.get_query_path(&query) {
                        Ok(p) => p,
                        Err(err) => {
                            error_flag.store(true, Ordering::SeqCst);
                            let mut convertion_error_guard = convertion_error.lock().unwrap();
                            *convertion_error_guard = Some(ConvertionError::new(
                                format!("Error generating path for query #{i}.\nQuery: {query}\nError: {err}")
                            ));
                            break
                        }
                    };
                    let elapsed_time = path_gen_start.elapsed().as_secs_f64();
                    let path_len = path.len();
                    let generated_query = path_query_generator.generate_with_substitute_model_and_value_chooser(
                        Box::new(PathModel::from_path_nodes(&path)),
                        Box::new(DeterministicValueChooser::from_path_nodes(&path)),
                    );
                    if *query != generated_query {
                        error_flag.store(true, Ordering::SeqCst);
                        let mut convertion_error_guard = convertion_error.lock().unwrap();
                        *convertion_error_guard = Some(ConvertionError::new(format!(
                            "\nAST -> path -> AST mismatch for query #{i}!\nOriginal  query: {}\nGenerated query: {}\nPath: {:?}",
                            query, generated_query, path
                        )));
                        break
                    }
                    result_tx.send((path_len, elapsed_time)).unwrap();
                }
            });
            handles.push(handle);
        }

        let mut path_length_time = vec![];
        let mut n_completed = 0usize;
        let process_result = |result: (usize, f64), n_completed: &mut usize, path_length_time: &mut Vec<(usize, f64)>| {
            *n_completed += 1;
            path_length_time.push(result);
        };

        for i in 0..n_tests {
            if error_flag.load(Ordering::SeqCst) {
                eprintln!("\nAborting generation & collection due to error in worker threads.");
                break;
            }
            if let Ok(result) = result_rx.try_recv() {
                process_result(result, &mut n_completed, &mut path_length_time);
            }
            if i % 1 == 0 {
                if self.config.main_config.print_progress {
                    print!(
                        "Generating: {}/{} | Converted: {}/{}      \r",
                        i+1, self.config.ast2path_testing_config.n_tests,
                        n_completed, self.config.ast2path_testing_config.n_tests
                    );
                    std::io::stdout().flush().unwrap();
                }
            }
            let query = Box::new(self.random_query_generator.generate());
            if tx.send((i, query)).is_err() {
                break; // Exit if the sending channel is closed
            }
        }

        // Drop the original sender to close the channel and notify worker threads
        drop(tx);

        println!();
        
        if error_flag.load(Ordering::SeqCst) {
            drop(result_tx)
        }
        for result in result_rx {
            process_result(result, &mut n_completed, &mut path_length_time);
            if self.config.main_config.print_progress && n_completed % 1 == 0 {
                print!("Converted: {}/{}      \r", n_completed, self.config.ast2path_testing_config.n_tests);
                std::io::stdout().flush().unwrap();
            }
            if n_completed == self.config.ast2path_testing_config.n_tests {
                break;
            }
            // Check if an error has been encountered to stop processing
            if error_flag.load(Ordering::SeqCst) {
                eprintln!("Aborting result collection due to error in worker threads.");
                break;
            }
        }

        for handle in handles {
            handle.join().unwrap();
        }

        // Check if an error occurred
        if error_flag.load(Ordering::SeqCst) {
            let convertion_error_guard = convertion_error.lock().unwrap();
            let convertion_error = convertion_error_guard.as_ref().unwrap();
            eprintln!("\nProcess aborted due to an error");
            return Err((*convertion_error).clone())
        } else {
            println!("\nAll queries converted successfully.");
        }

        fs::write("ast2path_time_parallel.txt", format!(
            "[\n{}\n]", path_length_time.into_iter().map(
                |(l, t)| format!("({l}, {t})")
            ).join(",\n")
        )).unwrap();

        Ok(())
    }
}