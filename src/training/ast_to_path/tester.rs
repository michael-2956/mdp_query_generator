use std::{collections::VecDeque, fs, io::Write, sync::{mpsc::{channel, Receiver, Sender}, Arc, Mutex}, thread, time::Instant};

use itertools::Itertools;
use sqlparser::ast::Query;

use crate::{config::Config, query_creation::{query_generator::{query_info::DatabaseSchema, value_choosers::DeterministicValueChooser, QueryGenerator}, state_generator::{markov_chain_generator::error::SyntaxError, state_choosers::{MaxProbStateChooser, ProbabilisticStateChooser}, substitute_models::{AntiCallModel, PathModel}, MarkovChainGenerator}}};

use super::{ConvertionError, PathGenerator};

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
        for i in 0..self.config.ast2path_testing_config.n_tests {
            let query = Box::new(self.random_query_generator.generate());
            if i % 1 == 0 {
                if self.config.main_config.print_progress {
                    print!("{}/{}      \r", i, self.config.ast2path_testing_config.n_tests);
                    std::io::stdout().flush().unwrap();
                }
            }
            // if i < 0 { continue; }
            // eprintln!("\nTested query: {query}\n");
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
        // TODO: redo with channels, since queue will not work like that
        let query_queue: Arc<Mutex<Option<VecDeque<Option<(usize, Box<Query>)>>>>> = Arc::new(Mutex::new(Some(VecDeque::new())));
        let (result_sender, result_receiver): (Sender<Result<(usize, f64), ConvertionError>>, Receiver<Result<(usize, f64), ConvertionError>>) = channel();
        let mut handles = vec![];

        // Spawn worker threads
        for _ in 0..n_threads {
            let result_sender = result_sender.clone();

            let config = self.config.clone();

            let thread_query_queue = Arc::clone(&query_queue);

            handles.push(thread::spawn(move || {
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
                while let Some(query_opt) = {
                    let mut queue_guard = thread_query_queue.lock().unwrap();
                    let item = queue_guard.as_mut().and_then(|queue| queue.pop_front());
                    item
                } {
                    let Some((i, query)) = query_opt else { continue; }; // queue is empty
                    // Process the task
                    let path_gen_start = Instant::now();
                    let path = match path_generator.get_query_path(&query) {
                        Ok(p) => p,
                        Err(e) => {
                            result_sender.send(Err(ConvertionError::new(
                                format!("Error generating path for query #{i}.\nQuery: {query}\nError: {e}")
                            ))).unwrap();
                            // cancel query generation & convertion
                            *thread_query_queue.lock().unwrap() = None;
                            break;
                        }
                    };
                    let elapsed_time = path_gen_start.elapsed().as_secs_f64();
                    let path_len = path.len();
                    let generated_query = path_query_generator.generate_with_substitute_model_and_value_chooser(
                        Box::new(PathModel::from_path_nodes(&path)),
                        Box::new(DeterministicValueChooser::from_path_nodes(&path)),
                    );
                    if *query != generated_query {
                        result_sender.send(Err(ConvertionError::new(format!(
                            "\nAST -> path -> AST mismatch!\nOriginal  query: {}\nGenerated query: {}\nPath: {:?}",
                            query, generated_query, path
                        )))).unwrap();
                        // cancel query generation & convertion
                        *thread_query_queue.lock().unwrap() = None;
                        break;
                    }
                    // Send the result back
                    result_sender.send(Ok((path_len, elapsed_time))).unwrap();
                }
                {
                    let mut queue_guard = thread_query_queue.lock().unwrap();
                    if let Some(queue) = queue_guard.as_mut() {
                        queue.push_back(None); // propagate poison
                    } // otherwise, generation was cancelled
                }
            }));
        }

        // Generate queries and submit tasks
        for i in 0..n_tests {
            if i % 1 == 0 {
                if self.config.main_config.print_progress {
                    print!("Generating: {}/{}      \r", i, self.config.ast2path_testing_config.n_tests);
                    std::io::stdout().flush().unwrap();
                }
            }
            let query = Box::new(self.random_query_generator.generate());
            {
                let mut queue_guard = query_queue.lock().unwrap();
                if let Some(queue) = queue_guard.as_mut() {
                    queue.push_back(Some((i, query)));
                } else {
                    // generation was cancelled by error
                    println!("\nGeneration cancelled"); std::io::stdout().flush().unwrap();
                    break;
                }
            }
        }

        if let Some(queue) = query_queue.lock().unwrap().as_mut() {
            // poison the task queue so workers will stop when queue is empty
            queue.push_back(None);
        }

        let mut path_length_time = vec![];
        let mut completed = 0;

        // Collect results and update progress
        while completed < n_tests {
            if let Ok(resp) = result_receiver.recv() {
                match resp {
                    Ok((path_len, elapsed_time)) => {
                        path_length_time.push((path_len, elapsed_time));
                        completed += 1;
    
                        if self.config.main_config.print_progress && completed % 1 == 0 {
                            print!("Converted: {}/{}      \r", completed, self.config.ast2path_testing_config.n_tests);
                            std::io::stdout().flush().unwrap();
                        }
                    },
                    Err(err) => {
                        return Err(err)
                    }
                }
            }
        }

        for handle in handles {
            handle.join().unwrap();
        }

        fs::write("ast2path_time.txt", format!(
            "[\n{}\n]", path_length_time.into_iter().map(
                |(l, t)| format!("({l}, {t})")
            ).join(",\n")
        )).unwrap();
        Ok(())
    }
}