use std::fs::File;
use std::io::{BufRead, BufReader};
use std::collections::HashMap;

pub struct Graph {
    // key is a name of a node
    // value is a vector of pairs, first element of which is a name of another node
    // and second element is a probability of moving to this node
    graph: HashMap <String, Vec<(String, f32)>>,
}

impl Graph {
    pub fn new () -> Self {
        Graph{
            graph : HashMap::new(),
        }
    }

    pub fn read(&mut self, filename: String) {
        let file = File::open(filename).unwrap();
        let reader = BufReader::new(file);
        for (index, line) in reader.lines().enumerate() {
            let line = line.unwrap();
            if self.is_edge(line.to_string()) {
                    self.add(line.to_string());
            }
        }
    }

    fn is_edge(&mut self, line: String) -> bool {
        if line.contains("->") {
            true
        } else {
            false
        }
    }

    fn add(&mut self, mut line: String) {
        line = self.clean_code(line);
        line = format!("{}{}", line, ' ');
        let mut word: bool = true;
        let mut quotes: bool = false;
        let mut from: String = "".to_string();
        let mut to: String = "".to_string();
        let mut tmp: String = "".to_string();
        let mut prev_char: char = ' ';
        for (i,c) in line.chars().enumerate() {
            if
            if word == false && c != '-' {
                if from == "" {
                    from = tmp;
                } else {
                    from = self.make_name(from);
                    to = self.make_name(tmp.clone());
                    self.insert(from, to);
                    from = (tmp.clone());
                }
                tmp = "".to_string();
            }
        }

    }


    fn clean_code (&mut self, mut arg: String) -> String {
        arg = format!("{}{}", arg, ' ');
        let mut content: bool = true;
        let mut avoid_space: bool = false;
        let mut prev_char: char = ' ';
        let mut result: String = "".to_string();
        for c in arg.chars() {
            let mut change_prev: bool = true;
            if c == '[' || c == '#' {
                result = format!("{}{}", result, prev_char);
                content = false;
            } else {
                if (c == '/' && prev_char == '/') || (c == '*' && prev_char == '/') {
                    content = false;
                }  else {
                    if (c == '/' && prev_char == '*') || c == ']' {
                        content = true;
                        prev_char = ' ';
                        avoid_space = true;
                        change_prev = false;
                    } else {
                        if content {
                            if !(avoid_space && prev_char == ' ') {
                                avoid_space = false;
                                result = format!("{}{}", result, prev_char);
                            }
                        }
                    }
                }
            }
            if change_prev {
                prev_char = c;
            }
        }
        result
    }


    fn make_name (&mut self, arg: String) -> String {
        let mut name : String = "".to_string();
        for c in arg.chars() {
            if !(c=='"') {
                name = format!("{}{}", name, c);
            }
        }
        name.trim().to_string()
    }

    fn insert(&mut self, sender: String, reciever: String) {
        if self.graph.contains_key(&sender) {
            self.graph.get_mut(&sender).unwrap().push((reciever, 0.0));
        } else {
            let mut v : Vec<(String, f32)> = Vec::new();
            v.push((reciever, 0.0));
            self.graph.insert(sender, v);
        }
        self.update_probs(sender.clone());
    }

    fn update_probs (&mut self, key: String) {
        let value: f32 = 0.5; //1.0 / self.graph[&key].len();
        for i in &mut self.graph.get_mut(&key) {
        //update second element in the pair in vector
        i.1 = value;
    }
    }
}
