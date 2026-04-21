use gcflobdd::gcflobdd::Gcflobdd;
use gcflobdd::gcflobdd::context::Context;
use gcflobdd::grammar::Grammar;
use std::cell::RefCell;

fn gen_balanced_grammar(level: usize) -> Vec<String> {
    if level == 0 {
        return vec!["S0 -> a".to_string()];
    }
    let mut grammar = vec![];
    for i in (1..level).rev() {
        grammar.push(format!("S{} -> S{} S{}", i, i - 1, i - 1));
    }
    grammar.push("S0 -> a a".to_string());
    grammar
}
fn size_to_readable(size: usize) -> String {
    let mut size = size as f64;
    let mut unit = 0;
    let prefixes = vec!["B", "KiB", "MiB", "GiB"];
    while size >= 1024.0 {
        size /= 1024.0;
        unit += 1;
    }
    format!("{:.2}{}", size, prefixes[unit])
}
fn main() {
    let args: Vec<String> = std::env::args().collect();
    let mut n = 18usize;
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--n" => {
                if i + 1 < args.len() {
                    n = args[i + 1].parse().unwrap();
                    i += 1;
                }
            }
            _ => {
                println!("Unknown argument: {}", args[i]);
            }
        }
        i += 1;
    }

    let grammar = Grammar::new(&gen_balanced_grammar(n)).unwrap();
    let context = RefCell::new(Context::default());

    let start_time = std::time::Instant::now();

    let mut xor = Gcflobdd::mk_false(&grammar, &context);

    for i in 0..(1 << n) {
        xor = xor.mk_xor(&Gcflobdd::mk_projection(i, &grammar, &context), &context);
    }
    let end_time = std::time::Instant::now();
    context.borrow_mut().gc();
    let gc_end_time = std::time::Instant::now();
    println!(
        "Solved in {} ms, GC in {} ms, with {} nodes, {} memory usage",
        end_time.duration_since(start_time).as_millis(),
        gc_end_time.duration_since(end_time).as_millis(),
        context.borrow().node_count(),
        size_to_readable(context.borrow().size_estimate())
    );
}
