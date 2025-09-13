use anyhow::Result;
use std::env;
use std::path::PathBuf;
mod view_cli;

fn main() -> Result<()> {
    env_logger::init_from_env(env_logger::Env::default().default_filter_or("info"));
    let start = std::time::Instant::now();

    let mut args = env::args().skip(1);
    let mode = args.next().unwrap_or_else(|| "scan".to_string());
    let root = args
        .next()
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."));
    let db_path = args.next().unwrap_or_else(|| "scan.db".to_string());

    let result = match mode.as_str() {
        "scan" => fast_disk_analyzer::run(root, db_path),
        "view" => view_cli::view_cli(db_path),
        _ => Err(anyhow::anyhow!("Unknown mode: {}", mode)),
    };

    let elapsed = start.elapsed();
    println!("Execution time: {:.3}s", elapsed.as_secs_f64());

    result
}