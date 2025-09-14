#![windows_subsystem = "windows"]
use anyhow::Result;
use std::env;
use std::path::PathBuf;
use std::io::IsTerminal;
mod view_cli;
mod scan;
mod gui;

fn main() -> Result<()> {
    env_logger::init_from_env(env_logger::Env::default().default_filter_or("info"));
    let start = std::time::Instant::now();

    let mut args = env::args().skip(1);
    let mode_arg = args.next();
    let mode = mode_arg.clone().unwrap_or_else(|| "scan".to_string());
    let root = args
        .next()
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."));
    let db_path = args.next().unwrap_or_else(|| "scan.db".to_string());

    if IsTerminal::is_terminal(&std::io::stdout()) && mode_arg.is_none() {
        println!("Launching GUI...");
        freya::launch::launch(gui::app);
        return Ok(());
    }

    let result = match mode.as_str() {
        "scan" => scan::run(root, db_path),
        "view" => view_cli::view_cli(db_path),
        "gui" => {
            println!("Launching GUI...");
            freya::launch::launch(gui::app);
            Ok(())
        }
        _ => Err(anyhow::anyhow!("Unknown mode: {}", mode)),
    };

    let elapsed = start.elapsed();
    println!("Execution time: {:.3}s", elapsed.as_secs_f64());

    result
}