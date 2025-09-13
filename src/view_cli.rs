use anyhow::{Context, Result};
use rusqlite::Connection;
use std::time::{Duration, UNIX_EPOCH};

pub fn view_cli(db_path: String) -> Result<()> {
    let conn = Connection::open(db_path).context("Failed to open database")?;

    // スキーマに合わせて mtime を取得するように変更
    let mut stmt = conn
        .prepare("SELECT path, size, mtime FROM files ORDER BY size DESC LIMIT 10")
        .context("Failed to prepare statement")?;

    let file_iter = stmt
        .query_map([], |row| {
            let path: String = row.get(0)?;
            let size: i64 = row.get(1)?;
            let mtime: i64 = row.get(2)?;
            Ok((path, size, mtime))
        })
        .context("Failed to query files")?;

    println!("{:<12} {:<20} {}", "Size", "Modified", "Path");
    println!("{}", "-".repeat(80));

    for file in file_iter {
        let (path, size, mtime) = file.context("Failed to get file data")?;
        let modified_time = UNIX_EPOCH + Duration::from_secs(mtime as u64);
        // chrono の開発設定によっては from() が使えるのでこちらで変換
        let datetime = chrono::DateTime::<chrono::Local>::from(modified_time);
        println!(
            "{:<12} {:<20} {}",
            size,
            datetime.format("%Y-%m-%d %H:%M:%S"),
            path
        );
    }

    let total_size: i64 = conn
        .query_row("SELECT COALESCE(SUM(size), 0) FROM files", [], |row| row.get(0))
        .context("Failed to query total size")?;

    fn human_readable(bytes: i64) -> String {
        let mut n = bytes as f64;
        let units = ["B", "KB", "MB", "GB", "TB", "PB"];
        let mut i = 0usize;
        while n >= 1024.0 && i + 1 < units.len() {
            n /= 1024.0;
            i += 1;
        }
        format!("{:.2} {}", n, units[i])
    }

    println!("\nTotal used: {} ({})", total_size, human_readable(total_size));

    let file_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM files", [], |row| row.get(0))
        .context("Failed to query file count")?;
    println!("Files: {}", file_count);

    Ok(())
}
