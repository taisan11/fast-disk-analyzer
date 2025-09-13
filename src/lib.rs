use anyhow::{Context, Result};
use log::{error, info, warn};
use rusqlite::{Connection, params};
use std::collections::HashMap;
use std::fs::metadata;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use kanal::{bounded, unbounded, Receiver as KanalReceiver, Sender as KanalSender};
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};
use walkdir::WalkDir;

#[derive(Debug)]
struct FileRecord {
    path: String,
    name: String,
    parent_path: Option<String>,
    /// "file", "dir", "other"
    file_type: String,
    size: u64,
    dev: i64,
    inode: i64,
    nlink: i64,
    mtime: i64,
    scanned_at: i64,
}

pub fn run(root: PathBuf, db_path: String) -> Result<()> {
    info!("Scanning: {}", root.display());
    info!("DB: {}", db_path);

    // init schema using a temporary connection (main thread); the writer thread
    // will open its own connection for writes.
    let mut init_conn = Connection::open(&db_path).context("open sqlite for init")?;
    init_schema(&init_conn)?;

    let scanned_at = current_unix_ts();

    // channels: paths -> workers, records -> db writer
    // path channel: use bounded channel to avoid unbounded memory growth when walking very large trees
    let (path_tx, path_rx) = bounded::<PathBuf>(1000);
    let path_rx = Arc::new(Mutex::new(path_rx)); // share receiver among workers

    let (rec_tx, rec_rx) = unbounded::<FileRecord>(); // many producers (workers), single consumer (writer)

    // spawn DB writer thread
    let db_path_for_writer = db_path.clone();
    let db_writer_handle = thread::spawn(move || {
        if let Err(e) = db_writer_thread(&db_path_for_writer, rec_rx, 500) {
            error!("DB writer error: {:?}", e);
        }
    });

    // spawn worker threads for metadata collection
    let worker_count = num_cpus::get().max(1);
    info!("Spawning {} worker threads", worker_count);
    let mut worker_handles = Vec::with_capacity(worker_count);
    for _ in 0..worker_count {
    let path_rx_cloned = Arc::clone(&path_rx);
    let rec_tx_cloned = rec_tx.clone();
        let scanned_at = scanned_at;
        let handle =
            thread::spawn(move || worker_thread(path_rx_cloned, rec_tx_cloned, scanned_at));
        worker_handles.push(handle);
    }
    // drop main-side clone of rec_tx so only worker clones and none of main hold it (writer can finish)
    drop(rec_tx);

    // producer: walkdir and send paths
    for entry in WalkDir::new(&root)
        .follow_links(false)
        .into_iter()
        .filter_map(|e| e.ok())
    {
        let p = entry.path().to_path_buf();
        if let Err(e) = path_tx.send(p) {
            warn!("Failed to send path to workers: {}", e);
            break;
        }
    }
    // close path channel so workers terminate when done
    drop(path_tx);

    // wait for workers
    for h in worker_handles {
        if let Err(e) = h.join() {
            warn!("Worker thread join error: {:?}", e);
        }
    }
    info!("All workers finished.");

    // at this point, all workers have finished producing records and rec_tx was dropped
    // DB writer will exit after consuming all records; wait for it
    if let Err(e) = db_writer_handle.join() {
        warn!("DB writer thread join error: {:?}", e);
    }
    info!("DB writer finished.");

    // compute dir_stats (main thread, using init_conn)
    compute_and_upsert_dir_stats(&mut init_conn)?;

    info!("Done.");
    Ok(())
}

fn init_schema(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        r#"
PRAGMA journal_mode = WAL;

CREATE TABLE IF NOT EXISTS files (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  path TEXT NOT NULL UNIQUE,
  name TEXT,
  parent_path TEXT,
  file_type TEXT,
  size INTEGER,
  dev INTEGER,
  inode INTEGER,
  mtime INTEGER,
  nlink INTEGER,
  scanned_at INTEGER
);

CREATE INDEX IF NOT EXISTS idx_files_parent ON files(parent_path);
CREATE INDEX IF NOT EXISTS idx_files_dev_inode ON files(dev, inode);

CREATE TABLE IF NOT EXISTS dir_stats (
  dir_path TEXT PRIMARY KEY,
  total_size INTEGER,
  file_count INTEGER,
  last_scanned INTEGER
);
"#,
    )?;
    Ok(())
}

fn worker_thread(
    path_rx: Arc<Mutex<KanalReceiver<PathBuf>>>,
    rec_tx: KanalSender<FileRecord>,
    scanned_at: i64,
) {
    loop {
        // receive path under mutex to allow multiple workers to share the single Receiver
        let path_opt = {
            let rx_lock = path_rx.lock().expect("path_rx lock poisoned");
            match rx_lock.recv() {
                Ok(p) => Some(p),
                Err(_) => None, // sender closed -> terminate
            }
        };
        let path = match path_opt {
            Some(p) => p,
            None => break,
        };

        match collect_record(&path, scanned_at) {
            Ok(rec) => {
                if let Err(e) = rec_tx.send(rec) {
                    warn!("Failed to send record to DB writer: {}", e);
                    break;
                }
            }
            Err(e) => {
                warn!("Skipping {:?}: {}", path, e);
            }
        }
    }
    // worker ends
}

fn db_writer_thread(db_path: &str, rec_rx: KanalReceiver<FileRecord>, batch_size: usize) -> Result<()> {
    let mut conn = Connection::open(db_path).context("open sqlite in db_writer")?;

    let mut batch: Vec<FileRecord> = Vec::with_capacity(batch_size);
    loop {
        match rec_rx.recv() {
            Ok(rec) => {
                batch.push(rec);
                if batch.len() >= batch_size {
                    flush_batch(&mut conn, &mut batch)?;
                }
            }
            Err(_) => {
                // all senders dropped -> flush remaining and exit
                break;
            }
        }
    }
    if !batch.is_empty() {
        flush_batch(&mut conn, &mut batch)?;
    }
    Ok(())
}

fn flush_batch(conn: &mut Connection, batch: &mut Vec<FileRecord>) -> Result<()> {
    let tx = conn.transaction()?;
    {
        let mut stmt = tx.prepare(
            "INSERT OR REPLACE INTO files (path, name, parent_path, file_type, size, dev, inode, mtime, nlink, scanned_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
        )?;
        for rec in batch.iter() {
            let res = stmt.execute(params![
                rec.path,
                rec.name,
                rec.parent_path,
                rec.file_type,
                rec.size as i64,
                rec.dev,
                rec.inode,
                rec.mtime,
                rec.nlink,
                rec.scanned_at
            ]);
            if let Err(e) = res {
                warn!("Failed to insert {:?}: {}", rec.path, e);
                // continue inserting other records
            }
        }
    }
    tx.commit()?;
    batch.clear();
    Ok(())
}

fn collect_record(path: &Path, scanned_at: i64) -> Result<FileRecord> {
    // Use metadata (follows symlinks). If you want to avoid following symlinks use symlink_metadata.
    let md = metadata(path)?;
    let file_type = if md.is_dir() {
        "dir"
    } else if md.is_file() {
        "file"
    } else {
        "other"
    }
    .to_string();

    let size = md.len();
    let (dev, inode, nlink) = dev_inode_nlink(&md);

    let mtime = md
        .modified()
        .ok()
        .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);

    let path_str = path.to_string_lossy().into_owned();
    let name = path
        .file_name()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_else(|| path_str.clone());
    let parent_path = path.parent().map(|p| p.to_string_lossy().into_owned());

    Ok(FileRecord {
        path: path_str,
        name,
        parent_path,
        file_type,
        size,
        dev,
        inode,
        nlink,
        mtime,
        scanned_at,
    })
}

#[cfg(unix)]
fn dev_inode_nlink(md: &std::fs::Metadata) -> (i64, i64, i64) {
    use std::os::unix::fs::MetadataExt;
    (md.dev() as i64, md.ino() as i64, md.nlink() as i64)
}

#[cfg(windows)]
fn dev_inode_nlink(md: &std::fs::Metadata) -> (i64, i64, i64) {
    // 不安定な Windows の by_handle API（file_index, number_of_links）は stable Rust では使えないため避ける。
    // 安定した代替として: dev は 0、inode はファイルサイズを粗い識別子として使用し、
    // number_of_links が利用できないため nlink は 1 を既定値とする。
    let dev: i64 = 0;
    let inode: i64 = md.len() as i64;
    let nlink: i64 = 1;
    (dev, inode, nlink)
}

fn current_unix_ts() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

fn compute_and_upsert_dir_stats(conn: &mut Connection) -> Result<()> {
    // Read all files (only regular files)
    let mut map: HashMap<String, (u64, u64)> = HashMap::new();

    // limit the scope of stmt and rows so they do not borrow `conn` when we need a mutable borrow later
    {
        let mut stmt =
            conn.prepare("SELECT path, parent_path, size FROM files WHERE file_type = 'file'")?;
        let rows = stmt.query_map([], |row| {
            let path: String = row.get(0)?;
            let parent: Option<String> = row.get(1)?;
            let size: i64 = row.get(2)?;
            Ok((path, parent, size))
        })?;

        for row_r in rows {
            let (_path, parent_opt, size_i64) = row_r?;
            let size = if size_i64 >= 0 { size_i64 as u64 } else { 0 };
            if let Some(mut dir) = parent_opt {
                loop {
                    let entry = map.entry(dir.clone()).or_insert((0u64, 0u64));
                    entry.0 += size;
                    entry.1 += 1;
                    if let Some(p) = Path::new(&dir).parent() {
                        let pstr = p.to_string_lossy().into_owned();
                        if pstr == dir {
                            break;
                        }
                        dir = pstr;
                    } else {
                        break;
                    }
                }
            }
        }
    }

    let tx = conn.transaction()?;
    let now = current_unix_ts();
    let mut upsert = tx.prepare(
        "INSERT INTO dir_stats (dir_path, total_size, file_count, last_scanned)
         VALUES (?1, ?2, ?3, ?4)
         ON CONFLICT(dir_path) DO UPDATE SET total_size=excluded.total_size, file_count=excluded.file_count, last_scanned=excluded.last_scanned",
    )?;

    for (dir_path, (total_size, file_count)) in map {
        upsert.execute(params![dir_path, total_size as i64, file_count as i64, now])?;
    }
    // drop the prepared statement to release the borrow on `tx` before committing
    drop(upsert);
    tx.commit()?;
    Ok(())
}
