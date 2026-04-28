use std::env;
use std::path::PathBuf;
use std::process::exit;

use redlinedb::{BackupOptions, Database, Step, ValueRef};

fn main() {
    if let Err(message) = run() {
        eprintln!("{message}");
        exit(1);
    }
}

fn run() -> Result<(), String> {
    let args = env::args().skip(1).collect::<Vec<_>>();
    if args.is_empty() || args.iter().any(|arg| arg == "--help" || arg == "-h") {
        print_help();
        return Ok(());
    }

    if args[0] == "backup" {
        if args.len() != 3 {
            return Err("usage: redlinedb backup SRC DST".to_owned());
        } else {
            let src = PathBuf::from(&args[1]);
            let dst = PathBuf::from(&args[2]);
            let db = Database::open(&src).map_err(|err| err.to_string())?;
            let _ = db
                .backup_to_path(dst, BackupOptions::default())
                .map_err(|err| err.to_string())?;
            return Ok(());
        }
    }

    if args[0] == "stats" {
        if args.len() < 2 {
            return Err("usage: redlinedb stats DB [--json]".to_owned());
        } else {
            let json = args.iter().any(|arg| arg == "--json");
            let db = Database::open(&args[1]).map_err(|err| err.to_string())?;
            let stats = db.stats().map_err(|err| err.to_string())?;
            if json {
                println!(
                    "{{\"schema_epoch\":{},\"resident_heap_pages\":{},\"wal_written_lsn\":{},\"wal_durable_lsn\":{}}}",
                    stats.schema_epoch,
                    stats.resident_heap_pages,
                    stats.wal_written_lsn,
                    stats.wal_durable_lsn
                );
            } else {
                println!("schema_epoch={}", stats.schema_epoch);
                println!("resident_heap_pages={}", stats.resident_heap_pages);
                println!("wal_written_lsn={}", stats.wal_written_lsn);
                println!("wal_durable_lsn={}", stats.wal_durable_lsn);
            }
            return Ok(());
        }
    }

    if args.len() >= 2 {
        let db = Database::open(&args[0]).map_err(|err| err.to_string())?;
        let sql = args[1..].join(" ");
        return run_query(db, &sql);
    }

    Err("usage: redlinedb DB SQL".to_owned())
}

fn run_query(db: Database, sql: &str) -> Result<(), String> {
    let mut conn = db.connect().map_err(|err| err.to_string())?;
    let mut stmt = conn.prepare(sql).map_err(|err| err.to_string())?;
    let column_count = stmt.column_count();
    while let Step::Row(row) = stmt.step().map_err(|err| err.to_string())? {
        let mut first = true;
        for index in 0..column_count {
            if !first {
                print!("\t");
            }
            first = false;
            match row.get_ref(index).map_err(|err| err.to_string())? {
                ValueRef::Null => print!("NULL"),
                ValueRef::Integer(value) => print!("{value}"),
                ValueRef::Real(value) => print!("{value}"),
                ValueRef::Text(value) => print!("{value}"),
                ValueRef::Blob(value) => print!("<blob:{}>", value.len()),
            }
        }
        println!();
    }
    Ok(())
}

fn print_help() {
    println!("redlinedb backup SRC DST");
    println!("redlinedb stats DB [--json]");
    println!("redlinedb DB SQL");
}
