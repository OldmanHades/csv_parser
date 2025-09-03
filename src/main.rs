use serde::Deserialize;
use csv::ReaderBuilder;
use rayon::prelude::*;
use std::error::Error;

#[derive(Debug, Deserialize, Clone)]
struct Record {
    id: u32,
    name: String,
    value: f64,
}

fn process_csv(path: &str) -> Result<(), Box<dyn Error>> {
    let mut rdr = ReaderBuilder::new()
        .has_headers(true)
        .from_path(path)?;

    let records: Vec<Record> = rdr.deserialize()
        .filter_map(Result::ok)
        .collect();

    // Process records in parallel
    let filtered: Vec<_> = records.par_iter()
        .filter(|r| r.value > 100.0)
        .cloned()
        .collect();

    for rec in filtered {
        println!("{:?}", rec);
    }

    Ok(())
}

fn main() {
    if let Err(err) = process_csv("data.csv") {
        eprintln!("Error processing CSV: {}", err);
    }
}