use serde::Deserialize;
use csv::ReaderBuilder;
use rayon::prelude::*;
use std::error::Error;

#[derive(Debug, Deserialize, Clone)]
struct Record {
    #[serde(rename = "Date")]
    date: String,
    #[serde(rename = "State")]
    state: String,
    #[serde(rename = "Region")]
    region: String,
    #[serde(rename = "Confirmed")]
    confirmed: u64,
    #[serde(rename = "Deaths")]
    deaths: u64,
    #[serde(rename = "Recovered")]
    recovered: u64,
}

fn process_csv(path: &str) -> Result<(), Box<dyn Error>> {
    let mut rdr = ReaderBuilder::new()
        .has_headers(true)
        .from_path(path)?;

    // Deserialize into our concrete schema and report any row-level errors instead of silently dropping them
    let mut records: Vec<Record> = Vec::new();
    for result in rdr.deserialize::<Record>() {
        match result {
            Ok(rec) => records.push(rec),
            Err(err) => eprintln!("Failed to parse record: {}", err),
        }
    }

    // Process records in parallel (example: filter by confirmed cases)
    let filtered: Vec<_> = records
        .par_iter()
        .filter(|r| r.confirmed > 1000)
        .cloned()
        .collect();

    for rec in filtered {
        println!("{:?}", rec);
    }

    Ok(())
}

fn main() {
    if let Err(err) = process_csv("covid_19_data.csv") {
        eprintln!("Error processing CSV: {}", err);
    }
}
