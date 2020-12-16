#[macro_use]
extern crate clap;
use clap::App;

extern crate csv;
use csv::{Reader, StringRecord, Writer};

use std::collections::HashMap;
use std::error::Error;
use std::time::Instant;

fn find_key_position(headers: &StringRecord, key: &str) -> Result<usize, String> {
    for (index, value) in headers.iter().enumerate() {
        if value == key {
            return Ok(index);
        }
    }

    Err(format!("Key '{}' not found in {:?}", key, headers))
}

fn join_rows(from: &StringRecord, to: &StringRecord, exclude: usize) -> StringRecord {
    let mut row = StringRecord::new();
    for value in from.iter() {
        row.push_field(value);
    }

    for (index, value) in to.iter().enumerate() {
        if index != exclude {
            row.push_field(value);
        }
    }

    row
}

fn main() -> Result<(), Box<dyn Error>> {
    // The YAML file is found relative to the current file, similar to how modules are found
    let yaml = load_yaml!("cli.yml");
    let matches = App::from_yaml(yaml).get_matches();

    let from_file = matches.value_of("from").unwrap();
    let join_file = matches.value_of("join").unwrap();
    let join_key = matches.value_of("key").unwrap();
    let output_file = matches.value_of("output").unwrap();

    println!("Arguments:");
    println!("from: {}", from_file);
    println!("join: {}", join_file);
    println!("key: {}", join_key);
    println!("output: {}", output_file);

    let mut from_rdr = Reader::from_path(from_file)?;
    let mut join_rdr = Reader::from_path(join_file)?;
    let mut wtr = Writer::from_path(output_file)?;

    // Find the position of the join key in the "from" file
    let from_headers = from_rdr.headers()?;
    let from_index = find_key_position(from_headers, join_key)?;
    let join_headers = join_rdr.headers()?;
    let join_index = find_key_position(join_headers, join_key)?;

    println!(
        "Key {} found at index {} in {}",
        join_key, from_index, from_file
    );
    println!(
        "Key {} found at index {} in {}",
        join_key, join_index, join_file
    );

    // Prepare the combined headers of the output
    let headers = join_rows(from_headers, join_headers, join_index);

    // Map the joined data onto a hashmap
    println!("Parsing joined CSV file into hashmap ...");
    let parsing_start = Instant::now();

    let mut hashed_records = HashMap::new();
    let mut join_records_count = 0;
    let mut join_failed_records_count = 0;
    // TODO(pap): check if there are duplicate keys in the join table.
    // In the current state, the values of the last row of a given key
    // will be used during the join.
    for result in join_rdr.records() {
        join_records_count += 1;
        let record = result?;
        if let Some(key) = record.get(join_index) {
            hashed_records.insert(key.to_string(), record.clone());
        } else {
            join_failed_records_count += 1;
        }
    }
    println!("Done. Took {} ms", parsing_start.elapsed().as_millis());
    println!(
        "Total Records in join CSV: {} (failed: {})",
        join_records_count, join_failed_records_count
    );

    let writing_start = Instant::now();
    println!("Writing to output file {}", output_file);
    // Write headers to the output file
    wtr.write_record(&headers)?;

    // Iterate over the rows in from table and append data from the join table
    let mut from_records_count = 0;
    let mut output_records_count = 0;
    let mut no_key_in_join_count = 0;
    for result in from_rdr.records() {
        from_records_count += 1;
        let from_record = result?;
        if let Some(key) = from_record.get(from_index) {
            if let Some(to_record) = hashed_records.get(key) {
                output_records_count += 1;
                let row = join_rows(&from_record, &to_record, join_index);
                wtr.write_record(&row)?;
            } else {
                no_key_in_join_count += 1;
            }
        }
    }
    wtr.flush()?;
    println!("Done. Took {} ms", writing_start.elapsed().as_millis());
    println!("From records: {}", from_records_count);
    println!("Output records: {}", output_records_count);
    println!(
        "Records in from with no key in join: {}",
        no_key_in_join_count
    );

    Ok(())
}
