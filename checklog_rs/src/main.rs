use clap::{Arg, ArgAction, Command};
use rustc_hash::FxHashMap;
use std::error::Error;
use std::fmt::Write as _;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

const REQUIRED_COLUMN: [&str; 11] = [
    "event_category",
    "event_type",
    "connection_name",
    "timestamp",
    "module_name",
    "line_number",
    "cell_key",
    "step_key",
    "level_name",
    "event_message",
    "response",
];

fn csv2logs(csv_file_path: &String) -> Result<(), Box<dyn Error>> {
    println!("csv file: {csv_file_path}");

    let file = File::open(csv_file_path)?;
    let mut rdr = csv::Reader::from_reader(file);
    let header = rdr.headers()?;
    let z = header.iter().collect::<Vec<_>>();
    for x in REQUIRED_COLUMN {
        if !z.contains(&x) {
            eprintln!(
                "Error  : csv header not contain required column \"{x}\", Skip this csv file"
            );
            return Ok(());
        }
    }

    let event_category_ix = header.iter().position(|x| x == "event_category").unwrap();
    let event_type_ix = header.iter().position(|x| x == "event_type").unwrap();
    let connection_name_ix = header.iter().position(|x| x == "connection_name").unwrap();
    let timestamp_ix = header.iter().position(|x| x == "timestamp").unwrap();
    let module_name_ix = header.iter().position(|x| x == "module_name").unwrap();
    let line_number_ix = header.iter().position(|x| x == "line_number").unwrap();
    let cell_key_ix = header.iter().position(|x| x == "cell_key").unwrap();
    let step_key_ix = header.iter().position(|x| x == "step_key").unwrap();
    let level_name_ix = header.iter().position(|x| x == "level_name").unwrap();
    let event_message_ix = header.iter().position(|x| x == "event_message").unwrap();
    let response_ix = header.iter().position(|x| x == "response").unwrap();

    let mut log_line_count: FxHashMap<String, i32> = FxHashMap::default();
    let mut log_name_file_map: FxHashMap<String, BufWriter<File>> = FxHashMap::default();

    let seq = String::from("sequence");
    log_line_count.insert(seq.clone(), 0);
    let seq_file_path = format!("{}-{seq}.log", &csv_file_path[..csv_file_path.len() - 4]);
    let seq_file = File::create(seq_file_path)?;
    let mut seq_file = BufWriter::new(seq_file);
    let mut line = csv::StringRecord::with_capacity(256, 85);
    let mut line_string = String::with_capacity(256);
    while rdr.read_record(&mut line)? {
        let event_category = &line[event_category_ix];
        match event_category {
            "seqlog" => {
                line_string.clear();
                write!(&mut line_string, "{}", &line[timestamp_ix])?;
                write!(&mut line_string, " {:<24}", &line[module_name_ix])?;
                write!(&mut line_string, " line:{:<4}", &line[line_number_ix])?;
                write!(&mut line_string, " {} ", &line[cell_key_ix])?;
                let sp: Vec<&str> = line[step_key_ix].split('|').collect();
                write!(&mut line_string, "{}", sp[sp.len() - 1])?;
                write!(&mut line_string, " {:<8}: ", &line[level_name_ix])?;
                writeln!(&mut line_string, "{}", &line[event_message_ix])?;
                seq_file.write_all(line_string.as_bytes())?;
                if let Some(x) = log_line_count.get_mut(&seq) {
                    *x += 1;
                }
            }
            "cesium-service" => {
                let mut _module_name = &line[module_name_ix];
                if _module_name.is_empty() {
                    _module_name = "cesiumlib";
                }
                line_string.clear();
                write!(&mut line_string, "{}", &line[timestamp_ix])?;
                write!(&mut line_string, " {_module_name:<24} ")?;
                write!(&mut line_string, "line:{:<4}", &line[line_number_ix])?;
                write!(&mut line_string, " {} ", &line[cell_key_ix])?;
                let sp: Vec<&str> = line[step_key_ix].split('|').collect();
                write!(&mut line_string, "{}", sp[sp.len() - 1])?;
                write!(&mut line_string, " {:<8}: ", &line[level_name_ix])?;
                writeln!(&mut line_string, "{}", &line[response_ix])?;
                seq_file.write_all(line_string.as_bytes())?;
                if let Some(x) = log_line_count.get_mut(&seq) {
                    *x += 1;
                }
            }
            "connection" => {
                let conn_name = &line[connection_name_ix];
                if !log_name_file_map.contains_key(conn_name) {
                    let _conn_file = format!(
                        "{}-{conn_name}.log",
                        &csv_file_path[..csv_file_path.len() - 4]
                    );
                    let _conn_file = File::create(_conn_file)?;
                    let mut _conn_file = BufWriter::new(_conn_file);
                    _conn_file.write_all(
                        "timestamp               event_type    event_message\n".as_bytes(),
                    )?;
                    log_name_file_map.insert(conn_name.to_string(), _conn_file);
                    log_line_count.insert(conn_name.to_string(), 1);
                };
                if let Some(_conn_file) = log_name_file_map.get_mut(conn_name) {
                    line_string.clear();
                    let timestamp = &line[timestamp_ix];
                    let event_type = format!(" {:<9} ", &line[event_type_ix]);
                    let event_msg = &line[event_message_ix];
                    for s in event_msg.lines() {
                        write!(&mut line_string, "{}", timestamp)?;
                        write!(&mut line_string, "{}", event_type)?;
                        writeln!(&mut line_string, "{}", s)?;
                    }
                    _conn_file.write_all(line_string.as_bytes())?;
                    if let Some(x) = log_line_count.get_mut(conn_name) {
                        *x += 1;
                    }
                }
            }
            _ => {}
        }
    }
    seq_file.flush()?;
    for (_k, v) in log_name_file_map.iter_mut() {
        v.flush()?;
    }
    println!("Output {} logs:", log_line_count.len());
    println!("{:<20}{:<20}Log_file_path", "Log_name", "Lines_count");
    for (k, v) in log_line_count.iter() {
        let _file_path = format!("{}-{k}.log", &csv_file_path[..csv_file_path.len() - 4]);
        println!("{k:<20}{v:<20}{_file_path}");
    }
    Ok(())
}

fn get_csv_file(paths: Vec<&str>) -> Result<Vec<String>, Box<dyn Error>> {
    let mut csv_files: Vec<PathBuf> = vec![];
    for _x in paths.iter() {
        let x = Path::new(_x);
        if x.is_file() {
            match x.extension() {
                None => println!("Not .csv file: {}", x.display()),
                Some(_x) => {
                    if _x == "csv" {
                        csv_files.push(x.to_path_buf());
                    } else {
                        println!("Not .csv file: {}", x.display());
                    }
                }
            }
        } else if x.is_dir() {
            for sub in x.read_dir()? {
                let sub = sub?.path();
                match sub.extension() {
                    None => println!("Not .csv file: {}", sub.display()),
                    Some(x) => {
                        if x == "csv" {
                            csv_files.push(sub);
                        } else {
                            println!("Not .csv file: {}", sub.display());
                        }
                    }
                }
            }
        } else {
            println!("Not exist    : {_x}");
        }
    }
    let csv_files = csv_files
        .iter()
        .map(|x| x.clone().into_os_string().into_string().unwrap())
        .collect::<Vec<_>>();
    Ok(csv_files)
}

fn main() {
    let cli = Command::new("MyApp")
        .version("1.0")
        .about("Convent BQ4 Dftium .csv logs to BQ3 style logs")
        .arg(
            Arg::new("filepath_or_folder")
                .action(ArgAction::Append)
                .required(true),
        )
        .after_help("Examples: ./checklog.exe xxx.csv")
        .get_matches();

    let filepath_or_folder = cli
        .get_many::<String>("filepath_or_folder")
        .unwrap_or_default()
        .map(|x| x.as_str())
        .collect::<Vec<_>>();

    let csv_files = get_csv_file(filepath_or_folder).unwrap();
    println!(" - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -");
    println!("trying to process {} csv files:", csv_files.len());
    for x in csv_files.iter() {
        println!("    {}", x);
    }
    for x in csv_files.iter() {
        println!(
            " - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -"
        );
        if let Err(e) = csv2logs(x) {
            eprintln!("fn csv2logs error: {e}");
        }
    }
}
