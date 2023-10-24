use std::fs::File;
use std::error::Error;
use std::path::{Path, PathBuf};
use std::collections::HashMap;
use std::io::{BufWriter, Write};
use std::str;
use csv;
use clap::{Command, Arg, ArgAction};


const HEADER: [&str; 85] = ["timestamp", "datetime", "event_id", "alias", "answered_by_default", "cell_area",
    "cell_key", "cell_product_id", "cell_serial_number", "cell_session_id", "check_response_code", "choices",
    "connection_name", "container_name", "current_sequence", "default_answer", "enabled", "end_time",
    "error_message", "event_category", "event_message", "event_type", "file_name", "function_name", "headers",
    "host", "input_dict", "iteration_count", "iteration_id", "jump_on_branch", "jump_on_error", "key",
    "level_name", "libname", "limit_def", "limit_id", "limit_type", "line_number", "machine_name", "measure_time",
    "media_url", "module_name", "module_path", "multi_select", "name", "object_type", "parallel_steps",
    "path_name", "port", "protocol", "question", "question_id", "regex", "response", "result_pass_fail",
    "runtime_secs", "sequence_key", "serial_number", "session_id", "setup", "start_time", "status", "status_code",
    "step_iteration", "step_key", "steps_completed", "stop_on_error", "system_log", "teardown", "test_area",
    "test_cell", "test_container", "test_id", "test_record_time", "test_step_id", "test_unique_id",
    "total_iteration_count", "traceback", "uid", "url", "user", "uuid", "uut_type", "value", "wildcard"];
const REQUIRED_COLUMN: [&str; 11] = ["event_category", "event_type", "connection_name", "timestamp",
    "module_name", "line_number", "cell_key", "step_key", "level_name", "event_message", "response"];

fn csv2logs(csv_file_path: &String) -> Result<(), Box<dyn Error>> {
    println!(" - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -");
    println!("csv file: {csv_file_path}");
    let _header = csv::StringRecord::from(Vec::from(HEADER));

    let file = File::open(csv_file_path)?;
    let mut rdr = csv::Reader::from_reader(file);
    let header = rdr.headers()?;
    if header != &_header {
        eprintln!("Warning: BQ4 csv logs format verify fail, format update ?");
    } else {
        println!("BQ4 csv logs format verify pass");
    }
    let z = header.iter().collect::<Vec<_>>();
    for x in REQUIRED_COLUMN {
        if !z.contains(&x) {
            eprintln!("Error  : csv header not contain required column \"{x}\", Skip this csv file");
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

    let mut log_line_count: HashMap<String, i32> = HashMap::new();
    let mut log_name_file_map: HashMap<String, BufWriter<File>> = HashMap::new();

    let seq = String::from("sequence");
    let seq_file_path = format!("{}-{seq}.log", csv_file_path[..csv_file_path.len() - 4].to_string());
    log_line_count.insert(seq.clone(), 0);
    let seq_file = File::create(seq_file_path)?;
    let mut seq_file = BufWriter::new(seq_file);
    let mut line = csv::ByteRecord::new();
    while rdr.read_byte_record(&mut line)? {
        let event_category = &line[event_category_ix];
        match event_category {
            b"seqlog" => {
                seq_file.write_all(&line[timestamp_ix])?;
                write!(&mut seq_file, " {:<24}", str::from_utf8(&line[module_name_ix]).unwrap())?;
                write!(&mut seq_file, " line:{:<4} ", str::from_utf8(&line[line_number_ix]).unwrap())?;
                seq_file.write_all(&line[cell_key_ix])?;
                seq_file.write_all(b" ")?;
                let step_key = &line[step_key_ix];
                match step_key[..].iter().rposition(|&x| x == b"|"[0]) {
                    Some(x) => seq_file.write_all(&step_key[x + 1..])?,
                    None => seq_file.write_all(&step_key)?,
                };
                write!(&mut seq_file, " {:<8}: ", str::from_utf8(&line[level_name_ix]).unwrap())?;
                seq_file.write_all(&line[event_message_ix])?;
                seq_file.write_all(b"\n")?;
                if let Some(x) = log_line_count.get_mut(&seq) { *x = *x + 1; }
            }
            b"cesium-service" => {
                let _module_name = match line.get(module_name_ix) {
                    Some(x) if !x[..].is_empty() => x,
                    _ => b"cesiumlib",
                };
                seq_file.write_all(&line[timestamp_ix])?;
                write!(&mut seq_file, " {:<24} ", str::from_utf8(_module_name).unwrap())?;
                write!(&mut seq_file, "line:{:<4} ", str::from_utf8(&line[line_number_ix]).unwrap())?;
                seq_file.write_all(&line[cell_key_ix])?;
                seq_file.write_all(b" ")?;
                let step_key = &line[step_key_ix];
                match step_key[..].iter().rposition(|&x| x == b"|"[0]) {
                    Some(x) => seq_file.write_all(&step_key[x + 1..])?,
                    None => seq_file.write_all(&step_key)?,
                };
                write!(&mut seq_file, " {:<8}: ", str::from_utf8(&line[level_name_ix]).unwrap())?;
                seq_file.write_all(&line[response_ix])?;
                seq_file.write_all(b"\n")?;
                if let Some(x) = log_line_count.get_mut(&seq) { *x = *x + 1; }
            }
            b"connection" => {
                let conn_name = str::from_utf8(&line[connection_name_ix]).unwrap();
                if !log_name_file_map.contains_key(conn_name) {
                    let _conn_file = format!("{}-{conn_name}.log", csv_file_path[..csv_file_path.len() - 4].to_string());
                    let _conn_file = File::create(_conn_file)?;
                    let mut _conn_file = BufWriter::new(_conn_file);
                    _conn_file.write_all(b"timestamp               event_type    event_message\n")?;
                    log_name_file_map.insert(conn_name.to_string(), _conn_file);
                    log_line_count.insert(conn_name.to_string(), 2);
                };
                if let Some(_conn_file) = log_name_file_map.get_mut(conn_name) {
                    let timestamp = &line[timestamp_ix];
                    let event_type = format!(" {:<9} ", str::from_utf8(&line[event_type_ix]).unwrap());
                    for msg in line[event_message_ix][..].split(|&x| x == b"\n"[0]) {
                        _conn_file.write_all(timestamp)?;
                        _conn_file.write_all(event_type.as_bytes())?;
                        _conn_file.write_all(msg)?;
                        _conn_file.write_all(b"\n")?;
                    };
                    if let Some(x) = log_line_count.get_mut(conn_name) { *x += 1; }
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
        let _file_path = format!("{}-{k}.log", csv_file_path[..csv_file_path.len() - 4].to_string());
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
        .arg(Arg::new("filepath_or_folder").action(ArgAction::Append).required(true))
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
        if let Err(e) = csv2logs(x) {
            eprintln!("fn csv2logs error: {e}");
        }
    }
}
