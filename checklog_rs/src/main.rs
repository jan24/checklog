use clap::{Arg, ArgAction, Command};
use csv_index::RandomAccessSimple;
use rustc_hash::{FxHashMap, FxHashSet};
use std::error::Error;
use std::fs::File;
use std::io::{Cursor, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::thread;
use std::thread::ScopedJoinHandle;

const THREAD_COUNT: u64 = 4;
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

struct RowReader<'a> {
    csv_path: &'a str,
    header_ix: [usize; 11],
    task_count: u64,
    task_seq: u64,
}

impl RowReader<'_> {
    pub fn new(
        csv_path: &str,
        header_ix: [usize; 11],
        task_count: u64,
        task_seq: u64,
    ) -> RowReader {
        RowReader {
            csv_path,
            header_ix,
            task_count,
            task_seq,
        }
    }
    pub fn get_reader(&self) -> Result<(csv::Reader<File>, u64, u64), Box<dyn Error>> {
        // return the reader, can be iter from start position
        let mut reader = csv::Reader::from_path(self.csv_path)?;
        let mut wtr: Cursor<Vec<u8>> = Cursor::new(Vec::new());
        RandomAccessSimple::create(&mut reader, &mut wtr)?;
        let mut idx = RandomAccessSimple::open(wtr)?;
        let rows_count = idx.len();
        let (start, end) = self.get_range(rows_count);
        let start_position = idx.get(start).unwrap();
        reader.seek(start_position)?;
        Ok((reader, start, end))
    }
    pub fn get_range(&self, rows_count: u64) -> (u64, u64) {
        // rows: len of all rows, contain header
        // task_count >=2
        // seq: >=1 and <= task_count
        // return the row index range for every task
        let x = rows_count / self.task_count;
        let start = 1 + x * (self.task_seq - 1);
        let end = if self.task_count == self.task_seq {
            1 + rows_count
        } else {
            1 + self.task_seq * x
        };
        (start, end)
    }
}

type LogCountMap = FxHashMap<String, i32>;
type LogfileMap = FxHashMap<String, Cursor<Vec<u8>>>;

fn process_rows(
    task: RowReader,
    mlock: Arc<Mutex<FxHashSet<String>>>,
) -> Result<(LogCountMap, LogfileMap), Box<dyn Error>> {
    let [event_category_ix, event_type_ix, connection_name_ix, timestamp_ix, module_name_ix, line_number_ix, cell_key_ix, step_key_ix, level_name_ix, event_message_ix, response_ix] =
        task.header_ix;

    let mut log_line_count_map: LogCountMap = FxHashMap::default();
    let mut log_name_file_map: LogfileMap = FxHashMap::default();
    let mut line = csv::StringRecord::with_capacity(256, 85);

    let mut seq_file: Cursor<Vec<u8>> = Cursor::new(Vec::new());
    log_line_count_map.insert("sequence".to_string(), 0);
    {
        let mut set = mlock.lock().unwrap();
        set.insert("sequence".to_string());
    }

    let (mut reader, start, end) = task.get_reader().unwrap();
    for _ in start..end {
        reader.read_record(&mut line)?;
        let event_category = line.get(event_category_ix);
        let event_category = match event_category {
            Some(x) => x,
            None => continue,
        };
        match event_category {
            "seqlog" => {
                write!(seq_file, "{}", &line[timestamp_ix])?;
                write!(seq_file, " {:<24}", &line[module_name_ix])?;
                write!(seq_file, " line:{:<4}", &line[line_number_ix])?;
                write!(seq_file, " {} ", &line[cell_key_ix])?;
                let sp: Vec<&str> = line[step_key_ix].split('|').collect();
                write!(seq_file, "{}", sp[sp.len() - 1])?;
                write!(seq_file, " {:<8}: ", &line[level_name_ix])?;
                writeln!(seq_file, "{}", &line[event_message_ix])?;
                if let Some(x) = log_line_count_map.get_mut("sequence") {
                    *x += 1;
                }
            }
            "cesium-service" => {
                let mut _module_name = &line[module_name_ix];
                if _module_name.is_empty() {
                    _module_name = "cesiumlib";
                }
                write!(seq_file, "{}", &line[timestamp_ix])?;
                write!(seq_file, " {_module_name:<24} ")?;
                write!(seq_file, "line:{:<4}", &line[line_number_ix])?;
                write!(seq_file, " {} ", &line[cell_key_ix])?;
                let sp: Vec<&str> = line[step_key_ix].split('|').collect();
                write!(seq_file, "{}", sp[sp.len() - 1])?;
                write!(seq_file, " {:<8}: ", &line[level_name_ix])?;
                writeln!(seq_file, "{}", &line[response_ix])?;
                if let Some(x) = log_line_count_map.get_mut("seqlog") {
                    *x += 1;
                }
            }
            "connection" => {
                let conn_name = &line[connection_name_ix];
                if !log_name_file_map.contains_key(conn_name) {
                    let mut _conn_file = Cursor::new(Vec::new());
                    log_line_count_map.insert(conn_name.to_string(), 0);
                    log_line_count_map.insert(conn_name.to_string(), 0);
                    log_name_file_map.insert(conn_name.to_string(), _conn_file);
                    {
                        let mut set = mlock.lock().unwrap();
                        set.insert(conn_name.to_string());
                    }
                };
                if let Some(_conn_file) = log_name_file_map.get_mut(conn_name) {
                    let timestamp = &line[timestamp_ix];
                    let event_type = format!(" {:<9} ", &line[event_type_ix]);
                    let event_msg = &line[event_message_ix];
                    for s in event_msg.lines() {
                        write!(_conn_file, "{}", timestamp)?;
                        write!(_conn_file, "{}", event_type)?;
                        writeln!(_conn_file, "{}", s)?;
                    }
                    if let Some(x) = log_line_count_map.get_mut(conn_name) {
                        *x += 1;
                    }
                }
            }
            _ => {}
        }
    }
    log_name_file_map.insert("sequence".to_string(), seq_file);
    Ok((log_line_count_map, log_name_file_map))
}

fn verify_header(csv_file_path: &String) -> Result<Option<[usize; 11]>, Box<dyn Error>> {
    println!("csv file: {csv_file_path}");
    let mut rdr = csv::Reader::from_path(csv_file_path).unwrap();
    let header = rdr.headers()?;
    let z = header.iter().collect::<Vec<_>>();
    for x in REQUIRED_COLUMN {
        if !z.contains(&x) {
            eprintln!(
                "Error  : csv header not contain required column \"{x}\", Skip this csv file"
            );
            return Ok(None);
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
    let header_ix = [
        event_category_ix,
        event_type_ix,
        connection_name_ix,
        timestamp_ix,
        module_name_ix,
        line_number_ix,
        cell_key_ix,
        step_key_ix,
        level_name_ix,
        event_message_ix,
        response_ix,
    ];
    Ok(Some(header_ix))
}

fn csv2logs(csv_file_path: &String) -> Result<(), Box<dyn Error>> {
    //
    let header_ix = match verify_header(csv_file_path)? {
        Some(x) => x,
        None => return Ok(()),
    };

    let conn_set: Arc<Mutex<FxHashSet<String>>> = Arc::new(Mutex::new(FxHashSet::default()));
    let thread_r = thread::scope(|scope| {
        let mut scope_vec: Vec<Box<ScopedJoinHandle<(LogCountMap, LogfileMap)>>> = Vec::new();
        for i in 1..=THREAD_COUNT {
            let task = RowReader::new(csv_file_path, header_ix, THREAD_COUNT, i);
            let lock = Arc::clone(&conn_set);
            let scopejh = scope.spawn(move || process_rows(task, lock).unwrap());
            scope_vec.push(Box::new(scopejh));
        }
        scope_vec.into_iter().map(|x| x.join()).collect::<Vec<_>>()
    });

    let conn_name_set = || {
        let lock = Arc::clone(&conn_set);
        let set = lock.lock().unwrap();
        set.clone().into_iter().collect::<Vec<String>>()
    };
    let mut conn_name_set = conn_name_set();
    println!("Output {} logs:", conn_name_set.len());
    println!("{:<20}{:<20}Log_file_path", "Log_name", "Lines_count");
    let mut thread_r: Vec<(LogCountMap, LogfileMap)> =
        thread_r.into_iter().map(|x| x.unwrap()).collect::<Vec<_>>();

    let thread_r = &mut thread_r;
    let conn_name_set = &mut conn_name_set;
    thread::scope(|scope| {
        for conn_name in conn_name_set.iter_mut() {
            let file_path = format!(
                "{}-{}.log",
                &csv_file_path[..csv_file_path.len() - 4],
                &conn_name
            );
            scope.spawn(|| {
                let thread_r = &thread_r;
                let file_path = file_path;
                let conn_name = conn_name;
                let mut file = File::create(&file_path).unwrap();
                let mut i = 0;
                let mut flag = false;
                for result in thread_r.iter() {
                    let log_line_count_map = &result.0;
                    let log_name_file_map = &result.1;
                    if let Some(x) = log_line_count_map.get(conn_name) {
                        if !flag && conn_name != "sequence" {
                            i += 1;
                        }
                        i += x;
                    }
                    if let Some(v) = log_name_file_map.get(conn_name) {
                        let _v = v.get_ref();
                        if !flag && conn_name != "sequence"{
                            let _ = file.write_all(
                                b"timestamp               event_type    event_message\n",
                            );
                            flag = true;
                        }
                        let _ = file.write_all(&_v[..]);
                    }
                }
                println!("{conn_name:<20}{i:<20}{file_path}");
            });
        }
    });
    Ok(())
}

fn get_csv_file(paths: Vec<&str>) -> Result<Vec<String>, Box<dyn Error>> {
    let mut csv_files: Vec<PathBuf> = Vec::new();
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
        println!("    {x}");
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
