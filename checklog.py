# -*- coding: utf-8 -*-
# Python 3.10
"""Convent BQ4 Dftium .csv logs to BQ3 style logs"""
import csv
import argparse
from pathlib import Path

HEADER = ['timestamp', 'datetime', 'event_id', 'alias', 'answered_by_default', 'cell_area', 'cell_key',
          'cell_product_id', 'cell_serial_number', 'cell_session_id', 'check_response_code', 'choices',
          'connection_name', 'container_name', 'current_sequence', 'default_answer', 'enabled', 'end_time',
          'error_message', 'event_category', 'event_message', 'event_type', 'file_name', 'function_name', 'headers',
          'host', 'input_dict', 'iteration_count', 'iteration_id', 'jump_on_branch', 'jump_on_error', 'key',
          'level_name', 'libname', 'limit_def', 'limit_id', 'limit_type', 'line_number', 'machine_name', 'measure_time',
          'media_url', 'module_name', 'module_path', 'multi_select', 'name', 'object_type', 'parallel_steps',
          'path_name', 'port', 'protocol', 'question', 'question_id', 'regex', 'response', 'result_pass_fail',
          'runtime_secs', 'sequence_key', 'serial_number', 'session_id', 'setup', 'start_time', 'status', 'status_code',
          'step_iteration', 'step_key', 'steps_completed', 'stop_on_error', 'system_log', 'teardown', 'test_area',
          'test_cell', 'test_container', 'test_id', 'test_record_time', 'test_step_id', 'test_unique_id',
          'total_iteration_count', 'traceback', 'uid', 'url', 'user', 'uuid', 'uut_type', 'value', 'wildcard']

REQUIRED_COLUMN = ["event_category", "event_type", "connection_name", "timestamp",
    "module_name", "line_number", "cell_key", "step_key", "level_name", "event_message", "response"]
def sp_event_msg(timestamp: str, event_type: str, event_message: str):
    """
    event_message may contains \r\n
    """
    r = []
    for _x in event_message.splitlines():
        r.append(f"{timestamp} {event_type:<10}{_x}\n")
    return r


def get_csv_file(paths):
    """Get csv files from given path, will not through a directory recursively"""
    csv_files = []
    for _x in paths:
        x = Path(_x)
        if x.is_file():
            if x.suffix == ".csv":
                csv_files.append(x)
            else:
                print(f"Not .csv file: {x}")
        elif x.is_dir():
            for sub in x.iterdir():
                if sub.suffix == ".csv":
                    csv_files.append(sub)
                else:
                    print(f"Not .csv file: {sub}")
        else:
            print(f"Not exist    : {_x}")
    return csv_files


def csv2logs(csv_full_path: str | Path):
    print(" -" * 40)
    print(f"csv file: {csv_full_path}")
    csv_full_path = str(csv_full_path)
    with open(csv_full_path, "r", encoding="utf-8") as f:
        csv_reader = csv.reader(f)
        header = next(csv_reader)
        if header != HEADER:
            print("Warning: BQ4 csv logs format verify fail, format update ?")
        else:
            print("BQ4 csv logs format verify pass")
        for x in REQUIRED_COLUMN:
            if x not in header:
                print(f"Error  : csv header not contain required column \"{x}\", Skip this csv file")
                return
        event_category_ix = header.index("event_category")
        event_type_ix = header.index("event_type")
        connection_name_ix = header.index("connection_name")
        timestamp_ix = header.index("timestamp")
        module_name_ix = header.index("module_name")
        line_number_ix = header.index("line_number")
        cell_key_ix = header.index("cell_key")
        step_key_ix = header.index("step_key")
        level_name_ix = header.index("level_name")
        event_message_ix = header.index("event_message")
        response_ix = header.index("response")

        log_line_count = {}
        log_name_file_map = {}  # log_name: log_file_handler

        seq = "sequence"
        seq_file = open(f"{csv_full_path[:-4]}-{seq}.log", "w", encoding="utf-8", newline="")

        log_line_count[seq] = 0
        log_name_file_map[seq] = seq_file
        for row in csv_reader:
            if row[event_category_ix] == "seqlog":
                log_line_count[seq] += 1
                seq_file.write(f"{row[timestamp_ix]:<24}{row[module_name_ix]:<24}")
                seq_file.write(f" line:{row[line_number_ix]:<4} {row[cell_key_ix]} {row[step_key_ix].split('|')[-1]}")
                seq_file.write(f" {row[level_name_ix]:<8}: {row[event_message_ix]}\n")
            elif row[event_category_ix] == "cesium-service":
                log_line_count[seq] += 1
                seq_file.write(f"{row[timestamp_ix]:<24}{row[module_name_ix] or 'cesiumlib':<24}")
                seq_file.write(f" line:{row[line_number_ix]:<4} {row[cell_key_ix]} {row[step_key_ix].split('|')[-1]}")
                seq_file.write(f" {row[level_name_ix]:<8}: {row[response_ix]}\n")
            elif row[event_category_ix] == "connection":
                conn_name = row[connection_name_ix]
                if conn_name in log_name_file_map:
                    _conn_file = log_name_file_map[conn_name]
                    log_line_count[conn_name] += 1
                    for x in sp_event_msg(row[timestamp_ix], row[event_type_ix], row[event_message_ix]):
                        _conn_file.write(x)
                else:
                    _conn_file = open(f"{csv_full_path[:-4]}-{conn_name}.log", "w", encoding="utf-8")
                    _conn_file.write(f"{'timestamp':<24}{'event_type    '}{'event_message'}\n")
                    for x in sp_event_msg(row[timestamp_ix], row[event_type_ix], row[event_message_ix]):
                        _conn_file.write(x)
                    log_line_count[conn_name] = 2
                    log_name_file_map[conn_name] = _conn_file
        [f.close() for f in log_name_file_map.values()]
    print(f"Output {len(log_line_count)} logs:")
    print(f"{'Log_name':<20}{'Lines_count':<20}{'Log_file_path'}")
    for k, v in log_line_count.items():
        print(f"{k:<20}{v:<20}{csv_full_path[:-4]}-{k}.log")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Convent BQ4 Dftium .csv logs to BQ3 style logs",
                                     epilog="example: python checklog.py xxx.csv")
    parser.add_argument("filepath_or_folder", nargs="+", )
    args = parser.parse_args()
    csv_files = get_csv_file(args.filepath_or_folder)

    print(" -" * 40)
    print(f"trying to process {len(csv_files)} csv files:")
    for x in csv_files:
        print(f"    {x}")

    for x in csv_files:
        csv2logs(x)
