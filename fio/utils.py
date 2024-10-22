# pylint: disable-all

import logging as log
import json
import tempfile
from utils import run_shell_command, convert_size, format_units_time, format_units_iops


def parse_fio_config(config):
    """
    Parse the fio config file and return a dictionary
    fio config will have all command line options
    required to run fio.
    """
    with open(config, 'r') as config_json:
        fio_config = json.load(config_json)
    log.info("Completed reading fio config file")
    return fio_config


def run_fio(fio_config, job_file):
    """
    Run fio tool with the given config and job file
    """
    log.info("Parsing fio options")
    fio_config = parse_fio_config(fio_config)
    log.info(f"Running fio with the job file - {job_file}")
    fio_cmd = f"fio {job_file} --output-format={fio_config['output_format']}"
    ret = run_shell_command(fio_cmd)
    if ret.returncode != 0:
        log.error(f"Error running fio - {ret.returncode}")
        return ret.stderr
    return ret.stdout


def fio_log_perf(operation, unit, value):
    """Format and log performance metrics."""
    log_formatter = {
        "latency": format_units_time,
        "iops": format_units_iops,
    }.get(unit, convert_size)
    per = "" if unit == "latency" else "/s"
    if operation == "average" and unit != "latency":
        per += " per host"
    if value != 0:
        log.info(f" {operation} {unit}: {log_formatter(value)}{per}")

def replace_fio_file_path(jobfile, file_path):
    """Create a temp file with the test_filepath required in specific tests"""
    with open(jobfile) as f:
        jobdata = f.read().replace("filename=/tmp", f"filename={file_path}")
    with tempfile.NamedTemporaryFile(delete=False, mode='w', suffix='.fio') as temp_jobfile:
        temp_jobfile.write(jobdata)
        temp_jobfile_path = temp_jobfile.name
    return temp_jobfile_path

class FioResult_Parser:
    """Parse and log FIO results."""

    def __init__(self, jobfile, results_str, reportitem):
        index = results_str.index('{')
        self.fio_output = json.loads(results_str[index:])
        self.summary = self.fio_output['jobs'][0]
        self.jobname = self.summary["jobname"]
        self.reportitem = reportitem

    def get_disk_stats(self):
        """Extract disk stats from the FIO output."""
        disk_stats = self.fio_output.get('disk_util', [])
        stats = {}
        for stat in disk_stats:
            device = stat.get('name')
            if device:
                stats[device] = {
                    "read_ios": stat.get('read_ios', 0),
                    "write_ios": stat.get('write_ios', 0),
                    "read_ticks": stat.get('read_ticks', 0),
                    "write_ticks": stat.get('write_ticks', 0),
                    "in_queue": stat.get('in_queue', 0),
                    "util": stat.get('util', 0.0)
                }
        return stats

    def summarize(self):
        metrics = {
            "bandwidth": {"read": "bw_bytes", "write": "bw_bytes"},
            "iops": {"read": "iops", "write": "iops"},
            "latency": {"read": "lat_ns", "write": "lat_ns"},
            "cpu": "usr_cpu",
            "disk_util": "disk_util"
        }
        for metric, keys in metrics.items():
            if self.reportitem.get(metric):
                if isinstance(keys, dict):
                    for op, key in keys.items():
                        value = self.summary[op][key] if metric != "latency" else self.summary[op][key]["mean"]
                        fio_log_perf(op, metric, value)
                    if metric != "latency":
                        total = sum(self.summary[op][key] for op in keys)
                        fio_log_perf("total", metric, total)
                        fio_log_perf("average", metric, total / len(keys))
                    else:
                        if (self.summary["read"]["lat_ns"]["mean"] > 0.0 and
                                self.summary["write"]["lat_ns"]["mean"] > 0.0):
                            avg_latency = (self.summary["read"]["lat_ns"]["mean"] + self.summary["write"]["lat_ns"][
                                "mean"]) / 2
                            fio_log_perf("average", metric, avg_latency)
                else:
                    value = self.summary.get(keys, None)
                    if value is not None:
                        fio_log_perf(metric, metric, value)
        # Log disk stats
        disk_stats = self.get_disk_stats()
        for device, stats in disk_stats.items():
            log.info(f"Disk stats for {device}: ios={stats['read_ios']}/{stats['write_ios']}, "
                     f"ticks={stats['read_ticks']}/{stats['write_ticks']}, in_queue={stats['in_queue']}, "
                     f"util={stats['util']:.2f}%")
        log.info("")

