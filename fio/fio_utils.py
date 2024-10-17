# pylint: disable-all

import logging as log

import json
import subprocess


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
    Run fio with the given config and job file
    """
    log.info("Parsing fio options")
    fio_config = parse_fio_config(fio_config)
    log.info(f"Running fio with the job file - {job_file}")
    fio_cmd = f"fio {job_file} --output-format={fio_config['output_format']}"
    ret = run_command(fio_cmd)
    if ret["status"] != 0:
        log.error(f"Error running fio - {ret['output']}")
    log.info(ret["output"])
    return ret["status"]


def run_command(cmd):
    """
    Run a command and return the output
    """
    output = None
    status = 0
    try:
        output = subprocess.check_output(cmd, stderr=subprocess.STDOUT, shell=True)
    except subprocess.CalledProcessError as e:
        log.error(f"Error running command - {cmd}")
        status = e.returncode

    return {"output": output, "status": status}
