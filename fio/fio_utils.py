# pylint: disable-all

import logging as log
import json

from utils import run_shell_command


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
    return ret.returncode
