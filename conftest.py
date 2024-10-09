""" Shared pytest fixtures.  These fixtures are available to all tests """
# pylint: disable=import-outside-toplevel
import logging as log
import os
import re
import time
import sys
import json
import pytest

if not os.path.abspath(os.path.dirname(__file__)) in sys.path:
    sys.path.append(os.path.abspath(os.path.dirname(__file__)))
from utils import collect_diags

pytest.context = {}


def pytest_sessionstart(session):
    """
    Called after the Session object has been created and
    before performing collection and entering the run test loop.
    """
    log.info(f'pytest session start: name: {session.name}, path: {session.path}')
    # Setting up environment in context
    with open("test_env.json", encoding='utf8') as file:
        pytest.context['environment'] = json.load(file)

    pytest.context["diags-on-exit"] = session.config.getoption("--diags-on-exit")
    pytest.context["diags-on-fail"] = session.config.getoption("--diags-on-fail")
    pytest.context["diags-on-pass"] = session.config.getoption("--diags-on-pass")


@pytest.fixture(autouse=True)
def log_test(request):
    """ pytest fixture for logging the test name when it starts and completes

    It also handles per test diagnostic collection

    This fixture is marked as 'autouse' so applies to all tests automatically
    """
    test_name = os.environ.get('PYTEST_CURRENT_TEST').split(':')[-1].split(' ')[0]

    # save test name in context, without the parameter part
    pytest.context['test_name'] = re.sub(r'\[\w+]', '', test_name)

    log.info(f'========== STARTING TEST {test_name} ==========')
    test_start_time = time.time()
    yield
    test_run_time_float = time.time() - test_start_time
    if test_run_time_float < 10:
        test_run_time = round(test_run_time_float, 2)
    else:
        test_run_time = round(test_run_time_float)
    if request.node.rep_setup.failed:
        log.info(f'========== TEST SETUP {test_name} FAILED in {test_run_time} seconds ==========')

    elif request.node.rep_setup.passed:
        if request.node.rep_call.failed:
            # Automatically download the cluster diags on test failure if required
            if pytest.context.get("diags-on-fail"):
                collect_diags(test_name, pytest.context.get("clusters"))
            log.info(f'========== TEST {test_name} FAILED in {test_run_time} seconds ==========')
        else:
            # Automatically download the cluster diags on test pass if required
            if pytest.context.get("diags-on-pass"):
                collect_diags(test_name, pytest.context.get("clusters"))
            log.info(f'========== TEST {test_name} PASSED in {test_run_time} seconds ==========')

    else:
        log.info(f'========== TEST {test_name} SKIPPED in {test_run_time} seconds ==========')