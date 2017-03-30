########
# Copyright (c) 2014 GigaSpaces Technologies Ltd. All rights reserved
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
#    * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    * See the License for the specific language governing permissions and
#    * limitations under the License.


import json
import subprocess
import os
import sys
import time
import threading
from StringIO import StringIO
import tempfile

# For process handling
import psutil

import requests

from cloudify import ctx as operation_ctx
from cloudify.workflows import ctx as workflows_ctx
from cloudify.decorators import operation, workflow
from cloudify.exceptions import NonRecoverableError
from cloudify.manager import get_rest_client
from cloudify.proxy.client import CTX_SOCKET_URL
from cloudify.proxy.server import (UnixCtxProxy,
                                   TCPCtxProxy,
                                   HTTPCtxProxy,
                                   StubCtxProxy)
from script_runner import eval_env
from cloudify_rest_client.exceptions import CloudifyClientError
from cloudify_rest_client.executions import Execution

try:
    import zmq  # noqa
    HAS_ZMQ = True
except ImportError:
    HAS_ZMQ = False

IS_WINDOWS = os.name == 'nt'

DEFAULT_POLL_INTERVAL = 0.1
DEFAULT_CANCEL_POLL_INTERVAL = 100

# pylint: disable=W0212
# pylint: disable=C0111


@operation
def run(script_path, process=None, **kwargs):
    ctx = operation_ctx._get_current_object()
    if script_path is None:
        raise NonRecoverableError('Script path parameter not defined')
    process = create_process_config(process or {}, kwargs)
    script_path = download_resource(ctx.download_resource, script_path)
    os.chmod(script_path, 0755)
    script_func = get_run_script_func(script_path, process)
    return process_execution(script_func, script_path, ctx, process)


@workflow
def execute_workflow(script_path, **kwargs):
    ctx = workflows_ctx._get_current_object()
    script_path = download_resource(
        ctx.internal.handler.download_blueprint_resource, script_path)
    return process_execution(eval_script, script_path, ctx)


def create_process_config(process, operation_kwargs):
    env_vars = operation_kwargs.copy()
    if 'ctx' in env_vars:
        del env_vars['ctx']
    env_vars.update(process.get('env', {}))
    for key, val in env_vars.items():
        if isinstance(val, (dict, list, set, bool)):
            env_vars[key] = json.dumps(val)
        else:
            env_vars[key] = str(val)
    process['env'] = env_vars
    return process


def process_execution(script_func, script_path, ctx, process=None):
    def returns(value):
        ctx._return_value = value
    ctx.returns = returns
    ctx._return_value = None
    script_func(script_path, ctx, process)
    return ctx._return_value


def get_run_script_func(script_path, process):
    eval_python = process.get('eval_python')
    if eval_python is True or (script_path.endswith('.py') and
                               eval_python is not False):
        return eval_script
    else:
        return execute


def cancellation_requested(ctx, rest_client):
    '''
        Checks if a task cancellation was requested by the user.

    :param `cloudify.context.CloudifyContext` ctx:
        Current Cloudify context.
    :param `cloudify_rest_client.CloudifyClient` rest_client:
        A Cloudify REST API client.
    :returns: False on REST error, None if no cancellation was request,
        or the execution status if a cancellation was requested.
    '''
    # Check the REST API for the execution status
    ctx.logger.debug(
        'Checking if the user requested an execution cancellation')
    try:
        execution_status = rest_client.executions.get(
            ctx.execution_id, _include=['status']).status
    except CloudifyClientError as exc:
        ctx.logger.warning(
            'Failed to get status of execution %s: %s'
            % (ctx.execution_id, str(exc)))
        return False
    # If the user hasn't request any cancellations, continue
    ctx.logger.debug('Execution status: %s' % execution_status)
    # Sometimes Cloudify prematurely marks the execution as "cancelled"
    # and in this case we still need to kill processes and confirm.
    if execution_status not in [Execution.CANCELLED,
                                Execution.CANCELLING,
                                Execution.FORCE_CANCELLING]:
        return None
    ctx.logger.info('User requested a task cancellation (%s)'
                    % execution_status)
    return execution_status


def handle_cancellations(ctx, process, rest_client, force_kill=False):
    '''
        Handles task cancellations by the user.

    :param `cloudify.context.CloudifyContext` ctx:
        Current Cloudify context.
    :param `psutil.Process` process:
        Process of the script executor.
    :param `cloudify_rest_client.CloudifyClient` rest_client:
        A Cloudify REST API client.
    :param Boolean force_kill:
        If True, uses SIGKILL signal on processes. SIGTERM otherwise.
    :returns: True if all offending processes have been terminated,
        False if some are still pending.
    '''
    def process_terminated_callback(proc):
        '''
            Terminated process callback.

        .. info:
            This is a somewhat global callback and will report
            process terminations not invoked by this script.
        '''
        ctx.logger.info('Process terminated: %s' % proc)
    # Check if the current process is still running
    if not process.is_running():
        ctx.logger.info('Parent process (%s) has been killed' % process.pid)
        return True
    # Check if any children processes are (still) running
    _, palive = psutil.wait_procs(
        process.children(recursive=True), timeout=1,
        callback=process_terminated_callback)
    # If no processes are left, terminate the current process
    if not palive:
        ctx.logger.info('All child processes have been terminated')
        ctx.logger.info('Attempting to terminate (%s) parent process %s' % (
            'SIGKILL' if force_kill else 'SIGTERM', process.pid))
        if force_kill:
            process.kill()
        else:
            process.terminate()
        return False
    ctx.logger.info('%s child process(es) still alive' % len(palive))
    # If some processes are (still) running, send a followup signal
    for proc in palive:
        ctx.logger.info('Attempting to terminate (%s) child process %s' % (
            'SIGKILL' if force_kill else 'SIGTERM', proc.pid))
        if force_kill:
            proc.kill()
        else:
            proc.terminate()
    return False


def execute(script_path, ctx, process):
    on_posix = 'posix' in sys.builtin_module_names

    proxy = start_ctx_proxy(ctx, process)

    env = os.environ.copy()
    process_env = process.get('env', {})
    env.update(process_env)
    env[CTX_SOCKET_URL] = proxy.socket_url

    cwd = process.get('cwd')
    poll_interval = process.get('poll_interval', DEFAULT_POLL_INTERVAL)
    cancel_poll_interval = process.get('cancel_poll_interval',
                                       DEFAULT_CANCEL_POLL_INTERVAL)

    command_prefix = process.get('command_prefix')
    if command_prefix:
        command = '{0} {1}'.format(command_prefix, script_path)
    else:
        command = script_path

    args = process.get('args')
    if args:
        command = ' '.join([command] + args)

    ctx.logger.info('Executing: {0}'.format(command))

    process = subprocess.Popen(command,
                               shell=True,
                               stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE,
                               env=env,
                               cwd=cwd,
                               bufsize=1,
                               close_fds=on_posix)
    pprocess = psutil.Process(process.pid).parent()

    return_code = None

    stdout_consumer = OutputConsumer(process.stdout)
    stderr_consumer = OutputConsumer(process.stderr)

    # Get an interface to the REST API
    rest_client = get_rest_client()
    poll_counter = 0
    cold_blooded = False

    while True:
        poll_counter += 1
        process_ctx_request(proxy)
        # If the parent process has finished, so have we
        if not pprocess.is_running():
            # Return the exit code of the script invoker process
            return_code = process.returncode
            ctx.logger.info('Process has exited (code=%s)'
                            % return_code)
            break
        # Handle task cancellation requests
        if (poll_counter % cancel_poll_interval) == 0:
            # Check if the user requested a cancellation
            exec_status = cancellation_requested(ctx, rest_client)
            if exec_status:
                # Use SIGKILL if this is a force_cancellation request
                if exec_status == Execution.FORCE_CANCELLING:
                    cold_blooded = True
                if handle_cancellations(ctx, pprocess, rest_client,
                                        force_kill=cold_blooded):
                    # If we're here, then all processes (including the
                    # parent process) are killed
                    break
                # Regardless of cancel type, send SIGKILL on followup attempts
                cold_blooded = True
        # ZZzzz
        time.sleep(poll_interval)

    proxy.close()
    stdout_consumer.join()
    stderr_consumer.join()

    ctx.logger.info('Execution done (return_code={0}): {1}'
                    .format(return_code, command))

    if return_code != 0:
        raise ProcessException(command,
                               return_code,
                               stdout_consumer.buffer.getvalue(),
                               stderr_consumer.buffer.getvalue())


def start_ctx_proxy(ctx, process):
    ctx_proxy_type = process.get('ctx_proxy_type')
    if not ctx_proxy_type or ctx_proxy_type == 'auto':
        if HAS_ZMQ:
            return TCPCtxProxy(ctx) if IS_WINDOWS else UnixCtxProxy(ctx)
        else:
            return HTTPCtxProxy(ctx)
    elif ctx_proxy_type == 'unix':
        return UnixCtxProxy(ctx)
    elif ctx_proxy_type == 'tcp':
        return TCPCtxProxy(ctx)
    elif ctx_proxy_type == 'http':
        return HTTPCtxProxy(ctx)
    elif ctx_proxy_type == 'none':
        return StubCtxProxy()
    else:
        raise NonRecoverableError('Unsupported proxy type: {0}'
                                  .format(ctx_proxy_type))


def process_ctx_request(proxy):
    if isinstance(proxy, StubCtxProxy):
        return
    if isinstance(proxy, HTTPCtxProxy):
        return
    proxy.poll_and_process(timeout=0)


def eval_script(script_path, ctx, process=None):
    eval_globals = eval_env.setup_env_and_globals(script_path)
    execfile(script_path, eval_globals)


def download_resource(download_resource_func, script_path):
    split = script_path.split('://')
    schema = split[0]
    if schema in ['http', 'https']:
        response = requests.get(script_path)
        if response.status_code == 404:
            raise NonRecoverableError('Failed downloading script: {0} ('
                                      'status code: {1})'
                                      .format(script_path,
                                              response.status_code))
        content = response.text
        suffix = script_path.split('/')[-1]
        script_path = tempfile.mktemp(suffix='-{0}'.format(suffix))
        with open(script_path, 'w') as fscript:
            fscript.write(content)
        return script_path
    else:
        return download_resource_func(script_path)


class OutputConsumer(object):

    def __init__(self, out):
        self.out = out
        self.buffer = StringIO()
        self.consumer = threading.Thread(target=self.consume_output)
        self.consumer.daemon = True
        self.consumer.start()

    def consume_output(self):
        for line in iter(self.out.readline, b''):
            self.buffer.write(line)
        self.out.close()

    def join(self):
        self.consumer.join()


class ProcessException(Exception):

    def __init__(self, command, exit_code, stdout, stderr):
        super(ProcessException, self).__init__(stderr)
        self.command = command
        self.exit_code = exit_code
        self.stdout = stdout
        self.stderr = stderr
