#!/usr/bin/python

"""
Job Task runs a user's Python code.


excepthook is replaced so that output goes to log, but this only happens
once a log message is received.

"""

# run this first so that os.fork() is monkey patched
import atfork
atfork.monkeypatch_os_fork_functions()

import os
import sys

from pimployee import log, setup_util, job_util
from pimployee.switchboard_client import UnixDomainSocketClient

try:
    qdesc = os.environ["QDESC"]
except KeyError:
    raise Exception('environment variable QDESC is not set')

setup_util.socket_set_default_timeout(60.0)

# creates a connection to the boss using a unix domain socket
# that the boss has made and placed on the filesystem
s = setup_util.connect_to_boss()

# create client object for easy sending and receiving of messages
c = UnixDomainSocketClient(s)

c.send({
    "type": "info",
    "qdesc": qdesc
})

m = c.read()
if m.meta['type'] != 'setup':
    raise Exception('First message was not a setup message')

# setup log
log.setup_log(m.meta['fileno'])
log.setup_excepthook()

log.logger.info('Log has been setup')

# adds automatically extracted package paths to end of sys.path
setup_util.setup_sys_path(m.meta.get('ap_version'), m.meta.get('ap_path'), m.meta.get('archive_path'))

# sets up cloud library
setup_util.setup_cloud(m.meta['hostname'], m.meta['ap_version'])

# save stderr descriptor, and restore on exit for atexit
orig_stderr_fd = os.dup(sys.stderr.fileno())    

# keys map to "type" of Message.meta['type']
message_handlers = {'stdout': setup_util.substitute_stdout_pipe,
                    'stderr': setup_util.substitute_stderr_pipe,
                    'logging': setup_util.setup_logging,
                    'faulthandler': setup_util.setup_faulthandler,
                    'pilog': lambda m: log.setup_pilog(m.meta['fileno']),
                    'assign': lambda m: None,
                    'die': lambda m: None}

while True:
    
    log.logger.info('Waiting for message from boss')
    
    m = c.read()
    handler = message_handlers.get(m.meta['type'])
    if handler:
        log.logger.info('Handling message %s m.meta' % m.meta)
        handler(m)
        log.logger.info('Done handling message %s m.meta' % m.meta)
    else:
        log.logger.info('Unrecognized message type %s' % m.meta['type'])
    
    if m.meta['type'] == 'assign':
        try:
            job_util.process_job(m, c)
            # use this to test for random crash
            #setup_util.crash()
        except job_util.EndProcessException:
            log.logger.info('EndProcessException raised. Process will end')
            break
    
    if m.meta['type'] == 'die':
        log.logger.info('Received message to die.')
        
        # restore stderr for atexit to work
        sys.stderr = sys.__stderr__             
        os.dup2(orig_stderr_fd, sys.stderr.fileno())
        
        break

# an interpreter shutdown sometimes crashes. we don't want faulthandler
# to deal with it
setup_util.disable_faulthandler()

log.logger.info('last statement in job_task before sys.exit(0)')

# force exit, otherwise non-daemon threads could keep the process alive
# indefinitely
sys.exit(0)

