import os
import sys
import socket

try:
    SERVER_ADDRESS = os.environ["SERVER_ADDRESS"]
    SERVER_PORT = int(os.environ["SERVER_PORT"])
except KeyError:
    print "error: please set SERVER_ADDRESS and SERVER_PORT environment variables"
    sys.exit(1)
except ValueError:
    print "error: SERVER_PORT environment variable must be an integer"
    sys.exit(1)

def socket_set_default_timeout(timeout=60.0):
    """Sets the default *timeout* for all sockets. Many users accessing APIs,
    and scraping websites find that their sockets sometime hang. Without a
    default timeout, their computation will seemingly hang forever, which
    often results in support tickets, and users blaming us."""
    
    socket.setdefaulttimeout(timeout)

def substitute_stdout_pipe(m):
    
    new_stdout_write_fd = m.meta['fileno']
    
    orig_stdout_fd = os.dup(sys.stdout.fileno())
    os.dup2(new_stdout_write_fd, sys.stdout.fileno())
    
    # create a new file to set buffer to 0
    stdout_file = os.fdopen(new_stdout_write_fd, 'w' if sys.version_info[0] == 3 else 'wb', 0)
    
    sys.stdout = stdout_file
    
    return orig_stdout_fd

def substitute_stderr_pipe(m):
    
    new_stderr_write_fd = m.meta['fileno']
    
    orig_stderr_fd = os.dup(sys.stderr.fileno())
    os.dup2(new_stderr_write_fd, sys.stderr.fileno())
    
    # create a new file to set buffer to 0
    stderr_file = os.fdopen(new_stderr_write_fd, 'w' if sys.version_info[0] == 3 else 'wb', 0)
    
    sys.stderr = stderr_file
    
    return orig_stderr_fd

import fcntl
import atfork

def connect_to_boss():
    # -- original code:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # set timeout to None since we now set a default finite timeout
    s.settimeout(None)
    s.connect((SERVER_ADDRESS, SERVER_PORT))
    #try:
    #    s.connect('./picloud.sock')
    #except Exception as e:
    #    print e
    
    # susceptible to race conditions
    # closes file descriptor on exec and fork
    flags = fcntl.fcntl(s, fcntl.F_GETFD)
    fcntl.fcntl(s, fcntl.F_SETFD, flags | fcntl.FD_CLOEXEC)
    atfork.atfork(child=lambda: s.close())
    
    return s

def setup_logging(m):
    
    import logging
    
    # use the root logger directly since logging.basicConfig is ignored if a root handler is already set
    logger = logging.getLogger('')
    handler = logging.StreamHandler(os.fdopen(m.meta['fileno'], 'w' if sys.version_info[0] == 3 else 'wb', 0))
    handler.setFormatter(logging.Formatter("[%(asctime)s] - [%(levelname)s] - %(message)s", datefmt=None))
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)

def setup_cloud(hostname, ap_version):
    
    import cloud
    
    cloud.config._showhidden()
    cloud.config.hostname = hostname
    cloud.config.running_on_cloud = True
        
    #turn off logging (pilog will handle it)
    cloud.config.serialize_logging = False
    cloud.config.save_log = False
    
    #larger transmit size for uploads
    cloud.config.max_transmit_data = 16000000
    
    cloud.config.ap_version = ap_version
    if not cloud.config.ap_version: #be sure not cast to None
        cloud.config.ap_version = '' 
    
    cloud.config.commit()

def setup_faulthandler(m):
    # for reporting the Python line at which a segfault occurs
    try:
        import faulthandler
    except ImportError:
        import log
        log.logger.info('Could not import faulthandler')
    else:
        faulthandler.enable(os.fdopen(m.meta['fileno'], 'w'))

def disable_faulthandler():
    try:
        import faulthandler
        faulthandler.disable()
    except:
        pass

def setup_sys_path(ap_version, ap_path, archive_path):
    
    # Validate that AFS still works
    try:
        if ap_version:
            # ap_path is created lazily, so if a user has never added a module,
            # then the directory will not exist.
            os.listdir(ap_path)
    except:
        import log
        log.logger.info('Could not list ap home directory %s. Aborting' % ap_path)
        sys.exit(1)
    
    if ap_path:
        sys.path.append(ap_path)
    
    if archive_path and os.path.exists(archive_path):
        # we must check whether the archive path exists
        # if it doesn't, the lazy importer will crash
        sys.path.append(archive_path)
        sys.path_hooks.append(lazy_importer(archive_path))
    
    # launch command was python /usr/.../job_task.py
    # Consequently /home/picloud will not be on path
    sys.path.append(os.getcwd())
    
    # HACK: Wipe __file__ attribute so that __main__ appears to be coming in on stdin (dependencies)
    delattr(sys.modules['__main__'],'__file__')


import imp
import site

def lazy_importer(target_path):
    
    class LazySiteImporter(object):
        """
        LazySiteImporter - only access the files at 'target_path'
        when all other paths in sys.path don't contain the requested
        module.
        http://www.python.org/dev/peps/pep-0302/
        
        TODO: Rename this!
        """
        
        def __init__(self, lazy_dir):
            """lazy_dir is a directory of packages to import lazily"""
            
            if lazy_dir != target_path:
                raise ImportError
            else:
                self.lazy_dir = lazy_dir
            
        def find_module(self, fullname, path=None):
            try:
                sub_paths = os.listdir(self.lazy_dir)
            except OSError:
                import log
                log.logger.warning('Could not read lazy_dir %s' % self.lazy_dir)
                raise ImportError #obey PEP 302
            
            imp.acquire_lock()
            if self.lazy_dir in sys.path:                 
                for path in sub_paths:
                    site.addsitedir(os.path.join(self.lazy_dir, path))    
                sys.path.remove(self.lazy_dir)
            imp.release_lock()
            
            return self
        
        def load_module(self, fullname):
            mod = __import__(fullname)
            return mod
        
    return LazySiteImporter


import ctypes    
def crash():
    '''\
    crash the Python interpreter...
    '''
    i = ctypes.c_char('a')
    j = ctypes.pointer(i)
    c = 0
    while True:
            j[c] = 'a'
            c += 1
