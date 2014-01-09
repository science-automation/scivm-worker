import os
import sys
import time
import random
import signal
import marshal
import resource
import threading
import traceback

import cProfile
import cPickle as pickle

#import cloud
import log

class EndProcessException(Exception):
    pass

def restrict_resources(core_type, cores):
    """Restrict the number of processes that a user can have running.
    Prevents fork bombing."""
    
    nproc = {'c1': 60,
             'c2': 200,
             'f2': 300,
             'm1': 300,
             's1': 50}.get(core_type, 100) * cores
             
    log.logger.info('Restricting (%s, %s) to %s procs', core_type, cores, nproc)
    
    # set max number of processes to prevent fork bombs
    resource.setrlimit(resource.RLIMIT_NPROC, (nproc, nproc))

def process_job(m, c):
    """Processes a job from an assign message"""
    
    import cloud
    
    restrict_resources(m.meta.get('core_type', 'c1'), m.meta.get('cores', 1))
    
    # set traceback max to 1M characters
    # we've had tracebacks that exceed Mongo's limit of 16 MB,
    # which causes the job outputs to go unsaved. 
    traceback_max_length = 1000000

    end_process = False
    process_id = os.getpid()
    
    log.logger.info('Assigned job %s' % m.meta['jid'])
    
    c.send({'type': 'processing'})
    log.logger.info('Sent processing message to boss')
    
    log.logger.info('Getting func, args, kwargs from payload -- not deserializing yet')
    
    log.logger.info('payload length %s payload parts %s' % (len(m.payload), m.meta['payload_parts']))
    
    func = m.payload[:m.meta['payload_parts'][0]]
    args = m.payload[m.meta['payload_parts'][0]:sum(m.meta['payload_parts'])]
    kwargs = m.payload[sum(m.meta['payload_parts']):]
    
    log.logger.info('Configuring cloud client for job owner')
    
    if m.meta['ujid']:
        # this only happens if the job has a ujid assigned already
        # otherwise, if the ujid isn't there, the parent relationship
        # will not be accurately recorded
        cloud.config.parent_jid = m.meta['ujid']
    
    cloud.config.api_key = m.meta['api_key']
    cloud.config.api_secretkey = m.meta['api_secretkey']
    
    cloud.config.url = m.meta['server_url']
    cloud.config.commit()
    
    # at this point, a new process where picloud is being used,
    # (such as a call to the picloud cli), will not have access
    # to these keys. so do a flush_config().
    cloud.cloudconfig.flush_config()
    
    # configures cloud to user's api/secret key. Sets it immutable
    cloud.setkey(int(m.meta['api_key']),
                 m.meta['api_secretkey'],
                 server_url=m.meta['server_url'],
                 immutable=True)
    
    log.logger.info('Deserializing func, args, kwargs')
    
    # start timing from deserialization
    start_time = time.time()
    
    try:
        func = deserialize(func)
        args = deserialize(args) if args else ()
        kwargs = deserialize(kwargs) if kwargs else {}
    except:
        log.pilogger.exception('Could not depickle job')
        tb = traceback.format_exc()[:traceback_max_length]
        
        c.send({'type': 'finished',
                'runtime': time.time() - start_time,
                'traceback': True},
               tb)
        
        # raise an exception to signal that the process should be killed
        raise EndProcessException('Could not depickle. Signal to kill process.')
    
    # catch SIGTERM (kill commands) and simply raise an exception
    # so that we get access to the traceback
    signal.signal(signal.SIGTERM, sigterm_handler)
    
    serialized_result = None
    exception_traceback = None
    
    try:
        
        if m.meta['job_type'] == 'filemap_mapper':
            func = func(mapper_combiner_generator)
        elif m.meta['job_type'] == 'filemap_reducer':
            func = func(reducer_generator)
        
        log.logger.info('Executing job')
        
        if m.meta['profile']:
            f_globals = {'__builtins__': globals()['__builtins__'],
                         '__name__': '__main__',
                         '__doc__': None,
                         '__package__': None}
            f_locals = {'func': func, 'args': args, 'kwargs': kwargs}
            statement = 'result = func(*args, **kwargs)'
            profiler = cProfile.Profile()
            profiler.runctx(statement, f_globals, f_locals)
            result = f_locals['result']
        else:
            result = func(*args, **kwargs)
        #time.sleep(100)
        # serialize the result
        adapter = getattr(cloud,'__cloud').adapter
        serialized_obj = adapter.getserializer(m.meta['fast_serialization'])(result)
        
        # TODO: Generate report somewhere?
        serialized_obj.run_serialization() 
        
        adapter.check_size(serialized_obj, None, True, 128000000)
        
        serialized_result = serialized_obj.serializedObject
        
        log.logger.info('Successfully executed job.')

    except BaseException as e:
        
        log.logger.exception('Executing job hit exception')
        
        # extract traceback to see exception stack and details
        exception_traceback = traceback.format_exc()[:traceback_max_length]
        
        if isinstance(e, (SystemExit, MemoryError)):
            end_process = True
        
    finally:
        
        # immediately exit if this is a fork of the original process
        if process_id != os.getpid():
            sys.exit(0)
        
        # set signal handler back to default
        signal.signal(signal.SIGTERM, signal.SIG_DFL)
        
        # closes threads
        cloud_open, cloud_mp_open = cloud.close(), cloud.mp.close()
        log.logger.info('Closed cloud and cloud.mp. Open check (cloud=%s, cloud.mp=%s)' % (cloud_open, cloud_mp_open))
        
        if not end_process:
            running_threads = threading.enumerate()[1:]
            if running_threads:
                time.sleep(0.02) #give some time for threads that may be shutting down
                running_threads = threading.enumerate()[1:]
            
                if running_threads:
                    end_process = True
                    log.logger.info('Users threads %s did not terminate' % repr(running_threads))
                    log.pilogger.info('Cannot use persistent process due to following thread(s) still running:\n%s\n' % repr(running_threads))

        runtime = time.time() - start_time
    
    # flush stdout and stderr
    sys.stdout.flush()
    sys.stderr.flush()
    
    if m.meta['profile']:
        profiler.create_stats()
        c.send({'type': 'profile'},
                marshal.dumps(profiler.stats))
    
    if serialized_result:
        c.send({'type': 'finished',
                'runtime': runtime},
               serialized_result)
    elif exception_traceback:
        c.send({'type': 'finished',
                'runtime': runtime,
                'traceback': True},
               exception_traceback)
    else:
        log.logger.error('Critical error: No result and no traceback.')
    
    if end_process:
        raise EndProcessException('Process should be ended')
    
    
def deserialize(s):
    get_func_end_time = None
    
    while True:
        try:
            return pickle.loads(s)
            break
        except ImportError:
            if not get_func_end_time:
                get_func_end_time = time.time() + 2.0
            elif time.time() > get_func_end_time:
                raise
            log.logger.info('Could not deserialize, retrying...')
            time.sleep(random.random())
    
def sigterm_handler(signum, frame):
    log.logger.info('Signal handler called with signal %s' % signum)
    raise SystemExit('Killed')


import itertools
import __builtin__

def mapper_combiner_generator( mapper, file_name, file_size, record_reader, combiner):    
    import cloud
    import types

    def inner(start_byte, end_byte):
        
        log.logger.info('mapper function attr')

        if not isinstance(start_byte, (int, long)):
            raise Exception('start_byte must be an integer')
        
        if not isinstance(end_byte, (int, long)):
            raise Exception('end_byte must be an integer')
        
        fobj = cloud.files.getf(file_name, start_byte, file_size)

        rr_it = record_reader(fobj, end_byte)
        if type(rr_it) != types.GeneratorType:
            raise Exception('record_reader is not a generator')

        if hasattr(itertools.chain, 'from_iterable'):  #Python2.6+
            map_results_it = itertools.chain.from_iterable( itertools.imap(mapper, rr_it) )
        else:
            map_results_it = ( map_result for map_result in itertools.imap(mapper, rr_it) )
        
        comb = combiner(map_results_it)
        if type(comb) != types.GeneratorType:
            raise Exception('combiner is not a generator')
        
        try:
            result = __builtin__.list( comb )
        except Exception as e:
            if e.message.find("is not iterable") > 0 :
                msg = e.message + "\n\nThis could be an issue with the mapper function not returning an iterable.\nPlease make sure that the mapper() is either a generator object or returns an iteratable."
                raise Exception(msg)
            else:
                raise e
            
        fobj.close()
        return result

    return inner

def reducer_generator(reducer):
    import cloud
    
    def ch_reducer(jids):

        c = cloud._getcloud()
        res = getattr( c, '_Cloud__iresult' )
        all_map_results_it = res(jids, by_jid=True)
        
        if hasattr(itertools.chain, 'from_iterable'):  #Python2.6+
            flattened_map_results = itertools.chain.from_iterable(all_map_results_it)
        else:
            flattened_map_results = (map_result for map_result in all_map_results_it)
        
        red = reducer( flattened_map_results )
        if not hasattr(red, '__iter__'):
            raise Exception('reducer is not an iterable')
        final_result = __builtin__.list( red ) #must return list for it to be serializable
        
        return final_result  
    
    return ch_reducer


