import json
import passfd
import threading

class Message(object):
    """Represents the unit by which we send messages between our socket
    clients."""
    
    STATE_META = 'META'
    STATE_PAYLOAD = 'PAYLOAD'
    STATE_FD = 'FD'
    STATE_READY = 'READY'
    META_LENGTH = 1024
    
    def __init__(self):
        self.bytes_so_far = 0
        self.ready = threading.Event()
        
        self.state = self.STATE_META
        self.meta = ''
        self.payload = ''
        
    
    def read(self, socket):
        """Socket should have something to be read."""
        
        if self.state == self.STATE_META:
            #print 'receiving meta'
            self.meta += socket.recv(self.META_LENGTH - len(self.meta))
            if len(self.meta) == self.META_LENGTH:
                #print 'meta before deserialization', self.meta
                self.meta = json.loads(self.meta)
                if self.meta.get('payload_length'):
                    self.state = self.STATE_PAYLOAD
                    #print 'changed state to payload'
                elif self.meta.get('has_fd'):
                    self.state = self.STATE_FD
                else:
                    self.state = self.STATE_READY
                    #print 'changed state to ready'
                    self.ready.set()
        
        if self.state == self.STATE_PAYLOAD:
            #print 'receiving payload'
            self.payload += socket.recv(self.meta.get('payload_length') - len(self.payload))
            #print 'length of payload so far', len(self.payload)
            if len(self.payload) == self.meta.get('payload_length'):
                if self.meta.get('has_fd'):
                    self.state = self.STATE_FD
                else:
                    self.state = self.STATE_READY
                    self.ready.set()
    
        if self.state == self.STATE_FD:
            # assume this always succeeds
            fileno, _ = passfd.recvfd(socket, 1)
            self.meta['fileno'] = fileno
            #print 'GOT A FILENO!!!', fileno
            self.state = self.STATE_READY
            self.ready.set()
    
    def is_ready(self):
        return self.ready.is_set()
    
    def block_until_ready(self):
        self.ready.wait()
        
    @staticmethod
    def serialize_message(meta, payload=None, has_fd=False):
        """
        *meta* should be dict that when serialized is less than 1024 bytes.
        *payload* can be a byte string of any length.
        """
        
        if payload:
            meta['payload_length'] = len(payload)
        if has_fd:
            meta['has_fd'] = True
        data = json.dumps(meta)
        
        if len(data) > Message.META_LENGTH:
            raise Exception('Message is longer than maximum allowed by protocol (%s bytes)' % Message.META_LENGTH)
        
        data += ' ' * (Message.META_LENGTH - len(data))
        if payload:
            data += payload
            del meta['payload_length']
        if has_fd:
            del meta['has_fd']
        
        #print 'sending the data of length', len(data)
        return data


class UnixDomainSocketClient(object):
    
    def __init__(self, socket):
        
        self.socket = socket
        self.current_message = Message()
        
    #def switchboard_registration_information(self):
    #    return {'fileno': self.socket.fileno(), 'callback': None, 'eventmask': EPOLLIN | EPOLLPRI | EPOLLET}
    #    
    def read(self, blocking=True):
        
        while True:
            self.current_message.read(self.socket)
            if self.current_message.is_ready():
                message = self.current_message
                self.current_message = Message()
                return message
            elif not blocking:
                return
                
    def send(self, meta, payload=None, fileno=None):
        """
        *meta* dict
        *payload* bytes
        *fileno* file descriptor
        """
        #print 'sending', Message.serialize_message(meta, payload, has_fd=fileno != None)
        self.socket.sendall(Message.serialize_message(meta, payload, has_fd=fileno != None))
        if fileno:
            passfd.sendfd(self.socket, fileno, '*')
