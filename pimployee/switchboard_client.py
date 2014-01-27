import json
import passfd
import threading
import Queue

class Sender(threading.Thread):
    
    def __init__(self, socket, sq, rq):
        super(Sender, self).__init__()
        self._sq = sq
        self._rq = rq
        self._socket = socket

    def run(self):
        while True:
            to_send = self._sq.get()
            if to_send is None:
                break
            try:
                self._socket.sendall(to_send)
            except IOError, e:
                m = Message()
                m.meta = {"type": 'die'}
                m.ready.set()
                self._rq.put_nowait(m)
                self._socket.close()
                break
        """
            to_send = None
            try:
                to_send = self._sq.get(timeout=10)
            except Queue.Empty:
                pass         
            
            try:
                if to_send is None:
                    self._socket.sendall(Message.serialize_message({"type": "hb"}))
                else:
                    self._socket.sendall(to_send)
            except BaseException, e:
                self._rq.put_nowait(e)
                break
        """

class Receiver(threading.Thread):
    
    def __init__(self, socket, rq, sq):
        super(Receiver, self).__init__()
        self._rq = rq
        self._sq = sq
        self._socket = socket

    def run(self):
        message = Message()
        while True:
            try:
                message.read(self._socket)
            except IOError, e:
                m = Message()
                m.meta = {"type": 'die'}
                m.ready.set()
                self._rq.put_nowait(m)
                break
            if message.is_ready():
                if message.meta["type"] == "hb":
                    self._sq.put_nowait(Message.serialize_message({"type": "hb"}))
                else:
                    self._rq.put(message)
                message = Message()

class Message(object):
    """Represents the unit by which we send messages between our socket
    clients."""
    
    STATE_META = 'META'
    STATE_PAYLOAD = 'PAYLOAD'
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
            chunk = socket.recv(self.META_LENGTH - len(self.meta))
            if chunk == "":
                raise IOError("Connection lost")
            self.meta += chunk
            if len(self.meta) == self.META_LENGTH:
                #print 'meta before deserialization', self.meta
                self.meta = json.loads(self.meta)
                if self.meta.get('payload_length'):
                    self.state = self.STATE_PAYLOAD
                    #print 'changed state to payload'
                else:
                    self.state = self.STATE_READY
                    #print 'changed state to ready'
                    self.ready.set()
        
        if self.state == self.STATE_PAYLOAD:
            #print 'receiving payload'
            self.payload += socket.recv(self.meta.get('payload_length') - len(self.payload))
            #print 'length of payload so far', len(self.payload)
            if len(self.payload) == self.meta.get('payload_length'):
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
        
        data = json.dumps(meta)
        
        if len(data) > Message.META_LENGTH:
            raise Exception('Message is longer than maximum allowed by protocol (%s bytes)' % Message.META_LENGTH)
        
        data += ' ' * (Message.META_LENGTH - len(data))
        if payload:
            data += payload
            del meta['payload_length']
        
        #print 'sending the data of length', len(data)
        return data


class UnixDomainSocketClient(object):
    
    def __init__(self, socket):
        self._socket = socket

        self._sq = Queue.Queue() 
        self._rq = Queue.Queue() 
        self._sender = Sender(socket, self._sq, self._rq)
        self._receiver = Receiver(socket, self._rq, self._sq)
        self._sender.daemon = True
        self._receiver.daemon = True
        self._sender.start()
        self._receiver.start()
    
    def kill(self):
        self._socket.close()

    def read(self):
        message = self._rq.get()
        if isinstance(message, BaseException):
            raise message
        return message

    def send(self, meta, payload=None, fileno=None):
        """
        *meta* dict
        *payload* bytes
        *fileno* file descriptor
        """
        self._sq.put_nowait(Message.serialize_message(meta, payload, has_fd=fileno != None))
