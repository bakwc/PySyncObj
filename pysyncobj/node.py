from .dns_resolver import globalDnsResolver


class Node(object):
    """
    A representation of any node in the network.

    The ID must uniquely identify a node. Node objects with the same ID will be treated as equal, i.e. as representing the same node.
    """

    def __init__(self, id, **kwargs):
        """
        Initialise the Node; id must be immutable, hashable, and unique.

        :param id: unique, immutable, hashable ID of a node
        :type id: any
        :param **kwargs: any further information that should be kept about this node
        """

        self._id = id
        for key in kwargs:
            setattr(self, key, kwargs[key])

    def __setattr__(self, name, value):
        if name == 'id':
            raise AttributeError('Node id is not mutable')
        super(Node, self).__setattr__(name, value)

    def __eq__(self, other):
        return isinstance(other, Node) and self.id == other.id

    def __ne__(self, other):
        # In Python 3, __ne__ defaults to inverting the result of __eq__.
        # Python 2 isn't as sane. So for Python 2 compatibility, we also need to define the != operator explicitly.
        return not (self == other)

    def __hash__(self):
        return hash(self.id)

    def __str__(self):
        return self.id

    def __repr__(self):
        v = vars(self)
        return '{}({}{})'.format(type(self).__name__, repr(self.id), (', ' + ', '.join('{} = {}'.format(key, repr(v[key])) for key in v if key != '_id')) if len(v) > 1 else '')

    def _destroy(self):
        pass
    @property
    def id(self):
        return self._id


class TCPNode(Node):
    """
    A node intended for communication over TCP/IP. Its id is the network address (host:port).
    """

    def __init__(self, address, **kwargs):
        """
        Initialise the TCPNode

        :param address: network address of the node in the format 'host:port'
        :type address: str
        :param **kwargs: any further information that should be kept about this node
        """

        super(TCPNode, self).__init__(address, **kwargs)
        self.address = address
        self.host, port = address.rsplit(':', 1)
        self.port = int(port)
        self.ip = globalDnsResolver().resolve(self.host)

    def __repr__(self):
        v = vars(self)
        filtered = ['_id', 'address', 'host', 'port', 'ip']
        formatted = ['{} = {}'.format(key, repr(v[key])) for key in v if key not in filtered]
        return '{}({}{})'.format(type(self).__name__, repr(self.id), (', ' + ', '.join(formatted)) if len(formatted) else '')
