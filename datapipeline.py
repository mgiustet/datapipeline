import random
import threading
import uuid
import xmltodict
from multiprocessing import shared_memory



# TODO: change ID system to have (also) IDs of the form block.socket, which are more descriptive
class Pipeline():
    """This object represents a data pipeline. It is composed of Blocks, each having some source Sockets and some output Sockets. Each Block has methods that can be called from your thread to read data from the source Sockets and push the results to the output Sockets."""

    def __init__(self):
        self._blocks = []
        self._blocks_ids = []
        self._sockets = []
        self._sockets_ids = []

    # def __str__(self):
    #     return self.get_structure()

    # PUBLIC METHODS

    @classmethod
    def parse_structure(cls, xml):
        """Take in the Pipeline structure as an XML string and return the constructed Pipeline object. See reference for XML syntax employed."""
        # xmltodict gives a list if there are multiple instances of a tag and a single element if there is only one. This fixes it so that we always deal with a list
        # of course doesn't work if obj is a list
        def listify(obj):
            return obj if type(obj) == type([]) else [obj]

        struct = xmltodict.parse(xml)
        # check that we have the outer pipeline tag
        if "pipeline" in struct:
            pipeline = listify(struct["pipeline"])[0]
            p = Pipeline()
            # check if we have Blocks
            if "block" in pipeline:
                blocks = listify(pipeline["block"])
                # add first all blocks and output Sockets to the Pipeline...
                for b in blocks:
                    id = b["@id"]
                    output_sockets_ids = []
                    if "output" in b:
                        output_sockets = listify(b["output"])
                        for out_soc in output_sockets:
                            output_sockets_ids.append(out_soc["@id"])
                    p.add_block(id, [], output_sockets_ids)
                # ...then iterate again to connect source Sockets. This way we can safely ignore the order in which the block tags are placed in the XML
                for b in blocks:
                    id = b["@id"]
                    if "source" in b:
                        source_sockets = listify(b["source"])
                        for source_soc in source_sockets:
                            p.get_block_by_id(id).add_source_socket(source_soc["@id"])
            return p
        return None

    def connect(self, source_sockets_ids):
        """Add a block on-the-fly that only reads data from the specified source Sockets."""
        if len(source_sockets_ids) > 0:
            b = Block(None, self, source_sockets_ids, [])
            self._add_block(b)
            return b
        return ValueError("Must provide at least one source Socket.")

    def add_block(self, id, sources_ids=[], outputs_ids=[]):
        """Add a Block to this Pipeline and return its ID. See the reference for the constructor of the Block class."""
        self._add_block(Block(id, self, sources_ids, outputs_ids))

    def remove_block(self, id):
        """Remove the Block from the Pipeline, disconnect it from all its source Sockets and remove recursively all Blocks which were depending on this one for their source Sockets."""
        block = self.get_block_by_id(id)
        # disconnect from source Sockets
        for x in block.source_sockets:
            address = block.source_sockets_addresses[x.id]
            x.disconnect(address)
        # to delete all the sub-Pipeline depending on this block, we call the function recursively on Blocks depending on this one
        if kill_children:
            for soc in block.output_sockets:
                children_ids = soc.get_connected_blocks()
                for child_id in children_ids:
                    self.remove_block(child_id)
                # when all the subtree depending on soc has been deleted, delete also soc
                self._remove_socket(soc)
        # delete the block
        self._blocks.remove(block)
        self._blocks_ids.remove(id)

    def get_block_ids(self):
        """Return a copy of the list of IDs of Blocks in this Pipeline."""
        return list(self._blocks_ids)

    def get_socket_ids(self):
        """Return a copy of the list of IDs of Sockets in this Pipeline."""
        return list(self._sockets_ids)

    def get_block_by_id(self, id):
        """Return the Block with the specified ID or raise a ValueError if no such ID is present."""
        if not id in self._blocks_ids:
            raise ValueError("Block ID {} doesn't exist.".format(id))
        for b in self._blocks:
            if b.id == id:
                return b

    def get_socket_by_id(self, id):
        """Return the Socket with the specified ID or raise a ValueError if no such ID is present."""
        if not id in self._sockets_ids:
            raise ValueError("Socket ID {} doesn't exist.".format(id))
        for s in self._sockets:
            if s.id == id:
                return s

    def get_structure(self):
        """Returns the Pipeline structure as a string with XML syntax."""
        # TODO
        return "To be done yet."

    # PRIVATE METHODS

    def _remove_socket(self, socket):
        # disconnect all clients from this Socket
        socket._disconnect_all()
        # delete the Socket
        self._sockets.remove(socket)
        self._sockets_ids.remove(socket.id)

    def _add_block(self, block):
        if block.id in self._blocks_ids:
            raise ValueError("There is already a block with ID {} in this pipeline.".format(block.id))
        self._blocks.append(block)
        self._blocks_ids.append(block.id)

    def _add_socket(self, socket):
        if socket.id in self._sockets_ids:
            raise ValueError("There is already a socket with ID {} in this pipeline.".format(socket.id))
        self._sockets.append(socket)
        self._sockets_ids.append(socket.id)




class Socket():
    """This object represents an output socket belonging to a Block object. It handles connections to multiple Block objects. The Block who owns it posts data to it and connected Block objects retrieve it. If size > 0 is provided, the Socket will try to read/write data faster."""

    def __init__(self, id, owner_block):
        # reserve IDs starting with "___", so that they can be used for internal purposes --- e.g. to stop the processes running in the PipelineManager
        if id.startswith("___"):
            raise ValueError("Socket IDs cannot start with '___'")
        self.id = id
        self.owner_block = owner_block
        self._clients_count = 0
        # dictionary containing addresses as keys and Block IDs of connected entities as values
        self._addresses = {}
        # general lock used to assign and remove addresses
        self._lock = threading.Lock()
        # dict which will be used to actually exchange data
        # if size > 0 is provided, SharedMemory objects will be used for the r/w operations
        self._data_space = {}
        # dict of locks to be used to read and write on _data_space. There are a couple of locks for each connected client
        self._rw_locks = {}
        # dict of booleans for signaling there is new data in this cell. There are a couple of booleans for each connected client
        self._new_data = {}
        # dict of Events for signaling that new data has been written. There is one Event for each connected client
        self._data_available = {}
        # dict of indexes for the cell on which to write next. There is one index for each connected client and they can have values 0-1
        self._next_indexes = {}

    # PUBLIC METHODS

    def get_connected_blocks(self):
        """Return a list of IDs of connected Blocks."""
        res = []
        # acquire lock to access _addresses
        with self._lock:
            res = list(self._addresses.values())
        # release the lock
        return res

    # PRIVATE METHODS

    def _connect(self, id):
        # internal method. To connect to a Block use the dedicated methods from Block and Connection classes!
        # acquire the lock to access _clients_count and _addresses. The others are safe since we call only atomic methods
        with self._lock:
            self._clients_count += 1
            for i in range(self._clients_count):
                # when we find an address which was not already assigned, assign it
                if not i in self._addresses:
                    # save address and ID of this client
                    self._addresses[i] = id
                    # add space and variables for this address
                    self._data_space[i] = [None,None]
                    self._rw_locks[i] = [threading.Lock(), threading.Lock()]
                    self._new_data[i] = [False, False]
                    self._data_available[i] = threading.Event()
                    self._next_indexes[i] = 0
                    # return the address
                    return i
        return None

    def _disconnect(self, address):
        # acquire the lock to access _clients_count, _addresses and _data_space
        with self._lock:
            # check if the specified address exists in the internal dictionary
            if address in self._addresses:
                # acquire locks to cells of this address to prevent read/write
                with self._rw_locks[address][0]:
                    with self._rw_locks[address][1]:
                        # delete all variables used, least the locks
                        del self._data_space[address]
                        del self._sizes[address]
                        del self._new_data[address]
                        del self._data_available[address]
                        del self._next_indexes[address]
                        # delete the address
                        self._clients_count -= 1
                        del self._addresses[address]
                # delete the locks
                # TODO: check that this doesn't make too much of a mess and no one accesses the lock while we delete them
                # maybe we should add a flag that tells r/w to skip their present iteration and return None
                del self._rw_locks[address]

    def _disconnect_all(self):
        # to be called when deleting the Socket
        copy = []
        with self._lock:
            copy = list(self._addresses.values())
        for address in copy:
            self._disconnect(address)

    def _read(self, address):
        # internal method, use methods from Block class to actually get data from the Socket!

        # acquire the lock to access _addresses and check that we exist
        with self._lock:
            if not address in self._addresses:
                return None
        # release the lock
        # we want to read new datas, therefore we check if we can find a suitable cell --- otherwise, we wait for the one in which it is being written right now
        for i in range(2):
            if not self._rw_locks[address][i].locked():
                if self._new_data[address][i]:
                    with self._rw_locks[address][i]:
                        # signal we read data
                        self._data_available[address].clear()
                        self._new_data[address][i] = False
                        # read and return data. This is safe since no one is writing at this index in this moment
                        return self._data_space[address][i]
                    # release the lock
        # we found nothing, release the lock --- we will try to acquire it again for a second try
        # since we found nothing, reasonably we have to wait for new data. Subscribe to changes of the _data_available Event
        self._data_available[address].wait()
        # green light, let's go and try to catch the new data
        for i in range(2):
            if not self._rw_locks[address][i].locked():
                if self._new_data[address][i]:
                    with self._rw_locks[address][i]:
                        #signal we read data
                        self._data_available[address].clear()
                        self._new_data[address][i] = False
                        # read and return data. This is safe since no one is writing at this index in this moment
                        return self._data_space[address][i]
        # if still nothing, return None
        return None

    def _write(self, data):
        # internal method, use methods from Block class to actually send data to the Socket!

        # data must be an array of bytes

        # acquire lock to prevent connecting/disconnecting during write operation
        # drawback: we can't write either during connection/disconnection of a single block
        with self._lock:
            # write data for each client
            for client in self._addresses.keys():
                # next index from which we want to read
                next = self._next_indexes[client]
                # at least one of the two cells in the column assigned to the client is free, we just need to check if the intended one is and if not switch to the other
                if self._rw_locks[client][next].acquire(blocking=False):
                    # write datas
                    self._data_space[client][next] = data
                    # flag that we wrote new data here
                    self._new_data[client][next] = True
                    # release the lock
                    self._rw_locks[client][next].release()
                    # signal that new data is available
                    self._data_available[client].set()
                    # next time we'll try to write on the other cell
                    self._next_indexes[client] = 1 - self._next_indexes[client]
                else:
                    # lock the other cell
                    with self._rw_locks[client][1 - self._next_indexes[client]]:
                        # write datas
                        self._data_space[client][1 - next] = data
                        # flag that we wrote new data here
                        self._new_data[client][1 - next] = True
                    # release the lock
                    # signal that new data is available
                    self._data_available[client].set()
                    # next time we'll try to write on the other cell --- that is, the same on which we wanted to write now but we couldn't. So we don't change _next_indexes
        # release the lock




class Block():
    """This object is a block in a Pipeline. It connects to source Sockets, processes the datas obtained from them thanks to its associated thread and makes the results available as datas in output Socket objects. It can connect to multiple source Sockets, as well as multiple output Sockets can be attached to it."""

    def __init__(self, id, pipeline, source_sockets_ids, output_sockets_ids):
        if pipeline is None:
            raise ValueError("Pipeline can't be None.")
        self.pipeline = pipeline
        self.id = ""
        # if the provided ID is empty...
        if id is None or id == "":
            # ...generate a random ID for the block
            self.id = "___" + str(uuid.uuid4())
            # if it failed and we have two identical IDs in the pipeline... Well, that's sheer bad luck
        # reserve IDs starting with "___" to ephemeral connections
        elif id.startswith("___"):
            raise ValueError("Block IDs cannot start with '___'")
        else:
            self.id = id
        self.source_sockets = []
        self.source_sockets_ids = []
        # dictionary with Socket IDs as keys and corresponding addresses as values
        self.source_sockets_addresses = {}
        self.output_sockets = []
        self.output_sockets_ids = []
        # connect with all specified source Sockets
        for x in source_sockets_ids:
            self.add_source_socket(x)
        # create output Sockets
        for x in output_sockets_ids:
            self.add_output_socket(x)

    def add_source_socket(self, id):
        """Connect to the Socket with the specified ID."""
        soc = self.pipeline.get_socket_by_id(id)
        address = soc._connect(self.id)
        if address == None:
            raise RuntimeError("Couldn't connect to socket with ID {}".format(id))
        self.source_sockets.append(soc)
        self.source_sockets_ids.append(id)
        self.source_sockets_addresses[id] = address

    def add_output_socket(self, id):
        """Create an output Socket with the specified ID."""
        soc = Socket(id, self)
        # signal the pipeline parent object that we are making data available on the following Socket
        self.pipeline._add_socket(soc)
        self.output_sockets.append(soc)
        self.output_sockets_ids.append(id)

    def remove_source_socket(self, id):
        """Disconnect from the Socket with the specified ID."""
        soc = self.pipeline.get_socket_by_id(id)
        soc._disconnect(self.source_sockets_addresses[id], self.ephemeral)

    def read(self, id):
        """Read data from the source Socket with specified ID."""
        if not id in self.source_sockets_addresses:
            raise ConnectionError("Block with ID {} is not connected to source Socket with ID {}.".format(self.id, id))
        soc = self.pipeline.get_socket_by_id(id)
        return soc._read(self.source_sockets_addresses[id])

    def write(self, data, id):
        """Push data to the output Socket with specified ID."""
        if not id in self.output_sockets_ids:
            raise ValueError("Block with ID {} has no output Socket with ID {}.".format(self.id, id))
        soc = self.pipeline.get_socket_by_id(id)
        soc._write(data)
