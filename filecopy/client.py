# Copyright 2011 Obsidian Research Corp. GLPv2, see COPYING.
import pickle
import socket
import contextlib
import os, sys
import time
import struct
import threading
from mmap import mmap
from collections import namedtuple
import rdma.ibverbs as ibv
from rdma.tools import clock_monotonic
import rdma.path
import rdma.vtools
from endpoint import Endpoint, read_memory_until_nonzero


ip_port = 4444


infotype = namedtuple('infotype', 'path sending_addr sending_rkey sending_size receiving_addr receiving_rkey receiving_size iters')

def client_mode(hostname, dev):
    sending_mem_size = 4
    receiving_mem_size = 4

    # Create endpoint with sending and receiving memory
    with Endpoint(sending_mem_size, receiving_mem_size, dev, start_thread=False) as end:
        # Prepare the data to send
        val = 1
        val_bytes = struct.pack('<i', val)
        end.write_to_memory(0, val_bytes)

        # Get address of server to connect to
        ret = socket.getaddrinfo(hostname, str(ip_port), 0, socket.SOCK_STREAM)
        ret = ret[0]

        with contextlib.closing(socket.socket(ret[0],ret[1])) as sock:
            # Connect to server using TCP
            sock.connect(ret[4])

            # Query for the source device GID
            sgid = end.ctx.end_port.default_gid
            print "SGID: %x" % sgid

            # Create path based on the device and GID
            path = rdma.path.IBPath(dev, SGID=sgid)

            # TODO: If using atomics to signal new messages, set this to non-zero?
            # http://www.ziepe.ca/python-rdma/manual/path.html?highlight=fill_path#rdma.path.fill_path
            rdma.path.fill_path(end.qp, path, max_rd_atomic=0)
            path.reverse(for_reply=False)

            # Send information about the path and memory region used to send data (over TCP)
            sock.send(pickle.dumps(infotype(path=path,
                                            sending_addr=end.sending_mr.addr,
                                            sending_rkey=end.sending_mr.rkey,
                                            sending_size=end.sending_mem_size,
                                            receiving_addr=end.receiving_mr.addr,
                                            receiving_rkey=end.receiving_mr.rkey,
                                            receiving_size=end.receiving_mem_size,
                                            iters=1)))

            # Get information from the server about its memory region to be used where we send data (over TCP)
            buf = sock.recv(1024)
            peerinfo = pickle.loads(buf)

            # Setup the path based on the information from the server
            end.path = peerinfo.path
            end.path.reverse(for_reply=False)
            end.path.end_port = end.ctx.end_port
            print "path to peer %r\nMR peer sending_addr=%x peer sending_key=%x\nMR peer receiving_addr=%x peer_receiving_key=%x" % (end.path, peerinfo.sending_addr, peerinfo.sending_rkey, peerinfo.receiving_addr, peerinfo.receiving_rkey)

            # Transition to RTS (Ready to Send) state
            end.connect(peerinfo)

            # Synchronize the transition to RTS (to ensure the server also gets to this stage)
            sock.send("Ready")
            sock.recv(1024)

            # Send data over RDMA
            startTime = time.time()
            end.send()
            endTime = time.time()
            print "-- rmda send: elapsed time = %f " % (endTime - startTime)

            # Wait until the server has also written a message back (reply message to mock the return value of a function call)
            startTime = time.time()
            read_memory_until_nonzero(end.receiving_mem)
            endTime = time.time()
            print "-- rmda recv: elapsed time = %f " % (endTime - startTime)

            # Stop sending any more messages over TCP and wait for the final message from the server before terminating
            sock.shutdown(socket.SHUT_WR)

            sock.recv(1024)

            print "---client end"
        print "---sock close"
    print "--- endpoint close"


def main():
    client_mode(sys.argv[1], rdma.get_end_port())


main()
