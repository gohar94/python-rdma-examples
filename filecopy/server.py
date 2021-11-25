# Copyright 2011 Obsidian Research Corp. GLPv2, see COPYING.
import pickle
import socket
import contextlib
import os, sys
import time
import threading
import struct
from mmap import mmap
from collections import namedtuple
import rdma.ibverbs as ibv
from rdma.tools import clock_monotonic
import rdma.path
import rdma.vtools
from endpoint import Endpoint


ip_port = 4444


infotype = namedtuple('infotype', 'path sending_addr sending_rkey sending_size receiving_addr receiving_rkey receiving_size iters')


def server_mode(dev):
    ret = socket.getaddrinfo(None,str(ip_port),0,
                             socket.SOCK_STREAM,0,
                             socket.AI_PASSIVE)
    ret = ret[0]
    with contextlib.closing(socket.socket(ret[0],ret[1])) as sock:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(ret[4])
        sock.listen(1)

        print "Listening port..."
        while True:
            s, _ = sock.accept()

            with contextlib.closing(s):
                totalStartTime = time.time()

                peerInfoStartTime = time.time()
                buf = s.recv(1024)
                peerinfo = pickle.loads(buf)
                peerInfoEndTime = time.time()
                print "sending_mem_size = ", peerinfo.sending_size
                print "--peerinfo: elapsed = %f secs" % (peerInfoEndTime - peerInfoStartTime)

                endPointStartTime = time.time()

                sending_mem_size = 4
                receiving_mem_size = 4

                with Endpoint(sending_mem_size, receiving_mem_size, dev, use_callback=True, start_thread=True) as end:
                    # Prepare the data to send
                    val = 2
                    val_bytes = struct.pack('<i', val)
                    end.write_to_memory(0, val_bytes)

                    with rdma.get_gmp_mad(end.ctx.end_port,verbs=end.ctx) as umad:
                        end.path = peerinfo.path
                        end.path.end_port = end.ctx.end_port
                        rdma.path.fill_path(end.qp,end.path)
                        rdma.path.resolve_path(umad,end.path)

                    s.send(pickle.dumps(infotype(path=end.path,
                                                sending_addr=end.sending_mr.addr,
                                                sending_rkey=end.sending_mr.rkey,
                                                sending_size=end.sending_mem_size,
                                                receiving_addr=end.receiving_mr.addr,
                                                receiving_rkey=end.receiving_mr.rkey,
                                                receiving_size=end.receiving_mem_size,
                                                iters=None)))

                    print "path to peer %r\nMR peer sending_addr=%x peer sending_key=%x\nMR peer receiving_addr=%x peer_receiving_key=%x" % (end.path, peerinfo.sending_addr, peerinfo.sending_rkey, peerinfo.receiving_addr, peerinfo.receiving_rkey)

                    end.connect(peerinfo)
                    endPointEndTime = time.time()
                    print "--endpoint: elapsed = %f secs" % (endPointEndTime - endPointStartTime)

                    startTime = time.time()

                    # Synchronize the transition to RTS
                    s.send("ready")
                    s.recv(1024)

                    s.shutdown(socket.SHUT_WR)
                    s.recv(1024)

                    endTime = time.time()

                    print "--xfer end: elapsed = %f secs" % (endTime - startTime)
                    print "--total   : elapsed = %f secs" % (endTime - totalStartTime)


def main():
    server_mode(rdma.get_end_port())


main()
