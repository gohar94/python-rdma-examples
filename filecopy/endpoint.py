# Copyright 2011 Obsidian Research Corp. GLPv2, see COPYING.
import pickle
import socket
import contextlib
import struct
import os, sys
import time
import threading
from mmap import mmap
from collections import namedtuple
import rdma.ibverbs as ibv
from rdma.tools import clock_monotonic
import rdma.path
import rdma.vtools


tx_depth = 100
memsize = 16*1024*1024
POLLING_INTERVAL_MS = 1.0 / 10000 # 0.1ms
zero = struct.pack('<i', 0)


def read_memory_check_nonzero(mem, callback=None):
    mem.seek(0)
    read_bytes = mem.read(4)
    if read_bytes != zero:
        read_unpacked_int = struct.unpack('<i', read_bytes)[0]
        print "BYTE FOUND: %d" % read_unpacked_int
        mem.seek(0)
        mem.write(zero)

        if callback is not None:
            print "Doing callback"
            callback()
        else:
            print "No callback"
        print "Returning"
        return True
    return False

def read_memory_until_nonzero(mem, callback=None):
    while not read_memory_check_nonzero(mem, callback):
        time.sleep(POLLING_INTERVAL_MS)
    print "Returning from big loop"


class MemoryCheckingThread(object):
    """ Threading example class
    The run() method will be started and it will run in the background
    until the application exits.
    """

    def __init__(self, mmap, callback=None):
        """ Constructor
        :type interval: float
        :param interval: Check interval, in milliseconds
        """
        self.mmap = mmap
        self.callback = callback
        self._stop_event = threading.Event()

        thread = threading.Thread(target=self.run, args=())
        thread.daemon = True 
        thread.start()

    def run(self):
        """ Method that runs forever """
        while not self._stop_event.is_set():
            read_memory_check_nonzero(self.mmap, self.callback)
            time.sleep(POLLING_INTERVAL_MS)

    def stop(self):
        self._stop_event.set()


def create_memory_map(size):
    mem = mmap(-1, size)
    mem.write(b"\x00" * size)
    mem.flush()
    return mem


class Endpoint(object):

    def __init__(self, sending_mem_size, receiving_mem_size, dev, use_callback=False, start_thread=False):
        self.ctx = rdma.get_verbs(dev)
        self.cc = self.ctx.comp_channel()
        self.cq = self.ctx.cq(2*tx_depth, self.cc)
        # Polling for Completion Queue (CQ) messages
        self.poller = rdma.vtools.CQPoller(self.cq)
        self.pd = self.ctx.pd()
        # Create the Queue Pair (QP) with Reliable Connection (RC), and associate the CQ with it
        self.qp = self.pd.qp(ibv.IBV_QPT_RC, tx_depth, self.cq, tx_depth, self.cq)
        self.sending_mem = create_memory_map(sending_mem_size)
        self.sending_mem_size = sending_mem_size
        self.sending_mr = self.pd.mr(self.sending_mem,
                                     ibv.IBV_ACCESS_LOCAL_WRITE|ibv.IBV_ACCESS_REMOTE_WRITE)
        self.receiving_mem = create_memory_map(receiving_mem_size)
        # TODO change the flags?
        self.receiving_mr = self.pd.mr(self.receiving_mem,
                                       ibv.IBV_ACCESS_LOCAL_WRITE|ibv.IBV_ACCESS_REMOTE_WRITE)
        self.receiving_mem_size = receiving_mem_size
        if start_thread:
            callback = None
            if use_callback:
                callback = self.send
            self.thread = MemoryCheckingThread(self.receiving_mem, callback=callback)
        else:
            self.thread = None

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        if self.thread is not None:
            self.thread.stop()
        return self.close()

    def close(self):
        print "Endpoint:close"
        if self.ctx is not None:
            self.ctx.close()

    def write_to_memory(self, offset, bytes):
        remaining = self.sending_mem_size - offset
        if len(bytes) > remaining:
            raise Exception("Not enough space in memory region")
        self.sending_mem.seek(offset)
        self.sending_mem.write(bytes)

    def connect(self, peerinfo):
        self.peerinfo = peerinfo
        # Modify the QP to RTS stage
        self.qp.establish(self.path, ibv.IBV_ACCESS_REMOTE_WRITE)

    def send(self):
        # Create a Work Request (WR) for sending the data
        swr = ibv.send_wr(wr_id=0,
                          remote_addr=self.peerinfo.receiving_addr,
                          rkey=self.peerinfo.receiving_rkey,
                          sg_list=self.sending_mr.sge(),
                          opcode=ibv.IBV_WR_RDMA_WRITE,
                          send_flags=ibv.IBV_SEND_SIGNALED) # This will create a work completion entry in the CQ

        n = 1
        depth = min(tx_depth, n, self.qp.max_send_wr)

        tpost = clock_monotonic()

        # Post the WR
        for i in xrange(depth):
            self.qp.post_send(swr)

        # Wait for a completion message in the Completion Queue (CQ)
        completions = 0
        posts = depth
        for wc in self.poller.iterwc(timeout=3):
            if wc.status != ibv.IBV_WC_SUCCESS:
                raise ibv.WCError(wc,self.cq,obj=self.qp)
            completions += 1
            if posts < n:
                self.qp.post_send(swr)
                posts += 1
                self.poller.wakeat = rdma.tools.clock_monotonic() + 1
            if completions == n:
                break
        else:
            raise rdma.RDMAError("CQ timed out")

        tcomp = clock_monotonic()

        rate = self.sending_mem_size/1e6/(tcomp-tpost)
        print "%.1f MB/sec" % rate
