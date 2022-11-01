import struct
import gzip
from collections import defaultdict

class parser:
    def __init__(self, f_name, mode):

        self.file = f_name
        self.sz_dst = defaultdict()
        self.pop_dst = defaultdict(int)
        self.mode = mode
        
    def readline(self):
        pass

    def open(self):
        pass

class binaryParser(parser):
    def __init__(self, f_name, mode):

        parser.__init__(self, f_name, mode)

    def open(self):

        self.s = struct.Struct("III")
        self.ifile = open(self.file, "rb")

    def readline(self):

        b   = self.ifile.read(12)
        r   = self.s.unpack(b)
        sz  = float(r[2])/1000
        obj = r[1]
        tm  = r[0]
        
        self.sz_dst[obj]   = sz
        self.pop_dst[obj] += 1

        return obj, int(sz), int(tm)


class euParser(parser):
    def __init__(self, f_name, mode):
        parser.__init__(self, f_name, mode)
        
    def open(self):
        self.ifile = open(self.file, "r")
        l = self.ifile.readline()

    def readline(self):
        l = self.ifile.readline()
        l = l.strip().split(" ")

        tm = int(l[0])
        obj = str(l[1]) + ":" + str(l[2])
        sz = int(float(l[4])/1000)
        tc = int(l[2])
        if sz <= 0:
            sz = 1

        if self.mode == "tc":
            return obj, sz, tm, tc
        else:
            return obj, sz, tm

class allParser(parser):
    def __init__(self, f_name, mode):
        parser.__init__(self, f_name, mode)
        
    def open(self):
        self.ifile = gzip.open(self.file, "rb")
        l = self.ifile.readline()

    def readline(self):
        l = self.ifile.readline().decode("utf-8")
        l = l.strip().split(" ")

        tm = int(l[0])
        obj = int(l[1])
        sz = int(float(l[2])/1000)
        if sz <= 0:
            sz = 1
        tc = int(l[3])
        obj = str(obj) + ":" + str(tc)

        if self.mode == "tc":
            return obj, sz, tm, tc
        else:
            return obj, sz, tm
        
    
class gzParser(parser):
    def __init__(self, f_name, mode):
        parser.__init__(self, f_name, mode)
        
    def open(self):
        self.ifile = gzip.open(self.file, "rb")
        l = self.ifile.readline()

    def readline(self):
        l = self.ifile.readline()
        l = l.strip().split(" ")

        tm = int(l[0])
        obj = int(l[1])
        sz = int(float(l[2]))
        if sz <= 0:
            sz = 1

        return obj, sz, tm

class genParser(parser):
    def __init__(self, f_name, mode):
        parser.__init__(self, f_name, mode)
        
    def open(self):
        self.ifile = open(self.file, "r")
        l = self.ifile.readline()

    def readline(self):
        l = self.ifile.readline()
        l = l.strip().split(",")

        tm = int(l[0])
        obj = str(l[1])
        sz = int(float(l[2]))

        return obj, sz, tm
    

class outputParser(parser):
    def __init__(self, f_name, sz_f_name):
        self.file = f_name
        self.sz_file = sz_f_name
        self.i = 0
        
    def open(self):
        self.ifile = open(self.file, "r")
        self.trace = self.ifile.readline()
        self.trace = self.trace.split(",")[:-1]
        
        self.i_sz_file = open(self.sz_file, "r")
        self.sizes = self.i_sz_file.readline()
        self.sizes = self.sizes.split(",")[:-1]
        self.sizes = [int(x) for x in self.sizes]

    def readline(self):
                
        tm = 0
        obj = self.trace[self.i]
        sz = int(self.sizes[int(obj)])
        self.i += 1
        return obj, sz, tm
    

    
    
