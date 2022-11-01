import sys
from parser import *
from collections import defaultdict
from gen_trace import *
from treelib import *
from util import *
import random

TB = 1000000000
GB = TB/1000
MIL = 1000000

## objects are assumed to be in KB
class cache:
    
    def __init__(self, max_sz):
        self.max_sz = max_sz
        self.items = defaultdict()
        self.curr_sz = 0
        self.debug = open("tmp.txt", "w")
        self.no_del = 0
        
    def initialize(self, inital_objects, sizes, initial_times):        

        ## create a tree structure
        trace_list, self.curr_sz = gen_leaves(initial_objects, sizes, None, None, self.items, initial_times)
        st_tree, lvl = generate_tree(trace_list)
        root = st_tree[lvl][0]
        root.is_root = True
        self.curr = st_tree[0][0]
        self.prev_rid = root.id        
        

    ## If object is in cache return the appropriate stack distance and inter-arrival-time
    ## Else insert the object at the head and return sd = -1 and iat = -1
    def insert(self, o, sz, tm):
        
        if o in self.items:            

            n = self.items[o]
            dt = tm - n.last_access
            
            if self.curr.obj_id == o:
                self.curr.last_access = tm
                return 0, dt
            
            sd = self.curr.findUniqBytes(n, self.debug) + self.curr.s + n.s            
            n.delete_node(self.debug)
            self.curr_sz -= n.s           
            n.s = sz            
            n.last_access = tm
            n.set_b()

            p_c = self.curr.parent            
            self.root = p_c.add_child_first_pos(n, self.debug)
            self.curr_sz += n.s

            if self.root.id != self.prev_rid:
                self.prev_rid = self.root.id
                
            self.curr = n
            
        else:

            n = node(o, sz)
            n.set_b()
            n.last_access = tm
            
            self.items[o] = n

            p_c = self.curr.parent
            self.root = p_c.add_child_first_pos(n, self.debug)

            if self.root.id != self.prev_rid:
                self.prev_rid = self.root.id
                            
            self.curr = n
            self.curr_sz += sz
                                
            sd = -1
            dt = -1
            
        ## if cache not full
        while self.curr_sz > self.max_sz:
            try:
                sz, obj = self.root.delete_last_node(self.debug)
                self.curr_sz -= sz
                del self.items[obj]
                self.no_del += 1
            except:
                print("no of deletions : ", self.no_del, obj, o)

        return sd, dt
                

lru             = cache(10*TB)
initial_objects = list()
initial_times   = {}

## Required quantities to be processed later
obj_sizes         = defaultdict(int)
obj_iats          = defaultdict(list)
sd_distances      = defaultdict(list)
sd_byte_distances = defaultdict(lambda : defaultdict(lambda : 0))
obj_reqs          = defaultdict(int)
bytes_in_cache    = 0
line_count        = 0

input_file        = sys.argv[1]
output_directory  = sys.argv[2]

f = open(input_file, "r")


## Initialize the LRU stack with objects from the trace
i = 0
while bytes_in_cache < 10*MIL:

    l   = f.readline()
    l   = l.strip().split(",")    
    tm  = int(l[0])
    obj = str(l[1])
    sz  = int(l[2])
        
    if sz <= 0:
        sz = 1

    obj_reqs[obj] += 1
    obj_iats[obj].append(-1)
    
    if obj not in obj_sizes:

        initial_objects.append(obj)        
        obj_sizes[obj] = sz
        bytes_in_cache += sz

    initial_times[obj] = tm

    i += 1
    line_count += 1
    if line_count % 100000 == 0:
        print(line_count)
    

lru.initialize(initial_objects, obj_sizes, initial_times)

## Stats to be processed later
i          = 0
line_count = 0

max_len    = 150000000
start_tm   = 0
total_bytes_req = 0
total_reqs      = 0
total_misses    = 0
bytes_miss      = 0

## Re-initialize obj_sizes and obj_reqs
obj_sizes   = defaultdict(int)
obj_reqs    = defaultdict(int)
obj_seen    = defaultdict(int)

max_iat = 100 * MIL

## Stack distance is grouped in multiples of 200 MB and inter-arrival time in 200 seconds
## Run the trace through LRU cache to obtain the footprint descriptors
while True:

    l   = f.readline()
    l   = l.strip().split(",")
    try:
        tm  = int(l[0])
        obj = str(l[1])
        sz  = int(l[2])
        
        if sz <= 0:
            sz = 1
    except:
        break
        
    if i == 0:
        start_tm = tm            

    try:
        k = lru.insert(obj, sz, tm)
    except:
        break

    sd, iat = k    
    
    if line_count%100000 == 0:
        print("Processed : ", line_count)    
    
    if sd != -1:

        if sd <= 100 * GB:
            sd = float(sd)/200000
            sd = int(sd) * 200000
        elif sd <= TB:
            sd = float(sd)/(20*GB)
            sd = int(sd) * 20*GB
        elif sd <= 10*TB:
            sd = float(sd)/(100*GB)
            sd = int(sd) * 100*GB            

        if iat <= 2000:
            iat = float(iat)/200
            iat = int(iat) * 200
        elif iat <= 20000:
            iat = float(iat)/400
            iat = int(iat) * 400
        else:
            iat = float(iat)/1000
            iat = int(iat) * 1000            


        if obj in obj_seen:
            sd_distances[obj].append((sd, iat))
        else:
            sd_distances[obj].append((-1, -1))
            total_misses += 1
            bytes_miss   += 1
    else:
        if obj in obj_seen:
            sd_distances[obj].append((10*TB+200000, max_iat))
        else:
            sd_distances[obj].append((-1, -1))
            total_misses += 1
            bytes_miss   += sz
                
    obj_seen[obj] = 1
    obj_reqs[obj] += 1
    obj_sizes[obj] = sz    
    total_bytes_req += sz
    total_reqs      += 1        
    i += 1
    
    line_count += 1
    if line_count > max_len:
        break

end_tm = tm        

## Write the joint distribution of <popularity, footprint_descriptor>
f = open(output_directory + "/pfd" + ".txt", "w")
write_popularity_descriptor(f, total_reqs, total_bytes_req, start_tm, end_tm, total_misses, bytes_miss, sd_distances, obj_reqs, obj_sizes)

## Write the joint probability distribution of the popularity of the object (i.e., the number of requests
## made for the object) and the size of the object
f = open(output_directory + "/popularity_sz.txt", "w")
write_popularity_dst(f, obj_reqs, obj_sizes)




    
        


