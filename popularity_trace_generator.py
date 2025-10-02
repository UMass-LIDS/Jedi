from joint_dst import *
from constants import *
from collections import defaultdict, deque
import datetime
import os
import time
import sys


## A class that defines the functions and objects required to generate a synthetic trace
class TraceGenerator():
    def __init__(self, trafficMixer, args, printBox=None):
        self.trafficMixer = trafficMixer
        self.args = args
        self.log_file = open("./OUTPUT/logfile.txt" , "w")
        self.read_popularity_dst()
        self.read_popularity_sz_dst()
        self.curr_iter = 0
        self.printBox = printBox
        self.mixing_time = 10*MIL
        

    ## Generate a synthetic trace
    def generate(self):

        ## setup popularity footprint descriptor
        pfd = self.trafficMixer.PFD_mix
        pfd.setupSampling()
        pfd.setupPopularityBasedStackDistance()
        
        ## object weight vector
        self.OWV = self.trafficMixer.weight_vector
        
        trafficClasses = self.trafficMixer.trafficClasses        

        self.MAX_SD = pfd.sd_keys[-1]

        ## sample 70 million objects
        print("Sampling the object sizes that will be assigned to the initial objects in the LRU stack ... this will take a while, please go back to sleep")

        if self.printBox != None:
            self.printBox.setText("Sampling initial objects ...")
        
        sizes          = []
        popularities   = []
        i = -1
        for i in range(len(self.OWV)-1):
            P              = self.popularity_dsts[trafficClasses[i]]
            P_SZ           = self.popularity_sz_dsts[trafficClasses[i]]            
            n_popularities = P.sample_keys(int(20*MIL * self.OWV[i]))
            n_sizes        = []

            for p in n_popularities:
                sz = P_SZ.sample(p)
                n_sizes.append(sz)

            popularities.extend(n_popularities)
            sizes.extend(n_sizes)
            
            
        P_SZ = self.popularity_sz_dsts[trafficClasses[i+1]]
        P    = self.popularity_dsts[trafficClasses[i+1]]
        n_popularities = P.sample_keys(int(20*MIL) - len(popularities))
        n_sizes = []
        for p in n_popularities:
            sz = P_SZ.sample(p)
            n_sizes.append(sz)

        popularities.extend(n_popularities)
        sizes.extend(n_sizes)

        popularity_sz = list(zip(popularities, sizes))
        random.shuffle(popularity_sz)
        popularities, sizes = zip(*popularity_sz)        
        popularities = list(popularities)
        sizes = list(sizes)
            
        ## Now fill the objects such that the stack is 10TB
        total_sz = 0
        total_objects = 0
        i = 0
        while total_sz < self.MAX_SD:
            total_sz += sizes[total_objects]
            total_objects += 1
            if total_objects % 100000 == 0:
                print("Initializing the LRU stack ... ", int(100 * float(total_sz)/self.MAX_SD), "% complete")

                if self.printBox != None:
                    self.printBox.setText("Initializing the LRU stack ... " + str(int(100 * float(total_sz)/self.MAX_SD)) + "% complete")
                

        ## debug file
        debug = open("./OUTPUT/debug.txt", "w")

        ## build the LRU stack
        trace = range(total_objects)

        ## Represent the objects in LRU stack as leaves of a B+Tree
        trace_list, ss = gen_leaves(trace, sizes, popularities, self.printBox)
                            
        ## Construct the tree
        st_tree, lvl = generate_tree(trace_list, self.printBox)
        root = st_tree[lvl][0]
        root.is_root = True
        curr = st_tree[0][0]

        curr, root = self.generate_trace(curr, root, total_objects, sizes, popularities, pfd)


    def generate_trace(self, curr, root, total_objects, sizes, popularities, pfd):

        debug = open("./OUTPUT/debug_cache_mix.txt", "w")

        trafficClasses = self.trafficMixer.trafficClasses        
        
        sampled_fds = []
        
        ## Initialize
        c_trace   = []
        tries     = 0
        i         = 0
        j         = 0
        k         = 0
        no_desc   = 0
        fail      = 0
        curr_max_seen = 0

        req_count = [0] * 20 * MIL

        obj_seen = {}
        
        i = 0
        
        while curr != None and i <= self.mixing_time + int(self.args.length):

            ## Sample based on popularity
            pp = popularities[curr.obj_id]

            if pp > 1:
                sz = curr.s

                ### modify and generalize this.
                if pp > 50 and pp <= 500:
                    pop = int(pp/10)*10
                elif pp > 500 and pp <= 5000:
                    pp = int(pp/50)*50
                elif pp > 5000:
                    pp = int(pp/100)*100
            
                ### modify and generalize this.        
                if sz <= 500:
                    sz_ = int(round(sz/10))*10
                    if sz <= 0:
                        sz_ = 1
                elif sz <= 1000:
                    sz_ = int(round(sz/100))*100
                elif sz <= 10000:
                    sz_ = int(round(sz/1000))*1000
                elif  sz <= 100000:
                    sz_ = int(round(sz/10000))*10000
                else:
                    sz_ = int(round(sz/100000))*100000
                
                req_k = str(pp) + ":" + str(sz_)                
                sd = pfd.sampleStackDistanceGivenPopularity(req_k)                
                if sd >= root.s:
                    sd = root.s - 1
                elif sd < 0:
                    sd = root.s - 1
                sampled_fds.append(sd)
            else:
                sd = 0
                
            n = node(curr.obj_id, curr.s)
            n.set_b()

            if i > self.mixing_time:
                c_trace.append(curr.obj_id)
            
            req_count[curr.obj_id] += 1
            obj_seen[curr.obj_id]   = 1
            
            inserted_at_top = False
            
            if req_count[curr.obj_id] >= popularities[curr.obj_id]:

                while root.s < self.MAX_SD:

                    if (total_objects + 1)%(20*MIL) == 0:
                        
                        popularities_ = []
                        sizes_        = []
                        ii = -1
                        for ii in range(len(self.OWV)-1):
                            P              = self.popularity_dsts[trafficClasses[ii]]
                            P_SZ           = self.popularity_sz_dsts[trafficClasses[ii]]
                            n_popularities = P.sample_keys(int(20*MIL * self.OWV[ii]))
                            n_sizes        = []
                            for p in n_popularities:
                                sz = P_SZ.sample(p)
                                n_sizes.append(sz)
                            popularities_.extend(n_popularities)
                            sizes_.extend(n_sizes)

                        P = self.popularity_dsts[trafficClasses[ii+1]]                            
                        P_SZ = self.popularity_sz_dsts[trafficClasses[ii+1]]
                        n_popularities = P.sample_keys(int(20*MIL) - len(popularities_))
                        n_sizes = []
                        for p in n_popularities:
                            sz = P_SZ.sample(p)
                            n_sizes.append(sz)

                        popularities_.extend(n_popularities)
                        sizes_.extend(n_sizes)

                        popularity_sz = list(zip(popularities_, sizes_))
                        random.shuffle(popularity_sz)
                        popularities_, sizes_ = zip(*popularity_sz)        
                        popularities_ = list(popularities_)
                        sizes_ = list(sizes_)
                                                
                        sizes.extend(sizes_)
                        popularities.extend(popularities_)
                        req_count.extend([0]*20*MIL)
                        

                    total_objects += 1
                    sz = sizes[total_objects]
                    n = node(total_objects, sz)
                    n.set_b()                
                    descrepency, x, y = root.insertAt(root.s - 1, n, 0, curr.id, debug)
            
                    if n.parent != None:
                        root = n.parent.rebalance(debug)

            else:
                
                try:
                    descrepency, land, o_id = root.insertAt(sd, n, 0, curr.id, debug)
                    if o_id == curr.obj_id:
                        inserted_at_top = True
                except:
                    print("sd : ", sd, root.s)

                if n.parent != None:
                    root = n.parent.rebalance(debug)
                                        
            if inserted_at_top == False:
                next, success = curr.findNext()
                while (next != None and next.b == 0) or success == -1:
                    next, success = next.findNext()

                del_nodes = curr.cleanUpAfterInsertion(sd, n, debug)
                curr = next
            else:
                del_nodes = curr.cleanUpAfterInsertion(sd, n, debug)
                curr = n
            
            i += 1

            if i % 10000 == 0:
                if i < self.mixing_time:
                    self.log_file.write("Mixing cache : " +  str(i) + "\n")
                    print("Mixing cache  : " +  str(i))
                    self.log_file.flush()
                    if self.printBox != None:
                        self.printBox.setText("Mixing cache : " + str(i*100/self.mixing_time) + "% complete ...")
                        self.curr_iter = i
                else:
                    self.log_file.write("Generating synthetic trace : " +  str(i - self.mixing_time) + "\n")
                    print("Generating synthetic trace  : " +  str(i - self.mixing_time))
                    self.log_file.flush()
                    if self.printBox != None:
                        self.printBox.setText("Mixing cache : " + str((i-self.mixing_time)*100/self.mixing_time) + "% complete ...")
                        self.curr_iter = i                    


        traffic_classes = ":".join([str(x) for x in self.trafficMixer.trafficClasses])
        if not os.path.exists("./OUTPUT/" + str(traffic_classes)):            
            os.mkdir("./OUTPUT/" + str(traffic_classes))
            
        f = open("./OUTPUT/" + str(traffic_classes) + "/gen_sequence.txt", "w")

        with open("./OUTPUT/" + str(traffic_classes) + "/command.txt", 'w') as fp:
            fp.write('\n'.join(sys.argv[1:]))

        # check if FOOTPRINT_DESCRIPTORS/traffic_classes[0]/req_rate.txt exists  
        if len(self.trafficMixer.trafficClasses) == 1 and os.path.exists("./FOOTPRINT_DESCRIPTORS/" + str(traffic_classes) + "/req_rate.txt"):
            self.assign_timestamps_realreqrate(c_trace, sizes, f)
        else:
        ## Assign timestamp based on the specified request rate
            self.assign_timestamps_reqrate(c_trace, sizes, self.args.req_rate, f)
        
        ## We are done!
        if self.printBox != None:
            self.printBox.setText("Done! Ready again ...")
        
        return curr, root

    def assign_timestamps_realreqrate(self, c_trace, sizes, f):

        ## This file specifies the request rate at five minute intervals
        ## We will use this to assign timestamps to the generated trace
        ## The timestamps will be in seconds
        f = open("./FOOTPRINT_DESCRIPTORS/" + str(self.trafficMixer.trafficClasses[0]) + "/req_rate.txt", "r")
        req_rates = []
        for l in f:
            req_rates.append(int(l.strip().split(", ")[1]))
        f.close()

        timestamp = 1
        i = 0
        j = 0
        req_rate = req_rates[j]
        requests_per_second = int(req_rate/300)
        for c in c_trace:
            f.write(str(timestamp) + "," + str(c) + "," + str(sizes[c]) + "\n")
            i += 1
            if i >= requests_per_second:
                timestamp += 1
                i = 0
            if timestamp % 300 == 0:
                j = (j + 1) % len(req_rates)
                req_rate = req_rates[j]
                requests_per_second = int(req_rate/300)

    
    ## Assign timestamp based on the byte-rate of the FD
    def assign_timestamps(self, c_trace, sizes, byte_rate, f):
        timestamp = 0
        KB_added = 0
        KB_rate = byte_rate/1000

        for c in c_trace:
            KB_added += sizes[c]
            f.write(str(timestamp) + "," + str(c) + "," + str(sizes[c]) + "\n")

            if KB_added >= KB_rate:
                timestamp += 1
                KB_added = 0

    def assign_timestamps_reqrate(self, c_trace, sizes, req_rate, f):
        timestamp = 1
        i = 1
        for c in c_trace:
            f.write(str(timestamp) + "," + str(c) + "," + str(sizes[c]) + "\n")

            if (i)%(int(req_rate)) == 0:
                timestamp += 1
            i += 1            

    ## Read object size distribution of the required traffic classes
    def read_obj_size_dst(self):
        self.sz_dsts = defaultdict()

        for c in self.trafficMixer.trafficClasses:
            sz_dst = SZ_dst("./FOOTPRINT_DESCRIPTORS/" + str(c) + "/sz.txt", 0, TB)
            self.sz_dsts[c] = sz_dst


    ## Read object size distribution of the required traffic classes
    def read_popularity_dst(self):
        self.popularity_dsts = defaultdict()

        for c in self.trafficMixer.trafficClasses:
            popularity_dst = POPULARITY_dst("./FOOTPRINT_DESCRIPTORS/" + str(c) + "/popularity_sz.txt", 0, TB)
            self.popularity_dsts[c] = popularity_dst


    def read_popularity_sz_dst(self):
        self.popularity_sz_dsts = defaultdict()

        for c in self.trafficMixer.trafficClasses:
            pop_sz_dst = POPULARITY_SZ_dst("./FOOTPRINT_DESCRIPTORS/" + str(c) + "/popularity_sz.txt")
            self.popularity_sz_dsts[c] = pop_sz_dst
