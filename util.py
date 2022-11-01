from treelib import *
from gen_trace import *
from collections import defaultdict
import numpy as np
import matplotlib.pyplot as plt


## Dictionary related utility functions
def plot_dict(x, label="-"):
    keys = list(x.keys())
    keys.sort()

    vals = []
    for k in keys:
        vals.append(x[k])
    
    sum_vals = sum(vals)
    vals = [float(x)/sum_vals for x in vals]
    vals = np.cumsum(vals)
    
    plt.plot(keys, vals, label=label)#, marker="^", markersize=3, markevery=500)


def save_dict(x, f):
    keys = list(x.keys())
    keys.sort()

    vals = []
    for k in keys:
        vals.append(x[k])
    
    sum_vals = sum(vals)
    vals = [float(x)/sum_vals for x in vals]
    vals = np.cumsum(vals)
    i = 0
    for k in keys:
        f.write(str(k) + " " + str(vals[i]) + "\n")
        i += 1


def get_dict(x, max_len):

    keys = list(x.keys())
    keys.sort()
    vals = []

    for k in keys:
        vals.append(x[k])

    vals = [float(x)/max_len for x in vals]
    return keys, vals

def get_dict_2(x, max_len):

    keys = list(x.keys())
    keys.sort()
    vals = []

    for k in keys:
        vals.append(x[k])

    max_len = sum(vals)
    vals = [float(x)/max_len for x in vals]

    return keys, vals

def convert_to_dict(x, max_len, type=1):

    a = defaultdict(int)

    for v in x:
        a[v] += 1
        
    if type==1:
        keys, vals = get_dict(a, max_len)
    else :
        keys, vals = get_dict_2(a, max_len)
        
    return keys, vals


## List related utility functions
def save_list(x, f):

    a = defaultdict(int)

    for v in x:
        a[v] += 1
    save_dict(a, f)
    

def plot_list(x, label="-", maxlim=100000000000):

    a = defaultdict(int)

    for v in x:
        if v < maxlim:
            a[v] += 1

    plot_dict(a, label)

def write_footprint_descriptor(f, total_reqs, total_bytes_req, start_tm, end_tm, total_misses, bytes_miss, sd_distances):
    f.write(str(total_reqs) + " " + str(total_bytes_req) + " " + str(start_tm) + " " + str(end_tm) + " " + str(total_misses) + " " + str(bytes_miss) + "\n")
    iat_keys = list(sd_distances.keys())
    iat_keys.sort()
    for iat in iat_keys:
        keys, vals = convert_to_dict(sd_distances[iat], total_reqs)
        for i in range(len(keys)):
            f.write(str(iat) + " " + str(keys[i]) + " " + str(vals[i]) + "\n")
    f.close()

def write_byte_footprint_descriptor(f, total_reqs, total_bytes_req, start_tm, end_tm, total_misses, bytes_miss, sd_byte_distances):
    f.write(str(total_reqs) + " " + str(total_bytes_req) + " " + str(start_tm) + " " + str(end_tm) + " " + str(total_misses) + " " + str(bytes_miss) + "\n")
    iat_keys = list(sd_byte_distances.keys())
    iat_keys.sort()
    for iat in iat_keys:
        sd_keys = list(sd_byte_distances[iat].keys())
        sd_keys.sort()
        for sd in sd_keys:
            count = sd_byte_distances[iat][sd]
            count = float(count)/total_bytes_req
            f.write(str(iat) + " " + str(sd) + " " + str(count) + "\n")
    f.close()
    
def write_iat_sz_dst(f, obj_iats, obj_sizes):
    avg_obj_iat = defaultdict(int)
    no_objects = 0
    one_hits = 0
    for obj in obj_iats:
        if len(obj_iats[obj]) > 1:
            iat = np.mean(obj_iats[obj][1:])/200
            iat = int(iat) * 200        
        else:
            iat = -1
            one_hits += 1        
        avg_obj_iat[obj] = iat
        no_objects += 1
    
    iat_sz = defaultdict(list)
    for obj in obj_sizes:
        iat = avg_obj_iat[obj]
        iat_sz[iat].append(obj_sizes[obj])

    j = 0
    for iat in iat_sz:
        f.write(str(iat) + "\n")
        keys, vals = convert_to_dict(iat_sz[iat], no_objects)
        for i in range(len(keys)):
            f.write(str(keys[i]) + " " + str(vals[i]) + "\n")
        j += 1
        if j % 10000 == 0:
            print("Parsed : ", j)        
    f.close()


def write_popularity_dst(f, obj_reqs, obj_sizes):
    total_objects = len(obj_sizes)
    pop_sz = defaultdict(list)

    for obj in obj_sizes:
        pop = obj_reqs[obj]
        sz  = obj_sizes[obj]

        # ### How do you modify and generalize this.
        if pop > 10 and pop <= 50:
            pop = int(pop/2)*2
        if pop > 50 and pop <= 500:
            pop = int(pop/20)*20
        elif pop > 500 and pop <= 5000:
            pop = int(pop/50)*50
        elif pop > 5000:
            pop = int(pop/200)*200
            
        ### How do you modify and generalize this.        
        if sz <= 200:
            sz = int(round(sz/10))*10
            if sz <= 0:
                sz = 1
        elif sz <= 1000:
            sz = int(round(sz/100))*100
        elif sz <= 10000:
            sz = int(round(sz/1000))*1000
        elif  sz <= 100000:
            sz = int(round(sz/20000))*20000
        else:
            sz = int(round(sz/100000))*100000
        
        pop_sz[pop].append(sz)

    for p in pop_sz:
        f.write(str(p) + "\n")
        keys, vals = convert_to_dict(pop_sz[p], total_objects)
        for i in range(len(keys)):
            f.write(str(keys[i]) + " " + str(vals[i]) + "\n")
    f.close()


def write_popularity_descriptor(f, total_reqs, total_bytes_req, start_tm, end_tm, total_misses, bytes_miss, sd_distances, obj_reqs, obj_sizes):
    f.write(str(total_reqs) + " " + str(total_bytes_req) + " " + str(start_tm) + " " + str(end_tm) + " " + str(total_misses) + " " + str(bytes_miss) + "\n")
    pop_sd = defaultdict(lambda : defaultdict(lambda : defaultdict(int)))
    
    for obj in sd_distances:

        pop = obj_reqs[obj]
        sz  = obj_sizes[obj]

        ### How do you modify and generalize this.
        if pop > 50 and pop <= 500:
            pop = int(pop/10)*10
        elif pop > 500 and pop <= 5000:
            pop = int(pop/50)*50
        elif pop > 5000:
            pop = int(pop/100)*100
            
        ### How do you modify and generalize this.        
        if sz <= 500:
            sz = int(round(sz/10))*10
            if sz <= 0:
                sz = 1
        elif sz <= 1000:
            sz = int(round(sz/100))*100
        elif sz <= 10000:
            sz = int(round(sz/1000))*1000
        elif  sz <= 100000:
            sz = int(round(sz/10000))*10000
        else:
            sz = int(round(sz/100000))*100000

        sd_iats = sd_distances[obj]
        for sd_iat in sd_iats:
            sd  = sd_iat[0]
            iat = sd_iat[1]
            pop_sd[(pop, sz)][sd][iat] += 1

    pop_keys = list(pop_sd.keys())
    pop_keys.sort()

    for key in pop_keys:
        p  = key[0]
        sz = key[1]
            
        sds = list(pop_sd[key].keys())
        sds.sort()
        for sd in sds:
            iats = list(pop_sd[key][sd].keys())            
            iats.sort()
            for iat in iats:
                f.write(str(p) + " " + str(sz) + " " + str(sd/1000) + " " + str(iat/100) + " " + str(round(float(pop_sd[key][sd][iat])/total_reqs, 9)) + "\n")
    f.close()
                
