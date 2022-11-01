import random, sys, math, copy
import numpy as np
import scipy as sp
import scipy.signal
import cmath
import numpy.fft as fft
from collections import defaultdict
from scipy.optimize import linprog, brentq
import multiprocessing
from multiprocessing import Process
from constants import *
from pfd import *
from multiprocessing.managers import BaseManager, DictProxy
from collections import defaultdict
from multiprocessing import Manager
from pathos.multiprocessing import ProcessingPool as Pool
from itertools import repeat
import sys
import copy
import os

class MyManager(BaseManager):
    pass

MyManager.register('defaultdict', defaultdict, DictProxy)

def convolve(obj):
    
    st1 = obj["st1"] 
    st2 = obj["st2"] 
    rate1 = obj["rate1"] 
    rate2 = obj["rate2"]
    sd_gran = obj["sd_gran"] 
    ps = obj["p"] 
    st12 = {}
    
    st1_fd = get_footprint_descriptor(st1)
    st1_fd_cond = copy.deepcopy(st1_fd)
    cond_prob_positive(st1_fd_cond)

    st2_fd = get_footprint_descriptor(st2)
    st2_fd_cond = copy.deepcopy(st2_fd)
    cond_prob_positive(st2_fd_cond)

    ## Get marginalized popularity descriptor by t and p
    st1_cond_pt = copy.deepcopy(st1)
    cond_prob_3d_positive(st1_cond_pt)
    
    st2_cond_pt = copy.deepcopy(st2)
    cond_prob_3d_positive(st2_cond_pt)
        
    for p in ps:
        t1_set = sorted(set(list(st1[p].keys())))
        t2_set = sorted(set(list(st2[p].keys())))
        t12_set = sorted(set(t1_set + t2_set))

        st12[p] = {}

        print(p)
        
        for t in t12_set:
            # Get individual footprint descriptors
            # Get marginalized footprint descriptors by t                        
            P1_pt = sum([v for v in st1[p][t].values()])    
            P2_pt = sum([v for v in st2[p][t].values()])                    

            st12[p][t] = {}

            if t < 0:
                if -1 not in st12[p][t]:
                    st12[p][t][-1] = 0
                if p in st1 and p in st2:
                    st12[p][-1][-1] += float(rate1)/(rate1 + rate2) * st1[p][-1][-1] + float(rate2)/(rate1 + rate2) * st2[p][-1][-1]
                elif p in st1:
                    st12[p][-1][-1] += float(rate1)/(rate1 + rate2) * st1[p][-1][-1]
                elif p in st2:
                    st12[p][-1][-1] += float(rate2)/(rate1 + rate2) * st2[p][-1][-1]
                continue
            
            for s1 in st1[p][t]:
                for s2 in st2_fd[t]:

                    if s1 < 0 or s2 < 0:
                        continue
                    sd = s1 + s2
                    if sd <= 100 * GB:
                        sd = float(sd)/200000
                        sd = int(sd) * 200000
                    elif sd <= TB:
                        sd = float(sd)/(20*GB)
                        sd = int(sd) * 20*GB
                    else:
                        sd = float(sd)/(100*GB)
                        sd = int(sd) * 100*GB                
                    
                    if sd not in st12[p][t]:
                        st12[p][t][sd] = 0

                    pr = ((rate1 * P1_pt/(rate1 + rate2))) * st1_cond_pt[p][t][s1] * st2_fd_cond[t][s2]
                    st12[p][t][sd] += pr

            for s1 in st2[p][t]:                
                for s2 in st1_fd[t]:
                    if s1 < 0 or s2 < 0:
                        continue
                    
                    sd = s1 + s2                    
                    if sd <= 100 * GB:
                        sd = float(sd)/200000
                        sd = int(sd) * 200000
                    elif sd <= TB:
                        sd = float(sd)/(20*GB)
                        sd = int(sd) * 20*GB
                    else:
                        sd = float(sd)/(100*GB)
                        sd = int(sd) * 100*GB                
                    
                    if sd not in st12[p][t]:
                        st12[p][t][sd] = 0
                    pr = ((rate2 * P2_pt/(rate1 + rate2))) * st2_cond_pt[p][t][s1] * st1_fd_cond[t][s2]
                    st12[p][t][sd] += pr
            

    print("Computed FD for : ", p)
    f3 = open("./tmp/" + str(p) + ".txt", "w")
    for p in st12:
        t_keys = list(st12[p].keys())
        t_keys.sort()
        p_ = p.split(":")[0]
        sz = p.split(":")[1]
        for t in t_keys:
            sd_keys = list(st12[p][t].keys())
            sd_keys.sort()
            for sd in sd_keys:
                f3.write(str(p_) + " " + str(sz) + " " + str(sd) + " " + str(t) + " " + str(st12[p][t][sd]) + "\n") 
                    
def cond_prob(st):
    for t in st.keys():            
        sum_st = sum([v for v in st[t].values()])            
        for s in st[t].keys():
            st[t][s] = st[t][s]/sum_st if sum_st > 0 else 0

def cond_prob_positive(st):
    for t in st.keys():            
        sum_st = sum([st[t][k] for k in list(st[t].keys()) if k >= 0])            
        for s in st[t].keys():
            st[t][s] = st[t][s]/sum_st if sum_st > 0 else 0
            
def cond_prob_3d(st):
    for p in st.keys():        
        for t in st[p].keys():
            prob_sum_st = sum([v for v in st[p][t].values()])            
            for s in st[p][t].keys():
                st[p][t][s] = st[p][t][s]/prob_sum_st if prob_sum_st > 0 else 0
                
def cond_prob_3d_positive(st):
    for p in st.keys():        
        for t in st[p].keys():
            prob_sum_st = sum([st[p][t][k] for k in list(st[p][t].keys()) if k >= 0])            
            for s in st[p][t].keys():
                st[p][t][s] = st[p][t][s]/prob_sum_st if prob_sum_st > 0 else 0
                               
def get_footprint_descriptor(st):
    st_p = defaultdict(lambda : defaultdict(float))
    for p in st.keys():
        for t in st[p].keys():
            for s in st[p][t].keys():
                st_p[t][s] += st[p][t][s]
    return st_p                

# Floor for st and ss
def floor(t_set, t):
    # Error 
    if len(t_set) < 1:
        return -1
    
    # Only one element in t_set 
    if len(t_set) == 1:
        return t_set[0]

    # Match found
    if t in t_set:
        return t

    # t < min
    if t < t_set[0]:
        return t_set[0]
    # t > max
    if t > t_set[len(t_set) - 1]:
        return t_set[len(t_set) - 1]

    # Floor
    start = 0
    end = len(t_set) - 1
    while start < end:
        middle = (start + end) // 2
        if t_set[middle] > t:
            end = middle
        else:
            start = middle
            if (end - start + 1 == 2):
                return t_set[start]

### Convolve 3d footprint descriptors
def convolve_3d_fft(st1, st2, st12, rate1, rate2, sd_gran):

    p1_set = sorted(set(list(st1.keys())))
    p2_set = sorted(set(list(st2.keys())))
    p12_set = sorted(set(p1_set + p2_set))                
        
    manager = Manager()

    args_util = []

    i = 0
    obj = {}

    buckets  = int(len(p12_set)/(multiprocessing.cpu_count() - 7))
    finished = 0
    for p12 in p12_set:
        if i % buckets == 0:
            if i > 0:
                args_util.append(obj)
                finished += len(obj["p"])
            obj = {}
            obj["st1"] = st1
            obj["st2"] = st2
            obj["rate1"] = rate1
            obj["rate2"] = rate2
            obj["sd_gran"] = sd_gran            
            obj["p"] = []
            obj["i"] = i
            
        obj["p"].append(p12)
        i += 1

    args_util.append(obj)
    pool = Pool(processes=multiprocessing.cpu_count() - 8)
    pool.map(convolve, args_util)
    pool.close()
    pool.join()
    
    path="./tmp/"
    for file in os.listdir(path):
        f = open(path + "/" + file, "r")
        for l in f:
            l = l.strip().split()
            p = l[0] + ":" + l[1]
            sd  = int(float(l[2]))
            iat = int(float(l[3]))
            pr = float(l[4])
            if pr > 1e-10:
                st12[p][iat][sd] = pr
        os.remove(path + "/" + file)
            
