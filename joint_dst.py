from collections import defaultdict
from random import choices
from util import *
import numpy as np
import bisect
import copy

    
class SZ_dst:
    def __init__(self, i_file, min_val, max_val):
        f = open(i_file, "r")
        self.all_keys = defaultdict(int)
        l = f.readline()
        sum_count = 0

        total_pr = 0
        for l in f:
            l = l.strip().split(" ")

            if len(l) == 1:
                continue
            else:
                key = int(float(l[0]))
                val = float(l[1])
                if key >= min_val and key <= max_val:
                    self.all_keys[key] += val                                
                    total_pr += val
                    
        p_keys = list(self.all_keys.keys())
        vals = []
        for k in p_keys:
            vals.append(self.all_keys[k])
        sum_vals = sum(vals)
        vals = [float(x)/sum_vals for x in vals]        
        self.p_keys = p_keys
        self.pr = vals

                
    def sample_keys(self, n):
        return choices(self.p_keys, weights=self.pr,k=n)


class POPULARITY_dst:

    def __init__(self, i_file, min_val, max_val):
        f = open(i_file, "r")
        self.popularities = defaultdict(float)

        l = f.readline()
        key = int(l.strip())
        sum_count = 0

        for l in f:
            l = l.strip().split(" ")
            if len(l) == 1:
                self.popularities[key] = sum_count
                sum_count = 0
                key = int(l[0])
                if key > max_val:
                    break                
            else:
                if key >= min_val:
                    sum_count += float(l[1])

        p_keys = list(self.popularities.keys())
        p_vals = []
        for k in p_keys:
            p_vals.append(self.popularities[k])

        sum_vals = sum(p_vals)
        p_vals   = [float(x)/sum_vals for x in p_vals]

        self.p_keys        = p_keys
        self.probabilities = p_vals


    def sample_keys(self, n):
        return choices(self.p_keys, weights=self.probabilities,k=n)



class POPULARITY_SZ_dst:

    def __init__(self, i_file):

        pop_sz = defaultdict(lambda : defaultdict(int))        
        f = open(i_file, "r")
        
        key = "-"
        keys_cnt = 0

        for l in f:
            l = l.strip().split(" ")
            if len(l) == 1:
                key = int(float(l[0]))
                continue                                
            objs = float(l[1])
            sz   = int(float(l[0]))             
            pop_sz[key][sz] += objs
                
        f.close()
        
        
        self.pop_sz_vals = defaultdict(lambda : list)
        self.pop_sz_prs  = defaultdict(lambda : list)

        sum_n_key = 0

        for key in pop_sz:
            sizes = list(pop_sz[key].keys())
            n_key = key

            self.pop_sz_vals[n_key] = sizes
            sum_n_key += n_key
            prs = []
            for s in sizes:
                prs.append(pop_sz[key][s])
            sum_prs = sum(prs)
            prs = [float(x)/sum_prs for x in prs]
            self.pop_sz_prs[n_key] = prs

        self.sample_each_popularity()


    def sample_each_popularity(self):
        self.samples = defaultdict(list)
        self.sampled_sizes = defaultdict(list)

        for k in self.pop_sz_prs:
            self.sampled_sizes[k] = choices(self.pop_sz_vals[k], weights=self.pop_sz_prs[k], k=10000)

        self.samples_index = defaultdict(int)
        self.popularities = list(self.pop_sz_prs.keys())
        self.popularities.sort()

    def findnearest(self, k):
        ind = bisect.bisect_left(self.popularities, k)
        if ind >= len(self.popularities):
            ind = len(self.popularities) - 1
        return self.popularities[ind]

    def sample(self, k):

        if k not in self.samples_index:
            k = self.findnearest(k)

        curr_index = self.samples_index[k]
        if curr_index >= len(self.sampled_sizes[k]):
            self.sampled_sizes[k] = choices(self.pop_sz_vals[k], weights=self.pop_sz_prs[k], k=10000)
            self.samples_index[k] = 0
            curr_index = 0

        self.samples_index[k] += 1        
        return int(self.sampled_sizes[k][curr_index])




    
class POPULARITY_SZ_dst_backup:

    def __init__(self, i_file):
        f = open(i_file, "r")
        self.pop_sz = defaultdict(lambda: defaultdict(float))
        popularities = defaultdict(float)
        
        l   = f.readline()
        key = int(l.strip())
        sum_count = 0

        sizes = []
        prs = []
        for l in f:
            l = l.strip().split(" ")
            if len(l) == 1:
                sum_prs = sum(prs)
                for i in range(len(sizes)):
                    self.pop_sz[key][sizes[i]] = float(prs[i])/sum_prs
                    
                key = int(float(l[0]))
                sizes = []
                prs  = []
                continue
            else:
                sz = int(float(l[0]))
                pr = float(l[1])
                sizes.append(sz)
                prs.append(pr)
                #self.pop_sz[key][sz]   += pr
                popularities[key] += pr
        f.close()

        ## Overall popularity distribution
        p_keys = list(popularities.keys())
        p_vals = []
        for k in p_keys:
            p_vals.append(popularities[k])

        sum_vals = sum(p_vals)
        p_vals   = [float(x)/sum_vals for x in p_vals]

        self.p_keys        = p_keys
        self.p_vals        = p_vals

        ## Popularity based size distribution
        self.pop_sz_keys = defaultdict(lambda : list)
        self.pop_sz_prs  = defaultdict(lambda : list)
        
        for key in self.pop_sz:
            sizes = list(self.pop_sz[key].keys())
            self.pop_sz_keys[key] = sizes
            prs = []
            for s in sizes:
                prs.append(self.pop_sz[key][s])
            sum_prs = sum(prs)
            prs = [float(x)/sum_prs for x in prs]
            self.pop_sz_prs[key] = prs
        
        self.sample_each_popularity()


    def print_probability(self, p, k):
        print(self.pop_sz[p][k])
        return

        
    def sample_each_popularity(self):
        self.samples = defaultdict(list)
        self.sampled_sizes = defaultdict(list)

        for k1 in self.pop_sz_prs:
            self.sampled_sizes[k1] = choices(self.pop_sz_keys[k1], weights=self.pop_sz_prs[k1], k=10000)

        self.samples_index = defaultdict(int)
        self.popularities = list(self.pop_sz_prs.keys())
        self.popularities.sort()


    def findnearest(self, k):
        ind = bisect.bisect_left(self.popularities, k)
        if ind >= len(self.popularities):
            ind = len(self.popularities) - 1
        return self.popularities[ind]


    def sample(self, k):
        if k not in self.samples_index:
            k = self.findnearest(k)

        curr_index = self.samples_index[k]
        if curr_index >= len(self.sampled_sizes[k]):
            self.sampled_sizes[k] = choices(self.pop_sz_keys[k], weights=self.pop_sz_prs[k], k=10000)
            self.samples_index[k] = 0
            curr_index = 0

        self.samples_index[k] += 1        
        return int(self.sampled_sizes[k][curr_index])

    def sample_keys(self, n):
        return choices(self.p_keys, weights=self.p_vals,k=n)

    

class SampleFootPrint:
    def __init__(self, fd, hr_type, min_val, max_val):
        self.sd_keys = []
        self.sd_vals = []
        self.sd_index = defaultdict(lambda : 0)
        self.SD = defaultdict(lambda : 0)        

        f = open(i_file, "r")
        l = f.readline()
        l = l.strip().split(" ")
        if hr_type == "bhr":
            bytes_miss = float(l[-1])
            bytes_req = float(l[1])
            self.SD[-1] = float(bytes_miss)/bytes_req
        else:
            reqs_miss = float(l[-2])
            reqs = float(l[0])
            self.SD[-1] = float(reqs_miss)/reqs                    
            self.sd_index[-1] = 0

        total_pr = 0
        for l in f:
            l = l.strip().split(" ")
            sd = int(l[1])
            self.SD[sd] += float(l[2])        
            total_pr += float(l[2])
            
        self.sd_keys = list(self.SD.keys())
        self.sd_keys.sort()

        i = 1
        curr_pr = 0
        self.sd_pr = defaultdict()

        for sd in self.sd_keys:
            self.sd_vals.append(self.SD[sd])
            curr_pr += self.SD[sd]

            if sd >= 0:
                self.sd_pr[sd] = float(curr_pr - self.SD[-1])/(1 - self.SD[-1])

            self.sd_index[sd] = i
            i += 1            
                            
    def sample_keys(self, obj_sizes, sampled_sds, n):
        return choices(self.sd_keys, weights = self.sd_vals, k = n)


    def findPr(self, sd):
        return self.sd_pr[sd]

