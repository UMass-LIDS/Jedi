from collections import defaultdict
from random import choices
from FDUtils import *
import gzip
import bisect

TB = 1000000000

class PFD():
    def __init__(self, sd_limit=10*TB, iat_limit=4000000):

        self.sd_limit  = sd_limit
        self.iat_limit = iat_limit
        
        self.no_reqs       = 0
        self.total_bytes   = 0
        self.start_tm      = 0
        self.end_tm        = 0
        self.requests_miss  = 0
        self.bytes_miss     = 0
        self.st            = defaultdict(lambda : defaultdict(lambda : defaultdict(float)))
        
    def read_from_file(self, f, iat_gran, sd_gran):
        
        l = f.readline().decode()
        l = l.strip().split(" ")

        self.no_reqs      = int(l[0])
        self.total_bytes  = float(l[1])
        self.start_tm     = int(l[2])
        self.end_tm       = int(l[3])
        self.requests_miss = int(l[4])
        self.bytes_miss    = float(l[5])
        self.req_rate     = self.no_reqs/(self.end_tm - self.start_tm)
        self.byte_rate    = self.total_bytes/(self.end_tm - self.start_tm)

        print("sd_gran : ", sd_gran)
        
        for l in f:
            l = l.decode()
            l   = l.strip().split(" ")
            p   = str(int(l[0])) + ":" + str(int(l[1]))
            sd  = int(float(l[2])) * 1000
            iat = int(float(l[3])) * 100
            if iat >= 0 and sd >= 0:
                iat = int(iat // iat_gran) * iat_gran
            pr  = float(l[4])            
            self.st[p][iat][sd] += pr

        self.iat_gran = iat_gran
        self.sd_gran  = sd_gran 
            
    ## convolve oneself with fd2 and store result in fd_res
    def addition(self, fd2, fd_res):

        print("Computing the traffic model for the traffic mix")
        
        rate1 = self.req_rate
        rate2 = fd2.req_rate
        
        convolve_3d_fft(self.st, fd2.st, fd_res.st, rate1, rate2, self.sd_gran)
        fd_res.no_reqs       = self.no_reqs + fd2.no_reqs
        fd_res.total_bytes   = self.total_bytes + fd2.total_bytes
        fd_res.start_tm      = min(self.start_tm, fd2.start_tm)
        fd_res.end_tm        = max(self.end_tm, fd2.end_tm)
        fd_res.requests_miss = self.requests_miss + fd2.requests_miss
        fd_res.bytes_miss    = self.bytes_miss + fd2.bytes_miss
        fd_res.req_rate      = self.req_rate + fd2.req_rate
        fd_res.byte_rate     = self.byte_rate + fd2.byte_rate
        #fd_res.shave_off_tail()
        #fd_res.condense()

        
    def shave_off_tail(self):
        pr = 0
        tail = []
        for p in self.st:
            for t in self.st[p]:
                for s in self.st[p][t]:
                    if s > self.sd_limit:
                        pr += self.st[p][t][s]
                        tail.append([p,t,s])
        for tup in tail:
            p = tup[0]
            t = tup[1]
            s = tup[2]
            del self.st[p][t][s]
            
        self.requests_miss += pr*self.no_reqs
        self.bytes_miss    += pr*self.total_bytes


    def condense(self):
        st_sub = defaultdict(lambda : defaultdict(lambda: defaultdict(float)))
        for p in self.st.keys():
            for t in self.st[p].keys():
                for sd in self.st[p][t].keys():
                    
                    if sd <= 100 * GB:
                        sd_sub = float(sd)/200000
                        sd_sub = int(sd) * 200000
                    elif sd <= TB:
                        sd_sub = float(sd)/(10*GB)
                        sd_sub = int(sd) * 10*GB
                    else:
                        sd_sub = float(sd)/(100*GB)
                        sd_sub = int(sd) * 100*GB            
                    
                    st_sub[p][t][sd_sub] += self.st[p][t][sd]

        self.st = st_sub
        
    
    def scale(self, scale_factor, iat_gran):
        
        self.no_reqs       *= scale_factor
        self.total_bytes   *= scale_factor
        self.requests_miss *= scale_factor
        self.bytes_miss    *= scale_factor
        self.req_rate      *= scale_factor
        self.byte_rate     *= scale_factor

        st_sub = defaultdict(lambda : defaultdict(lambda: defaultdict(float)))

        for p in self.st.keys():
            for iat in self.st[p].keys():
                if iat >= 0:
                    t = float(iat)/scale_factor
                    t = (float(t) // iat_gran) * iat_gran
                else:
                    t = iat
                for sd in self.st[p][iat].keys():
                    st_sub[p][t][sd] += self.st[p][iat][sd]

        self.st = st_sub

    
    def setupSampling(self):
        self.sd_keys = []
        self.sd_vals = []

        SD = defaultdict(lambda :0)

        for p in self.st:
            for t in self.st[p]:
                for s in self.st[p][t]:
                    SD[s] += self.st[p][t][s]

        SD[-1] = float(self.requests_miss)/self.no_reqs
                    
        self.sd_keys = list(SD.keys())
        self.sd_keys.sort()

        self.sd_pr = defaultdict()
        curr_pr    = 0 
        for sd in self.sd_keys:
            self.sd_vals.append(SD[sd])
            curr_pr += SD[sd]
            if sd >= 0:
                self.sd_pr[sd] = float(curr_pr - SD[-1])/(1 - SD[-1])
                   
        print("Finished reading the input models")

            
    def sample(self, n):
        return choices(self.sd_keys, weights=self.sd_vals, k=n)

    
    def findPr(self, sd):
        return self.sd_pr[sd]


    ### For a given popularity find stack distance
    def setupPopularityBasedStackDistance(self):
        self.popularity_sd = defaultdict(lambda : defaultdict(float))
        
        for p in self.st:
            for t in self.st[p]:
                for s in self.st[p][t]:
                    ## Ignoring stack distances that are lesser than 0
                    ## i.e., the first access is to be ignored
                    if s >= 0:
                        self.popularity_sd[p][s] += self.st[p][t][s]

        self.pop_sd_vals = defaultdict(lambda : [])
        self.pop_sd_prs  = defaultdict(lambda : [])
        
        for p in self.popularity_sd:

            ## Store available stack distances for the popularity score
            sds = list(self.popularity_sd[p].keys())
            self.pop_sd_vals[p] = sds

            ## Find the probability for a stack distance marginalized by popularity
            prs = []
            for sd in sds:
                prs.append(self.popularity_sd[p][sd])

            sum_prs = sum(prs)
            prs = [float(x)/sum_prs for x in prs]
            self.pop_sd_prs[p] = prs

        self.sampleStackDistancesForPopularity()
            

    ### For a given popularity find stack distance
    def sampleStackDistancesForPopularity(self):
        self.sampled_sds_for_popularity = defaultdict(lambda : [])

        self.popularities = set()
        self.popularity_sizes = defaultdict(lambda : set())
        
        for p in self.pop_sd_prs:
            self.sampled_sds_for_popularity[p] = choices(self.pop_sd_vals[p], weights=self.pop_sd_prs[p], k=10000)
            pop = int(p.split(":")[0])
            sz  = int(p.split(":")[1])
            self.popularities.add(pop)
            self.popularity_sizes[pop].add(sz)

        self.samples_index_popularity = defaultdict(int)

        self.popularities = list(self.popularities)
        self.popularities.sort()
        for p in self.popularities:
            self.popularity_sizes[p] = list(self.popularity_sizes[p])
            self.popularity_sizes[p].sort()                       
            

    ### For a given popularity find stack distance
    def sampleStackDistanceGivenPopularity(self, p):

        def findNearest(p):
            p = p.split(":")
            pop = int(p[0])
            sz  = int(p[1])

            ind = bisect.bisect_left(self.popularities, pop)
            if ind >= len(self.popularities):
                ind = len(self.popularities) - 1
            pop = self.popularities[ind]
            ind = bisect.bisect_left(self.popularity_sizes[pop], sz)
            if ind >= len(self.popularity_sizes[pop]):
                ind = len(self.popularity_sizes[pop]) - 1                

            sz = self.popularity_sizes[pop][ind]                
            return str(pop) + ":" + str(sz)

        if p not in self.sampled_sds_for_popularity:
            p = findNearest(p)

        curr_index = self.samples_index_popularity[p]
        if curr_index >= len(self.sampled_sds_for_popularity[p]):
            self.sampled_sds_for_popularity[p] = choices(self.pop_sd_vals[p], weights=self.pop_sd_prs[p], k=10000)
            self.samples_index_popularity[p]   = 0
            curr_index = 0
            
        self.samples_index_popularity[p] += 1
        return int(self.sampled_sds_for_popularity[p][curr_index])


    ## For a given stack distance find popularity
    def setupStackDistanceBasedStackPopularity(self):
        self.sd_popularity = defaultdict(lambda : defaultdict(int))
        
        for p in self.st:
            for t in self.st[p]:
                for s in self.st[p][t]:
                    self.sd_popularity[s][p] += self.st[p][t][s]

        self.sd_pop_vals = defaultdict(lambda : [])
        self.sd_pop_prs  = defaultdict(lambda : [])

        for sd in self.sd_popularity:

            ## Store available stack distances for the popularity score
            pops = list(self.sd_popularity[sd].keys())
            self.sd_pop_vals[sd] = pops

            ## Find the probability for a stack distance marginalized by popularity
            prs = []
            for p in pops:
                prs.append(self.sd_popularity[sd][p])
            sum_prs = sum(prs)
            prs = [float(x)/sum_prs for x in prs]
            self.sd_pop_prs[sd] = prs

        self.samplePopularityForStackDistance()
            

    def samplePopularityForStackDistance(self):
        self.sampled_popularity_for_sds = defaultdict(lambda : [])

        for sd in self.sd_pop_prs:
            self.sampled_popularity_for_sds[sd] = choices(self.sd_pop_vals[sd], weights=self.sd_pop_prs[sd], k=10000)
            
        self.samples_index_sds = defaultdict(int)

        self.sds  = list(self.sd_pop_prs.keys())
        self.sds.sort()
            

    def samplePopularityGivenStackDistance(self, sd):

        def findNearest(sd):
            ind = bisect.bisect_left(self.sds, sd)
            if ind >= len(self.sds):
                ind = len(self.sds) - 1
            return self.sds[ind]

        if sd not in self.sampled_popularity_for_sds:
            sd = findNearest(sd)
        
        curr_index = self.samples_index_sds[sd]
        
        if curr_index >= len(self.sampled_popularity_for_sds[sd]):
            self.sampled_popularity_for_sds[sd] = choices(self.sd_pop_vals[sd], weights=self.sd_pop_prs[sd], k=10000)
            self.samples_index_sds[sd]   = 0
            curr_index = 0
            
        self.samples_index_sds[sd] += 1
        return int(self.sampled_popularity_for_sds[sd][curr_index])

       
    def write_pfd_to_file(self, f):
        f.write(str(self.no_reqs) + " " + str(self.total_bytes) + " " + str(self.start_tm) + " " + str(self.end_tm) + " " + str(self.requests_miss) + " " + str(self.bytes_miss) + "\n")
        for p in self.st:
            p_ = p.split(":")
            pop = p_[0]
            sz  = p_[1] 
            for iat in self.st[p]:
                for sd in self.st[p][iat]:
                    if self.st[p][iat][sd] >= 1e-11:
                        f.write(str(pop) + " " + str(sz) + " " + str(float(sd/1000)) + " " + str(iat) + " " + str(self.st[p][iat][sd]) + "\n")
                    
        f.close()
