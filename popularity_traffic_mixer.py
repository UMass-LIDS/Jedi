import random, sys, math, copy
import numpy as np
import scipy as sp
import scipy.signal
import cmath
import numpy.fft as fft
import gzip
from collections import defaultdict
from pfd import *


## A class that describes the methods to find the traffic model for the traffic mix
class TrafficMixer():
    def __init__(self, args, printBox = None):

        self.printBox = printBox
        
        self.availableTcs   = self.available_traffic_classes()        
        self.trafficRatios  = [float(x) for x in args.traffic_ratio.split(":")]
        self.trafficClasses = [str(x) for x in args.traffic_classes.split(":")]

        self.args     = args

        ## The footprint descriptors are computed at the granularity of 200s for IAT
        ## and 200MB for Stack distance.
        self.iat_gran = 200
        self.sd_gran  = 200000

        ## Recompute the traffic ratios based on the available traffic rate
        trafficRatios  = []
        trafficClasses = []
        i = 0
        for t in self.trafficClasses:

            if t in self.availableTcs:
                trafficClasses.append(t)
                trafficRatios.append(self.trafficRatios[i]/self.availableTcs[t][0])
            i += 1
            
        self.trafficRatios  = trafficRatios
        self.trafficClasses = trafficClasses
        
        self.object_weight_vector()        
        self.readFDs()
        self.scale()
        self.mix()


    ## Read footprint descriptors from file
    def readFDs(self):

        if self.printBox != None:
            self.printBox.setText("Reading FDs from file ...")
        
        self.PFDs = []

        for i in range(len(self.trafficClasses)):
            pfd = PFD()
            print("reading : ", self.trafficClasses[i])                
            f  = gzip.open("./FOOTPRINT_DESCRIPTORS/" + self.trafficClasses[i] + "/pfd.txt.gz", "rb")
            pfd.read_from_file(f, self.iat_gran, self.sd_gran)
            self.PFDs.append(pfd)
            
            
    ## Scale the footprint descriptor based on the traffic volume specified by the user
    def scale(self):
        for i in range(len(self.trafficClasses)):
            self.PFDs[i].scale(self.trafficRatios[i], self.iat_gran)

    ## Compute the traffic models for the traffic mix
    def mix(self):

        if self.printBox != None:
            self.printBox.setText("Computing FD for traffic mix ...")
        
        if len(self.trafficClasses) == 1:
            self.PFD_mix = self.PFDs[0]
            return

        for i in range(len(self.trafficClasses) - 1):
            if i == 0:
                fd_prev_iter = PFD()
                self.PFDs[i].addition(self.PFDs[i+1], fd_prev_iter)
            else:
                fd_new = PFD()
                fd_prev_iter.addition(self.PFDs[i+1], fd_new)
                fd_prev_iter = fd_new

        self.PFD_mix = fd_prev_iter

        
    ## Output a vector that finds the ratio of the number of objects per traffic class
    ## that is to be present in the synthetic trace
    def object_weight_vector(self):
        self.weight_vector = []

        def find_uniqrate(f):
            urate = 0
            l = f.readline()
            l = l.decode()
            for l in f:
                l = l.decode()
                l = l.strip().split(" ")
                iat = int(float(l[2]))
                sd  = int(float(l[1]))
                rt  = float(sd)/(iat + 100)
                pr  = float(l[3])
                urate += pr * rt
            return urate

        for i in range(len(self.trafficClasses)):
            f = gzip.open("./FOOTPRINT_DESCRIPTORS/" + self.trafficClasses[i] + "/pfd.txt.gz", "rb")            
            U = find_uniqrate(f)
            f.close()

            f = open("./FOOTPRINT_DESCRIPTORS/" + self.trafficClasses[i] + "/popularity_sz.txt", "r")
            avg_obj_sz = 0
            for l in f:
                l = l.strip().split(" ")
                if len(l) >= 2:
                    sz = float(l[0])
                    pr = float(l[1])
                    avg_obj_sz += (sz*pr)

            self.weight_vector.append(float(U)/avg_obj_sz)

        normalizing_factor = sum(self.weight_vector)
        self.weight_vector = [float(x)/normalizing_factor for x in self.weight_vector]


    ## Print the available traffic classes
    def available_traffic_classes(self):
        availableTcs = defaultdict()
        f = open("./FOOTPRINT_DESCRIPTORS/available_fds.txt", "r")
        for l in f:
            l = l.strip().split(",")
            tc = l[1]
            volume = float(l[3])
            req_rate = float(l[4])
            availableTcs[tc] = ((req_rate, volume))
        return availableTcs
