from constants import *
from collections import defaultdict
import os
import argparse
import json
import sys


def define_arguments():
    parser = argparse.ArgumentParser(description='TRAGEN')

    parser.add_argument('-a', '--available_fds',   action='store_true',  help="show available footprint descriptors")

    parser.add_argument('-x', '--example',   action='store_true',  help="an example")

    parser.add_argument('-c', '--config_file',   action='store',  help="enter the full path to a json config file, example can be found in OUTPUT/config.json")
    
    return parser


def show_available_fds():
    print("available fds are: ")
    availableTcs = defaultdict()
    f = open("./FOOTPRINT_DESCRIPTORS/available_fds.txt", "r")
    for l in f:
        l = l.strip().split(",")
        tc = l[1]
        print(tc)
    return availableTcs


def create_empty_dirs():
    if not os.path.exists("./OUTPUT/"):            
        os.mkdir("./OUTPUT/")
    if not os.path.exists("./tmp/"):            
        os.mkdir("./tmp/")

            


def show_example():
    print("Here's an example command : python3 tragen_cli.py -c <config_file>")

    
## Fill the arguments as entered by the user.
class Arguments():
    def __init__(self):
        self.traffic_classes = ""
        self.traffic_ratio   = ""
        self.req_rate        = 1000
        self.length          = 100000000

        

def convertToReqRate(traffic_class, traffic_volume):
    f = gzip.open("./FOOTPRINT_DESCRIPTORS/" + traffic_class + "/pfd.txt.gz", "rb")
    l = f.readline()
    l = l.strip().split(" ")

    duration  = int(l[3]) - int(l[2])
    reqs      = int(l[0])
    req_rate  = float(reqs)/duration
    byte_rate = float(l[1])/duration
    gb_rate   = float(byte_rate)/GB

    return (traffic_volume*req_rate)/gb_rate         


def read_config_file(config_file):
    with open(config_file) as config:
        config = json.load(config)

    args = Arguments()

    ## Trace length
    args.length = config["Trace_length"]
    if args.length.isnumeric() == False or int(args.length) <= 0:
        print("Enter valid trace length (in number of requests")

    ## Input unit
    if config["Input_unit"] != "gbps" and config["Input_unit"] != "reqs/s":
        print("Input Input_unit as either 'reqs/s' or 'gbps'")
        sys.exit()
    input_unit = config["Input_unit"]
        
    ## Read traffic classes and their traffic volume
    traffic_classes = []
    traffic_ratio   = []

    for tc in config["Traffic_classes"]:
        traffic_classes.append(tc["traffic_class"])        
        traffic_volume = tc["traffic_volume"]

        if input_unit == "gbps":
            traffic_volume = convertToReqRate(tc["traffic_class"], float(tc["traffic_volume"]))
                
        traffic_ratio.append(traffic_volume)

    total_req_rate = sum([float(x) for x in traffic_ratio])
    args.req_rate = total_req_rate
    
    args.traffic_classes = ":".join([str(x).lower() for x in traffic_classes])
    args.traffic_ratio   = ":".join([str(x) for x in traffic_ratio])

    return args

    
        
        
