#!/usr/bin/env python
# encoding: utf8
#
# Copyright © Burak Arslan <burak at arskom dot com dot tr>,
#             Arskom Ltd. http://www.arskom.com.tr
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
#    1. Redistributions of source code must retain the above copyright notice,
#       this list of conditions and the following disclaimer.
#    2. Redistributions in binary form must reproduce the above copyright
#       notice, this list of conditions and the following disclaimer in the
#       documentation and/or other materials provided with the distribution.
#    3. Neither the name of the owner nor the names of its contributors may be
#       used to endorse or promote products derived from this software without
#       specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER BE LIABLE FOR ANY DIRECT,
# INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
# BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
# DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY
# OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
# NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE,
# EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#

import logging
import subprocess
import re

from time import time
from rpclib.decorator import srpc
from rpclib.service import ServiceBase
from rpclib.model.complex import Iterable
from rpclib.model.primitive import Integer
from rpclib.model.primitive import String
from rpclib.model.binary import ByteArray

from rpclib.util.simple import wsgi_soap_application

try:
    from wsgiref.simple_server import make_server
except ImportError:
    print("Error: example server code requires Python >= 2.5")
    raise

base_dfs_dir = "/user/hduser/"
hadoop_dir = "/usr/local/hadoop/"

def list_algorithms():
    file = open(hadoop_dir+"algorithms.txt","r")
    algos = [line.split() for line in file.read().split("\n") if line != ""]
    return algos

def list_datasets():
    str = subprocess.check_output([hadoop_dir+"bin/hadoop","dfs","-ls",base_dfs_dir+"snat_datasets/"])
    datasets = [re.search(base_dfs_dir+'snat_datasets/([A-z0-9_-]+)',line).group(1) for line in str.split("\n")[1:-1]]
    return datasets

class SnatService(ServiceBase):

    @srpc(String, ByteArray, _returns=Integer)
    def upload_data_set(data_set_name, data_set):
        file = open("/tmp/snat_upload."+data_set_name,"w")
        file.write(data_set_name)
        # bin/hadoop dfs -copyFromLocal isaac/ruby/inf.txt /user/hduser/influence-graph
        return 0 # On failure return 0, else return data_set_id

    @srpc(String, String, ByteArray, _returns=Integer)
    def upload_algorithm(algorithm_name, class_name, jar_file):
        return 0 # On failure return 0, else return algorithm_id

    @srpc(_returns=Iterable(String))
    def get_algorithms():
        return [algo[0] for algo in list_algorithms()]

    @srpc(_returns=Iterable(String))
    def get_data_sets():
        return list_datasets()

    @srpc(String, String, Integer, String, _returns=ByteArray)
    def execute_algorithm(algorithm_name, data_set_name, num_nodes, command_line_args):
        algo = [algo for algo in list_algorithms() if algo[0] == algorithm_name]
        dataset = [ds for ds in list_datasets() if ds == data_set_name]

        if len(algo) == 0:
            logging.error("No Algorithm with name: "+algorithm_name)
        elif len(dataset) == 0:
            logging.error("No data set with name: "+data_set_name)
        else:
            input = base_dfs_dir+"snat_datasets/"+dataset[0]
            output = base_dfs_dir+"snat_output/"+algorithm_name+"-"+data_set_name+"-"+str(int(time()))
            cmd = [hadoop_dir+"bin/hadoop","jar",algo[0][1],algo[0][2],input,output]
            logging.info(cmd)
            ret = subprocess.call(cmd)
            cmd = [hadoop_dir+"bin/hadoop","dfs","-cat",output+"/part-r-00000"]
            ret2 = subprocess.check_output(cmd)
            return ByteArray.from_string(ret2)
       
        return ByteArray.from_string("fail!")
                   
    @srpc(String, _returns=String)
    def show_status(required_data):
        # TODO: pass proper data to required_data 
        cmd = ["curl","localhost:"+required_data]
        return subprocess.check_output(cmd)
    

if __name__=='__main__':
    logging.basicConfig(level=logging.DEBUG)
    logging.getLogger('rpclib.protocol.xml').setLevel(logging.DEBUG)

    logging.info("listening to http://127.0.0.1:7790")
    logging.info("wsdl is at: http://localhost:7790/?wsdl")

    wsgi_app = wsgi_soap_application([SnatService], 'warwick.snat.soap')
    server = make_server('127.0.0.1', 7790, wsgi_app)
    server.serve_forever()
