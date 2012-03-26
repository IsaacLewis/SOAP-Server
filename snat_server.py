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
import base64

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
    myfile = open(hadoop_dir+"algorithms/algorithms.txt","r")
    algos = [line.split() for line in myfile.read().split("\n") if line != ""]
    return algos

def list_datasets():
    str = subprocess.check_output([hadoop_dir+"bin/hadoop","dfs","-ls",base_dfs_dir+"snat_datasets/"])
    datasets = [re.search(base_dfs_dir+'snat_datasets/([A-z0-9_-]+)',line).group(1) for line in str.split("\n")[1:-1]]
    return datasets

class SnatService(ServiceBase):

    @srpc(String, String, _returns=String)
    def upload_data_set(data_set_name, data_set):
        tmp_file_name = "/tmp/snat_upload."+data_set_name+".txt"

        # check if dataset of that name already exists
        dataset = [ds for ds in list_datasets() if ds == data_set_name]
        if len(dataset) != 0:
            return "fail! Dataset of that name already exists"

        # delete existing temp file, if it exists
        cmd = ["rm",tmp_file_name]
        subprocess.call(cmd)        
        myfile = open(tmp_file_name,"w")
        myfile.write(base64.b64decode(data_set))
        # bin/hadoop dfs -copyFromLocal /tmp/snat_upload.Readme2.txt /user/hduser/snat_datasets/RM
        myfile.close()
        
        cmd = [hadoop_dir+"bin/hadoop",
               "dfs",
               "-copyFromLocal",
               tmp_file_name,
               base_dfs_dir+"snat_datasets/"+data_set_name]

        return 0

    @srpc(String, String, String, _returns=String)
    def upload_algorithm(algorithm_name, class_name, jar_file):
        algo = [algo for algo in list_algorithms() if algo[0] == algorithm_name]
        if len(algo) != 0:
            return "Algorithm with that name exists!"

        jar_file_name = hadoop_dir + "algorithms/" + algorithm_name + ".jar"
        
        # delete existing jar file, if it exists
        cmd = ["rm",jar_file_name]
        subprocess.call(cmd)

        myfile = open(jar_file_name,"w")
        myfile.write(base64.b64decode(jar_file))
        myfile.close()

        myfile = open(hadoop_dir+"algorithms/algorithms.txt","a+")
        myfile.write("\n"+algorithm_name+" "+algorithm_name+".jar "+class_name)
        myfile.close()
        return "Success!"

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
            cmd = [hadoop_dir+"bin/hadoop","jar","algorithms/"+algo[0][1],algo[0][2],input,output]
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

    logging.info("listening to http://127.0.0.1:7792")
    logging.info("wsdl is at: http://localhost:7792/?wsdl")

    wsgi_app = wsgi_soap_application([SnatService], 'warwick.snat.soap')
    server = make_server('127.0.0.1', 7792, wsgi_app)
    server.serve_forever()
