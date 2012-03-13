from suds.client import Client
from rpclib.model.binary import ByteArray

snat_client = Client('http://localhost:7790/?wsdl')
result = snat_client.service.get_data_sets()
print result
result = snat_client.service.get_algorithms()
print result
# result = snat_client.service.execute_algorithm("Independent_Cascade","influence-graph",1,"")
result = snat_client.service.show_status("50070/dfshealth.jsp")
print result
file = open("/usr/local/hadoop/README.txt","r").read()

result = snat_client.service.upload_data_set("Readme", ByteArray.from_string("moo"))
