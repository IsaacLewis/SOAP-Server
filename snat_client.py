from suds.client import Client
from rpclib.model.binary import ByteArray
import base64

snat_client = Client('http://localhost:7792/?wsdl')
result = snat_client.service.get_data_sets()
print result
result = snat_client.service.get_algorithms()
print result
# result = snat_client.service.execute_algorithm("Independent_Cascade","influence-graph",1,"")
result = snat_client.service.show_status("50070/dfshealth.jsp")
print result

file = open("/usr/local/hadoop/wordcount.jar","r").read()
str = base64.b64encode(file)
result = snat_client.service.upload_algorithm("Independent_Cascade2","me.saac.i.IndyCascade", str)
