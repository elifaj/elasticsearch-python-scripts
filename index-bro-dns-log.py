from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk, scan

es  = Elasticsearch([{'host' : 'localhost', 'port' : 9200}])

def get_docs():
    for line in open('./dns.log').read().splitlines():
        split = line.split('\t')
        doc = {}
        doc['_index'] = 'maccdc2012'
        doc['_type'] = 'dns'
        doc['ts'] = split[0]
        doc['uid'] =  split[1]
        doc['id_orig_h'] = split[2]
        doc['id_orig_p'] = split[3]
        doc['id_resp_h'] = split[4]
        doc['id_resp_p'] = split[5]
        doc['proto'] = split[6]
        doc['trans_id'] = split[7]
        doc['query'] = split[8]
        doc['qclass'] = split[9]
        doc['qclass_name'] = split[10]
        doc['qtype'] = split[11]
        doc['qtype_name'] = split[12]
        doc['rcode'] = split[13]
        doc['rcode_name'] = split[14]
        doc['AA'] = split[15]
        doc['TC'] = split[16]
        doc['RD'] = split[17]
        doc['RA'] = split[18]
        doc['Z'] = split[19]
        doc['answers'] = split[20]
        doc['ttls'] = split[21]
        doc['rejected'] = split[22]
        yield doc

for a, b in streaming_bulk(es, get_docs()):
    print(a, b)
