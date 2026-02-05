import json
import sys
from argparse import ArgumentParser, ArgumentTypeError
from zlib import crc32

import mc_bin_client
import memcacheConstants

bucket_name = "default"
username = "Administrator"
password = "password"
kv_node_host = "localhost"
kv_node_port = 11210
kv_node_ssl = False
search_all_vbs = False

kv_nodes = []
vb_map = {}

def log(**obj):
    print(json.dumps(obj))

def disconnect():
    for client in kv_nodes:
        client.close()

def connect_client(host, port):
    print('connect_client', host, port)
    client = mc_bin_client.MemcachedClient(host, port, use_ssl=kv_node_ssl)
    client.req_features = {memcacheConstants.FEATURE_SELECT_BUCKET,
                           memcacheConstants.FEATURE_JSON,
                           memcacheConstants.FEATURE_COLLECTIONS}
    client.hello('find-doc')
    client.sasl_auth_plain(username, password)
    client.bucket_select(bucket_name)
    return client

def connect_cluster():
    client = connect_client(kv_node_host, kv_node_port)
    cluster_config = client.get_cluster_config()
    client.close()
    # print(json.dumps(cluster_config, indent=2))
    for server in cluster_config['vBucketServerMap']['serverList']:
        host = server.split(':')
        port = int(host[1])
        host = host[0]
        if host == '$HOST':
            host = kv_node_host
        if kv_node_ssl:
            port = kv_node_port
        client = connect_client(host, port)
        kv_nodes.append(client)
    for vbid, servers in enumerate(cluster_config['vBucketServerMap']['vBucketMap']):
        vb_map[vbid] = kv_nodes[servers[0]]
    assert len(vb_map) in [1024, 128, 64]

def get_vbid(doc_id):
    if isinstance(doc_id, str):
        doc_id = doc_id.encode()
    return ((crc32(doc_id) >> 16) & 0x7fff) % len(vb_map)

def get_doc(id: str, collection: str):
    docs = []
    vbs = range(len(vb_map)) if search_all_vbs else [get_vbid(id)]
    for vbid in vbs:
        client: mc_bin_client.MemcachedClient = vb_map[vbid]
        try:
            client.vbucketId = vbid
            flags, cas, doc = client.get(id, collection)
            docs.append((doc, cas, flags, vbid))
        except mc_bin_client.ErrorKeyEnoent:
            pass
    return docs

def add_doc(id: str, collection: str, value, flags, vbid=None):
    if vbid is None:
        vbid = get_vbid(id)
    client: mc_bin_client.MemcachedClient = vb_map[vbid]
    client.vbucketId = vbid
    client.add(id, exp=0, flags=flags, val=value, collection=collection)

def delete_doc(id: str, collection: str, cas, vbid=None):
    if vbid is None:
        vbid = get_vbid(id)
    client: mc_bin_client.MemcachedClient = vb_map[vbid]
    client.vbucketId = vbid
    client.delete(id, cas=cas, collection=collection)

def check_port(s):
    v = int(s)
    if v <= 0 or v >= 0x10000:
        raise ArgumentTypeError(f'{v} is not a valid port number')
    return v

def parse_args():
    parser = ArgumentParser(allow_abbrev=False)
    parser.add_argument('-b', '--bucket', default=bucket_name)
    parser.add_argument('-u', '--username', default=username)
    parser.add_argument('-p', '--password', default=password, help='"" for password prompt')
    parser.add_argument('--port', default=kv_node_port, type=check_port, help='KV node port (11210 or 11207 for TLS)')
    parser.add_argument('--host', default=kv_node_host, help='KV node hostname')
    parser.add_argument('--tls', default=kv_node_ssl, action='store_true')
    parser.add_argument('--restore', action='store_true', help='Copy found docs to the correct vbucket')
    parser.add_argument('--delete', action='store_true', help='Delete found docs')
    parser.add_argument('--id', metavar='DOC_ID', action='append', help='Single doc id (can be used multiple times)')
    parser.add_argument('--ids-file', metavar='FILE', dest='ids_file', help='JSON file with a list of doc ids')
    parser.add_argument('--collection', metavar='COLLECTION', default='_default._default', help='Scope/Collection')
    parser.add_argument('--value', action='store_true', help='Print doc value')
    parser.add_argument('--search-all-vbs', dest='search_all_vbs', action='store_true', help='Search all vbuckets')
    parser.add_argument('--add-to-vb', metavar='VB', dest='add_to_vb', type=int)
    return parser.parse_args()

def main():
    global bucket_name, username, password, kv_node_host, kv_node_port, kv_node_ssl, search_all_vbs
    options = parse_args()
    bucket_name = options.bucket
    username = options.username
    password = options.password
    kv_node_host = options.host
    kv_node_port = options.port
    kv_node_ssl = options.tls
    search_all_vbs = options.search_all_vbs
    if len(password) == 0:
        import getpass
        password = getpass.getpass()
    connect_cluster()
    print()
    if options.id:
        doc_ids = options.id
    elif options.ids_file is not None:
        with open(options.ids_file, 'r') as f:
            doc_ids = json.load(f)
        if not isinstance(doc_ids, list):
            print('Expected a list of doc ids in', options.ids_file)
            sys.exit(1)
    else:
        print('Either --id or --ids-file must be specified')
        sys.exit(1)
    if options.add_to_vb is not None:
        for id in doc_ids:
            try:
                add_doc(id, options.collection, b'{}', 0, options.add_to_vb)
                log(action='add', added=True, id=id, vb=options.add_to_vb)
            except mc_bin_client.ErrorKeyEexists:
                log(action='add', added=False, id=id, reason='exists')
        disconnect()
        return
    found_count = 0
    not_found_count = 0
    for id in doc_ids:
        assert isinstance(id, str)
        docs = get_doc(id, options.collection)
        if len(docs) == 0:
            log(action='get', found=False, id=id)
            not_found_count += 1
            continue
        found_count += len(docs)
        restored_one = False
        for (doc, cas, flags, vbid) in docs:
            if options.value:
                log(action='get', found=True, id=id, cas=cas, flags=flags, vb=vbid, value=doc)
            else:
                log(action='get', found=True, id=id, cas=cas, flags=flags, vb=vbid)
            if vbid == get_vbid(id):
                continue
            if options.restore and not restored_one:
                try:
                    add_doc(id, options.collection, doc, flags)
                    log(action='add', added=True, id=id)
                    restored_one = True
                except mc_bin_client.ErrorKeyEexists:
                    log(action='add', added=False, id=id, reason='exists')
            if options.delete:
                delete_doc(id, options.collection, cas, vbid)
                log(action='delete', id=id, vb=vbid)
    print('\n------------------------------------------')
    print('Found', found_count)
    print('Not found', not_found_count)
    disconnect()

if __name__ == '__main__':
    main()
