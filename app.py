from __future__ import division
from flask import Flask, request, make_response, jsonify, g
import requests as req
import re
import json
import sys
import os
import time
import math

app = Flask(__name__)

# print(sys.version)
node_port = os.environ.get('PORT', 8080)
node_ip = os.environ.get('IP', '0.0.0.0')
node_pair = os.getenv('IPPORT')
other_nodes = os.getenv('VIEW')
K = os.getenv('K')  # number of replicas per partition. Each partition owns a subset of keys
# make other_nodes into a list
if other_nodes is not None:
    other_nodes = other_nodes.split(",")
    number_of_nodes = len(other_nodes)
    node_id = other_nodes.index(node_pair)  # returns index of node_pair in other_nodes
dictionary = {}  # contains values of keys
causalPayloads = {}  # contains causal payloads of keys
timeStamps = {}  # contains time stamps of keys
vectorClock = []  # node's vector clock
my_partition_id = node_id//K  # do math to find partition id
my_partition_id_list = {}  # dictionary so we'll now how many replicas each has/ why dict????
num_partitions = math.ceil(len(other_nodes)/K)
my_partition_member_list = []



def update_my_partition_member_list():
    global my_partition_member_list
    global my_partition_id
    global K
    global other_nodes

    my_partition_member_list = []  #clear member list
    for x in range(my_partition_id*K, my_partition_id*K + K):
        try:
            my_partition_member_list.append(other_nodes[x])
        except IndexError:
            pass  #do nothing, error doesn't effect function

def update_my_partition_id_list():
    global my_partition_id_list
    global num_partitions
    global other_nodes
    global K

    my_partition_id_list = []
    num_partitions = math.ceil(len(other_nodes) / K)  # update num_partitions just in case
    for x in range(0, num_partitions):
        my_partition_id_list.append(x)





#ignore this
'''
if num_partitions * K <= node_id:
    for n in range(num_partitions * K, len(other_nodes)):
        my_partition_member_list.append(other_nodes[n])

elif my_partition_id in range(0,(len(other_nodes)-remainder)//2):
    for n in range(0,(len(other_nodes)-remainder)//2):
        my_partition_member_list.append(other_nodes[n])

elif my_partition_id in range((len(other_nodes)-remainder)//2, len(other_nodes)-remainder):
    for n in range(0,(len(other_nodes)-remainder)//2):
        my_partition_member_list.append(other_nodes[n])
'''

#what does this even do? why do we do this?
#changed so we can get the # of replicas in each node without a request
for num in range(0, num_partitions):  #add partition ids to partition_id_list
    if remainder != 0:
        my_partition_id_list[num] = K
    else:
        my_partition_id_list[num] = remainder
        remainder = 0 #set remainder to 0 so the rest of the aprtitions will have the number of K

#form my_partition_member_list
for ippor in other_nodes:
    if ippor//K == my_partition_id:
        my_partition_member_list.append(ippor)


# hashing function
def hashit(key):
    #global other_nodes
    global num_partitions
    asciiKey = ''.join(str(ord(c)) for c in key)  # converts key to concat of ascii values
    # #asciiKey = sum([ord(c) for c in key])
    return int(asciiKey) % num_partitions
    #return int(asciiKey) % len(other_nodes)


# check if the given key belongs to this node
def node_check(key):
    k = hashit(key)
    # return k == node.ID
    return k is node_id


# get the node that has the key
def get_real_node(key):
    k = hashit(key)
    return other_nodes[k]


# not sure what this does/will be used for
def rehash():
    global node_id
    global dictionary
    global other_nodes

    delKeys = {}
    print("rehash called")
    for key in dictionary:
        node_location = hashit(key)
        if node_id != node_location:  # (key,value) not on right node
            url_str = 'http://' + other_nodes[node_location] + '/kvs/' + key
            print(key)
            print(dictionary[key])
            print("key not in right location, calling put on correct location")
            req.put(url_str, data={'val': dictionary[key]})  # store key in correct node
            delKeys[key] = dictionary[key]

    #remove keys that were shifted
    for key in delKeys:
        dictionary.pop(key)
    # del tempDict[key]
    # print(tempDict)
    # dictionary = tempDict.copy()
    return


def remove_view_change(ip_port):
    global node_id
    global node_pair
    global other_nodes
    global dictionary
    global my_partition_id_list
    global num_partitions
    global K
    global my_partition_member_list
    global my_partition_id
    other_nodes.remove(ip_port)  # remove ip_port from VIEW

    try:
        node_id = other_nodes.index(node_pair)  # update node_id
    except ValueError:
        node_id = -1  # you are not in view

    my_partition_id = other_nodes.index(node_pair)//K
    remove_partition = False

    if my_partition_id_list[other_nodes.index(ip_port)//K] == 1:
        remove_partition = True

    if other_nodes.index(ip_port)//K == my_partition_id:
        my_partition_member_list.remove(ip_port)

    if remove_partition :
        num_partitions -= 1
        my_partition_id_list.pop(ip_port)


    # send update view msg on other nodes
    viewString = ",".join(other_nodes)
    for node in other_nodes:
        print("for node")
        print(node)
        if node != node_pair:
            # create viewString

            print(viewString)
            print("sending update to ")
            print(node)
            url_str = 'http://' + node + '/update/' + viewString
            print(url_str)
            req.put(url_str)

    #update msg to remove node
    print("sending update to ")
    print(node)
    url_str = 'http://' + ip_port + '/update/' + viewString
    req.put(url_str)

    rehash()




def add_view_change(ip_port):
    global node_id
    global node_pair
    global other_nodes
    global my_partition_id_list
    global num_partitions
    global K
    global my_partition_member_list
    global my_partition_id

    add_new_partition = True

    other_nodes.append(ip_port)
    node_id = other_nodes.index(node_pair)  # update node_id
    my_partition_id = other_nodes.index(node_pair)//K
    #num_partitions = len(other_nodes)//K

    #update self
    if other_nodes.index(ip_port)//K == my_partition_id:
        my_partition_member_list.append(ip_port) #add new node to member list if its this parition

    for num in range(0, len(my_partition_id_list)):
        if my_partition_id_list[num] < K and num != my_partition_id:

            add_new_partition = False
            break

    if add_new_partition :
        num_partitions += 1
        my_partition_id_list[num_partitions] = 1

        #something to tell the new node it is the new partition

    print("add view change called")
    # send update view msg on other nodes
    # for node in other_nodes:
    for node in other_nodes:
        if node != node_pair:
            # create viewString
            viewString = ",".join(other_nodes)
            print(viewString)
            print("sending update to ")
            print(node)
            url_str = 'http://' + node + '/update/' + viewString
            print(url_str)
            req.put(url_str)
    rehash()


@app.route('/update/<view>', methods=['PUT'])
def update(view):
    global node_id
    global node_pair
    global other_nodes
    global my_partition_id_list
    global num_partitions
    global K
    global my_partition_member_list
    global my_partition_id

    other_nodes = view
    other_nodes = other_nodes.split(",")
    my_partition_id = other_nodes.index(node_pair)//K
    num_partitions = len(other_nodes)//K
    remainder = 0

    for num in range(0, num_partitions):  #add partition ids to partition_id_list
        if remainder != 0:
            my_partition_id_list[num] = K
        else:
            my_partition_id_list[num] = remainder
            remainder = 0

    print("updating to")
    print (other_nodes)
    try:
        node_id = other_nodes.index(node_pair)
    except ValueError:
        node_id = -1  #node is not in view
    print(node_id)
    rehash()
    response = jsonify(msg='success')
    return make_response(response, {"msg": "success"})


@app.route('/kvs/<key>', methods=['PUT', 'GET', 'DELETE'])
def kvs(key):
    global other_nodes
    global node_id
    global dictionary
    global node_pair
    global my_partition_id_list
    global my_partition_id
    global my_partition_member_list

    # view_update
    # I'm pretty sure im doing this wrong
    # might need to check if it is a put request
    if key == 'update_view':
        # get add or delete
        update_type = request.args.get('type')
        print (update_type)
        # ipport = request.args.get('ip_port')
        ipport = request.form['ip_port']
        print (ipport)
        if update_type == 'add':
            add_view_change(ipport)
            response = jsonify(msg='success')
            return make_response(response, {"msg": "success"})

        if update_type == 'delete':
            remove_view_change(ipport)
            response = jsonify(msg='success')
            return make_response(response, {"msg": "success"})

    if not (re.match("^[A-Za-z0-9_]*$", key) and len(key) <= 250 and len(key) >= 1):  # key check
        status_code = 404
        response = jsonify(msg='error', error='key is not alphanumeric or not within 1-250 char limit')
        return make_response(response, status_code, {"msg": "error"})

    if request.method == 'GET':
        try:
            if key == 'get_partition_id':
                my_partition_id = node_id // K  # update just in case, remove if we update somewhere else
                response = jsonify(msg='success', partition_id=my_partition_id)
                return make_response(response, 200, {"msg": "success"})
            if key == 'get_all_partition_ids':
                #update_my_partition_id_list()  # update just in case, remove if we update somewhere else
                # response = jsonify(msg='success', partition_id_list=my_partition_id_list)
                response = jsonify(msg='success', partition_id_list=my_partition_id_list.keys())
                return make_response(response, 200, {"msg": "success"})
            if key == 'get_partition_members':
                # generate list to return
                partition_id_get = request.form['partition_id']
                partition_members_list = []
                for member_num in range(partition_id_get * K, partition_id_get * K + K):
                    try:
                        partition_members_list.append(other_nodes[member_num])
                    except IndexError:
                        pass  # do nothing
                response = jsonify(msg='success', partition_members=partition_members_list)
                return make_response(response, 200, {"msg": "success"})

            if hashit(key) != node_id:
                url = 'http://' + other_nodes[hashit(key)] + '/kvs/' + key
                r = req.get(url)
                return r.content, r.status_code
            else:
                if key in dictionary:
                    status_code = 200
                    response = jsonify(msg='success', value=dictionary[key], partition_id=my_partition_id,
                                       causal_payload=causalPayloads[key], timestamp=timeStamps[key])
                    return make_response(response, status_code, {"msg": "success"})
                else:
                    status_code = 404
                    response = jsonify(msg='error', error='key does not exist', owner=node_pair)
                    return make_response(response, status_code, {"msg": "error"})

        except(req.exceptions.ConnectionError, req.exceptions.Timeout):
            status_code = 404
            response = jsonify(msg='error', error='service  is not avalible')
            return make_response(response, status_code, {"msg": "error"})

    elif request.method == 'PUT':
        try:
            # val = request.args.get('val')
            val = request.form['val']
            causal_payload = request.form['causal_payload']

            if hashit(key) != node_id:
                url = 'http://' + other_nodes[hashit(key)] + '/kvs/' + key
                # response = jsonify(msg='success', owner = other_nodes[hashit(key)])
                # return make_response(response,{"msg": "success"})
                r = req.put(url, data={'val': val})
                return r.content, r.status_code
            else:

                #infinite loop!
                #this might fix it?
                if dictionary[key] == val:
                    status_code = 200
                    response = jsonify(msg='success', partition_id=my_partition_id, causal_payload=causalPayloads[key], timestamp=timeStamps[key])
                    return make_response(response, status_code, {"msg": "success"})

                dictionary[key] = val
                causalPayloads[key] = causal_payload
                timeStamps[key] = time.time()
                for node_ipport in my_partition_member_list:
                    if node_ipport != node_pair:
                        url = 'http://' + node_ipport + '/kvs/' + key
                        req.put(url, data={'val': val , 'causal_payload' : causal_payload})

                status_code = 200
                response = jsonify(msg='success', partition_id=my_partition_id, causal_payload=causalPayloads[key], timestamp=timeStamps[key])
                return make_response(response, status_code, {"msg": "success"})

        except(req.exceptions.ConnectionError, req.exceptions.Timeout):
            status_code = 404
            response = jsonify(msg='error', error='service  is not avalible')
            return make_response(response, status_code, {"msg": "error"})


if __name__ == '__main__':
    app.config['MAX_CONTENT_LENGTH'] = 1572864  # bigger than 1.5 mb
    # app.run(debug=True, port=8080, host='0.0.0.0')
    app.run(debug=True, port=int(node_port), host=node_ip, threaded = True)
