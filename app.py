from __future__ import division
from flask import Flask, request, make_response, jsonify, g
from collections import Counter
import requests as req
import re
import json
import sys
import os
import time
import math
import random

app = Flask(__name__)

# print(sys.version)
node_port = os.environ.get('PORT', 8080)
node_ip = os.environ.get('IP', '0.0.0.0')
node_pair = os.getenv('IPPORT')
other_nodes = os.getenv('VIEW')
K = int(os.getenv('K'))  # number of replicas per partition. Each partition owns a subset of keys
dictionary = {}  # contains values of keys
causalPayloads = {}  # contains causal payloads of keys
timeStamps = {}
vectorClock = []
my_partition_member_dict = {}
if other_nodes is not None:
    other_nodes = other_nodes.split(",")
    number_of_nodes = len(other_nodes)
    node_id = other_nodes.index(node_pair)  # returns index of node_pair in other_nodes
    my_partition_id = other_nodes.index(node_pair) // K  # do math to find partition id
    num_partitions = int(math.ceil(len(other_nodes) / K))
    for n in range(0, num_partitions):
        my_partition_member_dict[n] = []  # create empty lists for each partition id

    # form my_partition_member_dict
    for n in range(0, num_partitions):
        for ippor in other_nodes:
            if other_nodes.index(ippor) // K == n:
                my_partition_member_dict[n].append(ippor)

else:
    node_id = -1
    my_partition_id = -1
    num_partitions = -1


# return True if clock2 > clock1, both clocks must be lists
def compareClocks(clock1, clock2):
    return False  # our clocks suck hehe
    if len(clock1) != len(clock2):
        print("vector clocks are of different size, return False")
        return False
    else:
        oneGreater = False
        allLess = True
        for n in range(0, len(clock2) - 1):
            if clock1[n] < clock2[n]:
                oneGreater = True
        if clock1[n] > clock2[n]:
            allLess = False
        if allLess and oneGreater:
            return True
        else:
            return False


# hashing function
def hashit(key):
    global other_nodes
    asciiKey = ''.join(str(ord(c)) for c in key)  # converts key to concat of ascii values
    # #asciiKey = sum([ord(c) for c in key])
    return int(asciiKey) % num_partitions


def rehash():
    global node_id
    global dictionary
    global causalPayloads
    global timeStamps
    global other_nodes
    global timeStamps

    delKeys = {}
    print("rehash called")
    for key in dictionary:
        partition_location = hashit(key)
        if partition_location != my_partition_id:
            for node in my_partition_member_dict[partition_location]:
                url_str = 'http://' + node + '/backup/' + key
                print(key)
                print(dictionary[key])
                print("key not in right location, calling backup put on correct location")
                req.put(url_str, data={'val': dictionary[key], 'causal_payload': causalPayloads[key],
                                       'time': timeStamps[key]})  # store key in correct node
                delKeys[key] = dictionary[key]

    # remove keys that were shifted
    for key in delKeys:
        dictionary.pop(key)
        causalPayloads.pop(key)
        timeStamps.pop(key)
    # del tempDict[key]
    # print(tempDict)
    # dictionary = tempDict.copy()
    return


def remove_view_change(ip_port):
    global node_id
    global node_pair
    global other_nodes
    global dictionary
    global my_partition_member_dict
    global num_partitions
    global K
    global my_partition_id

    other_nodes.remove(ip_port)  # remove ip_port from VIEW
    num_partitions = int(math.ceil(len(other_nodes) / K))
    my_partition_id = other_nodes.index(node_pair) // K

    for n in range(0, num_partitions):
        my_partition_member_dict[n] = []

    for n in range(0, num_partitions):
        for ippor in other_nodes:
            if other_nodes.index(ippor) // K == n:
                my_partition_member_dict[n].append(ippor)

    try:
        node_id = other_nodes.index(node_pair)  # update node_id
    except ValueError:
        node_id = -1  # you are not in view

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

    rehash()


def add_view_change(ip_port):
    global node_id
    global node_pair
    global other_nodes
    global my_partition_member_dict
    global num_partitions
    global K
    global my_partition_id

    add_new_partition = True
    other_nodes.append(ip_port)
    node_id = other_nodes.index(node_pair)  # update node_id
    my_partition_id = other_nodes.index(node_pair) // K  # update my_partition_id

    num_partitions = int(math.ceil(len(other_nodes) / K))

    # update self
    # if other_nodes.index(ip_port)//K == my_partition_id:  #added node is in my partition
    # my_partition_member_dict[my_partition_id].append(ip_port) #add new node to member list if its this parition
    for n in range(0, num_partitions):
        my_partition_member_dict[n] = []

    for n in range(0, num_partitions):
        for ippor in other_nodes:
            if other_nodes.index(ippor) // K == n:
                my_partition_member_dict[n].append(ippor)

    rehash()
    # something to tell the new node it is the new partition
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


@app.route('/echo', methods=['GET'])
def echo():
    msg = request.args.get('msg')
    if msg is None:
        return ""
    else:
        return msg


@app.route('/update/<view>', methods=['PUT'])
def update(view):
    global node_id
    global node_pair
    global other_nodes
    global my_partition_member_dict
    global num_partitions
    global K
    global my_partition_id

    other_nodes = view
    other_nodes = other_nodes.split(",")
    my_partition_id = other_nodes.index(node_pair) // K
    num_partitions = int(math.ceil(len(other_nodes) / K))

    for n in range(0, num_partitions):
        my_partition_member_dict[n] = []

    for n in range(0, num_partitions):
        for ippor in other_nodes:
            if other_nodes.index(ippor) // K == n:
                my_partition_member_dict[n].append(ippor)

    print("updating to")
    print (other_nodes)
    try:
        node_id = other_nodes.index(node_pair)
    except ValueError:
        node_id = -1  # node is not in view
    print(node_id)
    rehash()
    response = jsonify(msg='success')
    return make_response(response, {"msg": "success"})


@app.route('/gossip/<key>', methods=['PUT', 'GET'])
def gossip(key):
    global node_pair

    if request.method == 'PUT':
        val = request.form['val']
        time = request.form['time']
        causal_payload = request.form['causal_payload']


    elif request.method == 'GET':
        for ipport in my_partition_member_dict[my_partition_id]:
            if ipport != node_pair:
                url = 'http://' + ipport + '/backup/' + key
                req.get(url, data={'ipport': node_pair})
                
    


@app.route('/backup/<key>', methods=['PUT', 'GET'])
def backup(key):
    global other_nodes
    global node_id
    global dictionary
    global node_pair
    global my_partition_id_list
    global my_partition_id
    global causalPayloads
    global timeStamps

    if request.method == 'PUT':
        val = request.form['val']
        causal_payload = request.form['causal_payload']
        newTime = request.form['time']
        dictionary[key] = val
        causalPayloads[key] = causal_payload
        timeStamps[key] = newTime
        response = jsonify(msg='success')
        return make_response(response, 200, {"msg": "success"})

    elif request.method == 'GET':
        sender_id = request.form['ipport']
        val = dictionary[key]
        newTime = timeStamps[key]
        url = 'http://' + sender_id + '/gossip/' + key
        r = req.put(url, data={'val': val, 'time': newTime, 'causal_payload': causalPayloads[key]})
        return r.content, r.status_code


@app.route('/kvs/<key>', methods=['PUT', 'GET'])
def kvs(key):
    global other_nodes
    global node_id
    global dictionary
    global node_pair
    global my_partition_member_dict
    global my_partition_id
    global causalPayloads
    global timeStamps
    global num_partitions

    # view_update
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
            response = jsonify(msg='success', partition_id=my_partition_id, number_of_partitions=num_partitions)
            return make_response(response, {"msg": "success"})

        if update_type == 'remove':
            remove_view_change(ipport)
            response = jsonify(msg='success', number_of_partitions=num_partitions)
            return make_response(response, {"msg": "success"})

    if not (re.match("^[A-Za-z0-9_]*$", key) and len(key) <= 250 and len(key) >= 1):  # key check
        status_code = 404
        response = jsonify(msg='error', error='key is not alphanumeric or not within 1-250 char limit')
        return make_response(response, status_code, {"msg": "error"})
    if request.method == 'GET':
        causal_payload = request.form['causal_payload']
        try:
            if key == 'get_partition_id':
                response = jsonify(msg='success', partition_id=my_partition_id)
                return make_response(response, 200, {"msg": "success"})
            if key == 'get_all_partition_ids':
                response = jsonify(msg='success', partition_id_list=my_partition_member_dict.keys())
                return make_response(response, 200, {"msg": "success"})
            if key == 'get_partition_members':
                partition_id_get = request.form['partition_id']
                response = jsonify(msg='success', partition_members=my_partition_member_dict[partition_id_get])
                return make_response(response, 200, {"msg": "success"})
            hashedKey = hashit(key)
            if hashedKey != my_partition_id:
                url = 'http://' + my_partition_member_dict[hashedKey][0] + '/kvs/' + key
                r = req.get(url, data={'causal_payload': causal_payload})
                return r.content, r.status_code
            else:
                if key in dictionary:
                    try:
                    	myVectorClockIndex = other_nodes.index(node_pair)
                        causalPayloads[key][myVectorClockIndex] += 1  # increment vector clock for receive (the line of bugs)
                    except IndexError:
                        pass
                    status_code = 200
                    response = jsonify(msg='success', value=dictionary[key], partition_id=my_partition_id,
                                       causal_payload=causalPayloads[key], timestamp=timeStamps[key])
                    return make_response(response, status_code, {"msg": "success"})
                else:
                  status_code = 404
                  response = jsonify(msg='success', value='1', partition_id=my_partition_id,
                                       causal_payload='', timestamp=time.time())
                  return make_response(response, status_code, {"msg": "success"})

        except(req.exceptions.ConnectionError, req.exceptions.Timeout):
            status_code = 404
            response = jsonify(msg='error', error='service  is not avalible')
            return make_response(response, status_code, {"msg": "error"})
    
    elif request.method == 'PUT':
        try:
            # val = request.args.get('val')
            val = request.form['val']
            causal_payload = request.form['causal_payload']
            hashedKey = hashit(key)  # store hashed key for performance
            if hashedKey != my_partition_id:
                url = 'http://' + my_partition_member_dict[hashedKey][0] + '/kvs/' + key
                try:
                    r = req.put(url, data={'val': val, 'causal_payload': causal_payload})
                except requests.exceptions.RequestException:  # timeout
                    url = 'http://' + my_partition_member_dict[hashedKey][1] + '/kvs/' + key
                    r = req.put(url, data={'val': val, 'causal_payload': causal_payload})
                return r.content, r.status_code
        
            else:  # key belongs to my partition
                causal_payload = causal_payload.split('.')  # parse string to list of strings
                if causal_payload != ['']:
                    causal_payload = list(map(int, causal_payload))  # convert to int
                if key in dictionary:  # we have prior info on this key
                    myVectorClockIndex = other_nodes.index(node_pair)
                    causalPayloads[key][myVectorClockIndex] += 1  # increment vector clock before compare
                    if compareClocks(causalPayloads[key], causal_payload):  # if passed in VC greater than stored VC
                        causalPayloads[key] = causal_payload
                        dictionary[key] = val
                        current_time = time.time()
                        timeStamps[key] = current_time
                    else:  # compareClocks is False, either imcomp VC or passed in VC not greater than stored VC
                        current_time = time.time()
                        if current_time > timeStamps[key]:  # passed in timeStamp greater than stored
                            causalPayloads[key] = causal_payload
                            dictionary[key] = val
                            timeStamps[key] = current_time
                        if current_time == timeStamps[key]:  # timeStamps are the same
                            print("Can't compare via VC or timeStamp, write lost")
                            pass  # idk what to do here, compare by node_id??? but cant?
                else:  # no prior info on this key
                    dictionary[key] = val  # set value to value passed in
                    current_time = time.time()
                    timeStamps[key] = current_time  # set time to current time
                    if causal_payload == ['']:  # causal payload passed in is empty
                        causalPayloads[key] = []  # init list for causalPayloads
                        for n in range(0, len(other_nodes)):  # init causal payload to 0's
                            causalPayloads[key].append(0)  # 0.0.0.0.0.0
                    else:
                        causalPayloads[key] = causal_payload  # set stored VC to passed in VC
                    myVectorClockIndex = other_nodes.index(node_pair)
                    causalPayloads[key][myVectorClockIndex] += 1  # increment vector clock
                    # send to all replicas
            causal_payload_string = ".".join(list(map(str, causalPayloads[key])))  # convert list to string
            for node_ipport in my_partition_member_dict[my_partition_id]:
                if node_ipport != node_pair:
                    url = 'http://' + node_ipport + '/backup/' + key
                    req.put(url, data={'val': val, 'causal_payload': causal_payload_string, 'time': current_time})
        
            # return msg
            status_code = 200
            response = jsonify(msg='success', partition_id=my_partition_id, causal_payload=causal_payload_string,
                               timestamp=timeStamps[key])
            return make_response(response, status_code, {"msg": "success"})
    
        except(req.exceptions.ConnectionError, req.exceptions.Timeout):
            status_code = 404
            response = jsonify(msg='error', error='service  is not avalible')
            return make_response(response, status_code, {"msg": "error"})

if __name__ == '__main__':
    app.config['MAX_CONTENT_LENGTH'] = 1572864  # bigger than 1.5 mb
    # app.run(debug=True, port=8080, host='0.0.0.0')
    app.run(debug=True, port=int(node_port), host=node_ip, threaded=True)
