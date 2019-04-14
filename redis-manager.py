#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Author : Yohan Bataille (ING)
# redis-manager v2

# Requires:
# redis-cli
# python 2 (2.6 and 2.7 tested)

import sys
import os
import json
import time
import copy
from subprocess import PIPE, Popen
import argparse
import SocketServer
import socket
from BaseHTTPServer import BaseHTTPRequestHandler
import threading
import traceback
import pprint
import urllib2
from random import randint


# positional : REDIS_PATH, REDIS_NODES, ENV, HTTP_PORT (in this order, all mandatory, ENV : "DEV" or "PROD")
# optional : time (-t), dry_run (-n)

parser = argparse.ArgumentParser()
parser.add_argument("REDIS_PATH", help="Path to redis-cli binary. Example: '/opt/redis/bin/redis-cli'")
parser.add_argument("REDIS_NODES", help="Please specify a list of nodes as a single string, grouping nodes by datacenter and using '/' to separate datacenters if there is more than one. Example: '10.166.119.49:7000, 10.166.119.49:7001, 10.166.119.49:7002 / 10.166.119.48:7003, 10.166.119.48:7004, 10.166.119.48:7005'.")
parser.add_argument("ENV", choices=['DEV', 'PROD'], help="Please specify 'DEV' or 'PROD'.")
parser.add_argument("HTTP_PORT", help="Listen to request on this HTTP port.")
parser.add_argument("-m", "--other_managers", help="List of other managers as a single string. Example: 'slzuss3vmq00.sres.stech:8080, slzuss3vmq01.sres.stech:8080'.")
parser.add_argument("-t", "--time", help="Time to wait between checks, in seconds. Default: 3")
parser.add_argument("-w", "--failover_max_wait", help="How long to wait for Redis cluster to obey a failover command, in seconds. Default: 30")
parser.add_argument("-u", "--unresponsive_timeout", help="How long to wait before assuming the manager main loop is stuck, in seconds. Default: 30")
parser.add_argument("-n", "--dry_run", help="Do not send modifications commands to the cluster.", action="store_true")
args = parser.parse_args()

# if environment >= NEHOM ==> all_checks needs to be True
all_checks = True
if args.ENV == "DEV":
    all_checks = False
other_managers = list()
if args.other_managers:
    # Clean the string
    other_managers = "".join(args.other_managers.split())
    other_managers = other_managers.split(',')
loop_time = 3
if args.time:
    loop_time = int(args.time)
failover_max_wait = 30
if args.failover_max_wait:
    failover_max_wait = int(args.failover_max_wait)
unresponsive_timeout = 30
if args.unresponsive_timeout:
    unresponsive_timeout = int(args.unresponsive_timeout)
test = False
if args.dry_run:
    test = True
redis_cli = args.REDIS_PATH
http_port = int(args.HTTP_PORT)

print("HTTP_PORT: " + str(http_port))
print("REDIS_NODES: " + args.REDIS_NODES)
print("all_checks: " + str(all_checks))
print("loop_time: " + str(loop_time))
print("failover_max_wait: " + str(failover_max_wait))
print("other_managers: " + str(other_managers))
print("redis_cli: " + redis_cli)
print("test: " + str(test))

#[root@slzuss3vmq00 ~]# /opt/ZUTA0/Logiciel/RDI/bin/redis-cli -h 10.166.119.48 -p 7002 --raw CLUSTER INFO
#cluster_state:fail
#[root@slzuss3vmq00 ~]# /opt/ZUTA0/Logiciel/RDI/bin/redis-cli -h 10.166.119.48 -p 7002 --raw CLUSTER NODES
#bd6aef93f187bab16d12236cce2faf0ac40ad727 10.166.119.48:7004 master,fail? - 1496355476275 1496355472467 11 disconnected 5461-10922
#7d134a073e5be16f2d2a73e27cc63b062e74c91f 10.166.119.48:7005 master,fail? - 1496355476275 1496355475475 9 disconnected 10923-16383
#af5f4f889bf6ee6573ec57625216e7d4416e37ae 10.166.119.48:7001 slave bd6aef93f187bab16d12236cce2faf0ac40ad727 0 1496357907477 11 connected
#0585d0b297d3a15013472ab83187d5b0422a4f23 10.166.119.48:7002 myself,slave 7d134a073e5be16f2d2a73e27cc63b062e74c91f 0 0 8 connected
#bea1cec63063542b0cc003b58e198b68c7993545 10.166.119.48:7000 slave e44f67740aa2be4a568587d8ac7d4914614934fb 0 1496357910486 10 connected
#e44f67740aa2be4a568587d8ac7d4914614934fb 10.166.119.48:7003 master - 0 1496357909482 10 connected 0-5460
#[root@slzuss3vmq00 ~]# /opt/ZUTA0/Logiciel/RDI/bin/redis-cli -h 10.166.119.48 -p 7002 --raw INFO
## Replication
#role:slave
#master_host:10.166.119.48
#master_port:7005
#master_link_status:down
#[root@slzuss3vmq00 ~]# /opt/ZUTA0/Logiciel/RDI/bin/redis-cli -h 10.166.119.48 -p 7003 --raw INFO
## Replication
#role:master
#connected_slaves:1
#slave0:ip=10.166.119.48,port=7000,state=online,offset=3809,lag=1

def api_help():
    return """
### HTTP API description:
# /cluster_status: 'manager disabled'
#                  'Unknown state'
#                  'KO'
#                  'Unknown cluster'
#                  'At risk'
#                  'Imbalanced'
#                  'OK'

# /manager_status: 'active'
#                  'active asleep'
#                  'active asleep repartition disabled'
#                  'active repartition disabled'
#                  'passive'
#                  'passive asleep'
#                  'passive asleep repartition disabled'
#                  'passive repartition disabled'
#                  'failed'
#                  'starting'

# /help : This message

# /debug/enable

# /debug/disable

# /sleep?seconds=<seconds>

# /prepare_for_reboot/<IP>&duration=<seconds>: 
#                      'SIMILAR REQUEST ALREADY IN PROGRESS'
#                      'WAIT'
#                      'DONE'
#                      'CANNOT PROCEED: passive manager'
# example : /prepare_for_reboot/10.166.20.120&duration=300

"""

# TODO: Remove global variables.

def main():
    startup_nodes_list, startup_nodes_by_datacenter, startup_nodes_by_server = cluster_startup_topo()
    global failover_without_quorum_requested
    failover_without_quorum_requested = list()
    global failover_with_quorum_requested
    failover_with_quorum_requested = list()
    global plan
    plan = dict()
    global cluster_state
    global manager_status
    global request_active
    global sleep_until
    global no_repartition_until
    global no_repartition_duration
    global slave_only_engine
    global raw_topo
    global last_loop_epoch
    current_cluster_topo = list()

    if all_checks:
        print("PROD mode enabled: master imbalance correction by server and datacenter activated.")
    else:
        print("DEV mode enabled: master imbalance correction by server and datacenter deactivated.")

    while True:
        last_loop_epoch = time.mktime(time.gmtime())
        num_manager_active = 0
        sleep = False
        for manager in other_managers:
            try:
                r = urllib2.urlopen('http://' + manager + '/manager_status', None, 1)
                l = r.readlines()
                if len(l) == 1 and (l[0].split()[0] == 'passive' or l[0].split()[0] == 'starting'):
                    if request_active:
                        r2 = urllib2.urlopen('http://' + manager + '/request_active', None, 1)
                        l2 = r2.readlines()
                        if len(l2) == 1 and l2[0] == 'yes':
                            print("other has requested activation")
                            request_active = False
                            sleep = True
                elif len(l) == 1 and l[0].split()[0] == 'active':
                    if num_manager_active == 0:
                        num_manager_active += 1
                    else:
                        print("Too many active managers!")
                elif len(l) == 1 and l[0].split()[0] == 'failed':
                    print("manager " + manager + " is KO.")
                else:
                    print("manager " + manager + " has answered with garbage: " + str(l))
            except:
                print("manager " + manager + " is not responding.")
        if num_manager_active == 0 and (manager_status == 'passive' or manager_status == 'starting'):
            if request_active:
                print("Becoming active!")
                manager_status = 'active'
            elif sleep:
                print("Sleeping to let another manager activate.")
                time.sleep(randint(1, 10))
                continue
            else:
                request_active = True
                print("Manager election in progress.")
                time.sleep(randint(1, 10))
                continue
        elif num_manager_active == 1 and manager_status == 'starting':
            print("Manager election finished, we are passive!")
            manager_status = 'passive'
        elif num_manager_active > 1 and manager_status == 'active':
            print("Becoming passive!")
            manager_status = 'passive'

        if sleep_until != 0:
            delta = sleep_until - time.mktime(time.gmtime())
            if delta <= 0:
                sleep_until = 0
            else:
                print("Sleeping as requested. " + str(delta) + " seconds remaining.")
                time.sleep(loop_time)
                continue

        raw_topo, raw_topo_str = cluster_topo(startup_nodes_list, startup_nodes_by_datacenter)
        if raw_topo is None:
            cluster_state = 'Unknown state'
            print('Critical failure: cannot get cluster topology. Doing nothing.')
            time.sleep(loop_time)
            continue
        bool_cluster_online = cluster_online(startup_nodes_list)
        if bool_cluster_online:
            pass
        else:
            cluster_state = 'KO'
            print('Cluster failure.')
        # print(raw_topo_str)
        if not same_cluster(startup_nodes_list, raw_topo):
            pprint.pprint(raw_topo, width=300)
            cluster_state = 'Unknown cluster'
            print('Not the exact cluster we know. Doing nothing.')
            time.sleep(loop_time)
            continue
        for node in raw_topo:
            if node_status(node) == 'cluster still assessing':
                print('Something is going on but Redis Cluster is still assessing the situation. Doing nothing.')
                pprint.pprint(raw_topo, width=300)
                time.sleep(loop_time)
                continue
        # Loop over nodes, detect slaves with offline master and promote one slave (keep track of treated failed masters)
        if not bool_cluster_online:
            pprint.pprint(raw_topo, width=300)
            if has_quorum(raw_topo):
                print("Cluster has quorum and can recover by itself. Doing nothing.")
            else:
                failed_masters = list()
                new_failover_without_quorum_requested = list()
                for node in raw_topo:
                    if node_role(node) == 'slave' and node_status(node) == 'ok':
                        if master_status_of_slave(raw_topo, node) != 'ok':
                            masterid = masterid_of_slave(node)
                            if masterid in failed_masters:
                                print("Slave " + node_name(node) + " does not see master " + node_name_from_id(raw_topo, masterid) + ", but a slave has already been promoted. Doing nothing.")
                            elif manager_status == 'active':
                                if failover_without_quorum_did_not_happen(raw_topo)["one_did_not_happen"]:
                                    if not failover_without_quorum_did_not_happen(raw_topo)["one_cleared"]:
                                        print("Cluster did not comply with our previous failover request. Waiting.")
                                else:
                                    print("Failed master: " + node_name_from_id(raw_topo, masterid) + ". Promoting slave " + node_name(node) + ".")
                                    failover_without_quorum(node_ip(node), node_port(node))
                                    new_failover_without_quorum_requested.append({'slave': node_object_from_node_name(node_name(node)), 'epoch': time.mktime(time.gmtime())})
                                failed_masters.append(masterid)
                failover_without_quorum_requested = failover_without_quorum_requested + new_failover_without_quorum_requested
                if failed_masters == list():
                    print("Critical failure : no slave remaining, cannot do anything.")
        else:
            # Detect risky situations
            failure = False
            for node in raw_topo:
                if node_role(node) == 'master' and node_status(node) == 'ok':
                    # Detect master without slave
                    if not master_has_at_least_one_slave(raw_topo, node):
                        print("Master " + node_name(node) + " has no slave !")
                        failure = True
                elif node_role(node) == 'slave' and all_checks:
                    # Detect slave on same server as master.
                    if node_ip(node) == node_ip_from_id(raw_topo, masterid_of_slave(node)):
                        print("Slave " + node_name(node) + " is on the same server as master " + node_name_from_id(raw_topo, masterid_of_slave(node)))
                        failure = True
                    # Detect slave on same datacenter as master.
                    if node_datacenter(node) == node_datacenter_from_id(raw_topo, masterid_of_slave(node)):
                        print("Slave " + node_name(node) + " is on the same datacenter as master " + node_name_from_id(raw_topo, masterid_of_slave(node)))
                        failure = True
            if failure:
                cluster_state = 'At risk'
                time.sleep(loop_time)
                continue
            if current_cluster_topo == list():
                pprint.pprint(raw_topo, width=300)
                current_cluster_topo = raw_topo
            elif cluster_has_changed(current_cluster_topo, raw_topo):
                print("Cluster topology has changed")
                pprint.pprint(raw_topo, width=300)
                current_cluster_topo = raw_topo
            if plan != dict() and manager_status == 'active':
                #pprint.pprint(plan, width=300)
                steps = [int(key) for key in plan.keys()]
                steps.remove(0)
                current_step = min(steps)
                if not cluster_has_changed(current_cluster_topo, plan['0']['starting_topo']):
                    if failover_with_quorum_did_not_happen(raw_topo)["one_cleared"]:
                        print("Cluster did not comply with our previous failover request. We reached the timeout. Plan Failed. Forget it.")
                        plan = dict()
                    else:
                        print("Still waiting for the cluster to proceed with the failover.")
                elif cluster_has_changed(current_cluster_topo, plan[str(current_step)]['target_topo']):
                    print("Cluster topology is not what we would expect. Something happened. Plan failed. Forget it.")
                    plan = dict()
                else:
                    if len(steps) > 1:
                        print "Step " + str(current_step) + " succeeded."
                        del plan[str(current_step)]
                        print "Launching step " + str(current_step + 1) + "."
                        slave = plan[str(current_step + 1)]['slave']
                        master = plan[str(current_step + 1)]['master']
                        print("Slave " + slave + " will replace his master " + master)
                        node_object = node_object_from_node_name(slave)
                        failover_with_quorum(node_object['host'], node_object['port'])
                        failover_with_quorum_requested.append({'slave': node_object, 'epoch': time.mktime(time.gmtime())})
                    else:
                        print("Final step succeeded. The cluster is now balanced.")
                        print("I love it when a plan comes together!")
                        plan = dict()
                time.sleep(loop_time)
                continue
            if slave_only_engine is not None and manager_status == 'active':
                if failover_without_quorum_did_not_happen(raw_topo)["one_did_not_happen"] or failover_with_quorum_did_not_happen(raw_topo)["one_did_not_happen"]:
                    if not failover_without_quorum_did_not_happen(raw_topo)["one_cleared"] and not failover_with_quorum_did_not_happen(raw_topo)["one_cleared"]:
                        print("Cluster did not comply with our previous failover request. Waiting.")
                else:
                    # Failover all master nodes on this engine
                    for node in raw_topo:
                        if node_role(node) == 'master' and node_status(node) == 'ok' and node_ip(node) == slave_only_engine:
                            slave = get_one_slave(raw_topo, node)
                            if slave is None:
                                print("Master " + node_name(node) + " has no slave !")
                            else:
                                failover_with_quorum(node_ip(slave), node_port(slave))
                                failover_with_quorum_requested.append({'slave': node_object_from_node_name(node_name(slave)), 'epoch': time.mktime(time.gmtime())})
                # Engine has already only slaves, starting the clock.
                if not has_master(slave_only_engine, raw_topo) and no_repartition_until == 0:
                    no_repartition_until = time.mktime(time.gmtime()) + no_repartition_duration
                if no_repartition_until != 0:
                    delta = no_repartition_until - time.mktime(time.gmtime())
                    if delta <= 0:
                        # We reached the requested duration, resetting.
                        no_repartition_until = 0
                        slave_only_engine = None
                        no_repartition_duration = 0
                    else:
                        print("Skipping master imbalance correction as requested " + str(delta) + " seconds remaining.")
                        time.sleep(loop_time)
                        continue
                if slave_only_engine is not None:
                    print("Still trying to remove slaves from " + slave_only_engine)
                    time.sleep(loop_time)
                    continue

            # Loop over nodes, detect imbalanced master repartition and promote slaves accordingly
            imbalanced = False
            if not all_checks:
                pass
            elif len(startup_nodes_by_server) < 2:
                print("Only one server: skipping master imbalance correction.")
            else:
                server_master_repartition_dict = server_master_repartition(server_list(startup_nodes_by_server), raw_topo)
                datacenter_master_repartition_dict = datacenter_master_repartition(datacenter_count(startup_nodes_by_datacenter), raw_topo)

                # Detect too many masters on a server.
                name, master_count, master_total_count = detect_imbalance(server_master_repartition_dict)
                if name is not None:
                    cluster_state = 'Imbalanced'
                    imbalanced = True
                    print server_master_repartition_dict
                    #pprint.pprint(raw_topo, width=300)
                    print("Too many masters on server " + str(name) + ": " + str(master_count) + "/" + str(master_total_count))
                    if manager_status == 'active':
                        master, slave = find_failover_candidate(raw_topo, server_master_repartition_dict, datacenter_master_repartition_dict, startup_nodes_by_server, startup_nodes_by_datacenter)
                        if master is None or slave is None:
                            print("Could not find a failover solution.")
                        else:
                            if failover_without_quorum_did_not_happen(raw_topo)["one_did_not_happen"] or failover_with_quorum_did_not_happen(raw_topo)["one_did_not_happen"]:
                                if not failover_without_quorum_did_not_happen(raw_topo)["one_cleared"] and not failover_with_quorum_did_not_happen(raw_topo)["one_cleared"]:
                                    print("Cluster did not comply with our previous failover request. Waiting.")
                            else:
                                print("Slave " + slave + " will replace his master " + master)
                                node_object = node_object_from_node_name(slave)
                                failover_with_quorum(node_object['host'], node_object['port'])
                                failover_with_quorum_requested.append({'slave': node_object, 'epoch': time.mktime(time.gmtime())})
                    time.sleep(loop_time)
                    continue
                if len(startup_nodes_by_datacenter) < 2:
                    print("Only one datacenter: skipping master imbalance correction by datacenter.")
                else:
                    # Detect too many masters on a datacenter.
                    # It is possible to have no imbalance by server but an imbalance by datacenter (+1 master on each server of a datacenter compared to the other and at least 2 servers by datacenter).
                    name, master_count, master_total_count = detect_imbalance(datacenter_master_repartition_dict)
                    if name is not None:
                        cluster_state = 'Imbalanced'
                        imbalanced = True
                        print("Too many masters on datacenter " + str(name) + ": " + str(master_count) + "/" + str(master_total_count))
                        if manager_status == 'active':
                            master, slave = find_failover_candidate(raw_topo, server_master_repartition_dict, datacenter_master_repartition_dict, startup_nodes_by_server, startup_nodes_by_datacenter)
                            if master is None or slave is None:
                                print("Could not find a failover solution.")
                            else:
                                if failover_without_quorum_did_not_happen(raw_topo)["one_did_not_happen"] or failover_with_quorum_did_not_happen(raw_topo)["one_did_not_happen"]:
                                    if not failover_without_quorum_did_not_happen(raw_topo)["one_cleared"] and not failover_with_quorum_did_not_happen(raw_topo)["one_cleared"]:
                                        print("Cluster did not comply with our previous failover request. Waiting.")
                                else:
                                    print("Slave " + slave + " will replace his master " + master)
                                    node_object = node_object_from_node_name(slave)
                                    failover_with_quorum(node_object['host'], node_object['port'])
                                    failover_with_quorum_requested.append({'slave': node_object, 'epoch': time.mktime(time.gmtime())})
                        time.sleep(loop_time)
                        continue
            if not imbalanced:
                cluster_state = 'OK'
        time.sleep(loop_time)


def failover_without_quorum_did_not_happen(raw_topo):
    global failover_without_quorum_requested
    one_did_not_happen = False
    one_cleared = False
    for slave_dict in failover_without_quorum_requested:
        found_node_role = None
        for node in raw_topo:
            if node_name_from_node_object(slave_dict['slave']) == node_name(node):
                found_node_role = node_role(node)
                break
        if found_node_role == 'master':
            failover_without_quorum_requested.remove(slave_dict)
        elif time.mktime(time.gmtime()) - slave_dict['epoch'] > failover_max_wait:
            print("Cluster has not performed failover for slave " + node_name_from_node_object(slave_dict['slave']) + " requested " + str(failover_max_wait) + " seconds ago. Removing the failover request.")
            failover_without_quorum_requested.remove(slave_dict)
            one_cleared = True
        else:
            one_did_not_happen = True
    return {"one_did_not_happen": one_did_not_happen, "one_cleared": one_cleared}


def failover_with_quorum_did_not_happen(raw_topo):
    global failover_with_quorum_requested
    one_did_not_happen = False
    one_cleared = False
    for slave_dict in failover_with_quorum_requested:
        found_node_role = None
        for node in raw_topo:
            if node_name_from_node_object(slave_dict['slave']) == node_name(node):
                found_node_role = node_role(node)
                break
        if found_node_role == 'master':
            failover_with_quorum_requested.remove(slave_dict)
        elif time.mktime(time.gmtime()) - slave_dict['epoch'] > failover_max_wait:
            print("Cluster has not performed failover for slave " + node_name_from_node_object(slave_dict['slave']) + " requested " + str(failover_max_wait) + " seconds ago. Removing the failover request.")
            failover_with_quorum_requested.remove(slave_dict)
            one_cleared = True
        else:
            one_did_not_happen = True
    return {"one_did_not_happen": one_did_not_happen, "one_cleared": one_cleared}


def has_quorum(raw_topo):
    masters_ok_count = masters_ok_count_for_cluster(raw_topo)
    all_masters_count = masters_count_for_cluster(raw_topo)
    if masters_ok_count < all_masters_count / 2 + 1:
        return False
    return True


def detect_imbalance(master_repartition_dict):
    master_total_count = sum(master_repartition_dict.values())
    set_count = len(master_repartition_dict)
    for set_name, master_count in master_repartition_dict.iteritems():
        # sets can be datacenters or servers
        # If we have only 2 sets and an even number of masters, we at least try not to have more than half the masters on one set
        # If we have only 2 sets and an odd number of masters, we at least try not to have more than half the masters + 1 on one set
        if set_count == 2:
            if master_total_count % 2 == 0:
                if master_count > master_total_count / 2:
                    return set_name, master_count, master_total_count
            elif master_total_count % 2 != 0:
                if master_count > master_total_count / 2 + 1:
                    return set_name, master_count, master_total_count
        # If we have 3 sets and 4 masters, we will have 2 masters on one set
        elif set_count == 3:
            if master_count > master_total_count / 2:
                return set_name, master_count, master_total_count
        else:
            if master_total_count % 2 == 0:
                if master_count > master_total_count / 2 - 1:
                    return set_name, master_count, master_total_count
            elif master_total_count % 2 != 0:
                if master_count > master_total_count / 2:
                    return set_name, master_count, master_total_count
    return None, None, None


# Find the solution which minimizes the number of steps. The constraints are server_master_repartition and datacenter_master_repartition.
def find_failover_candidate(raw_topo, server_master_repartition_dict, datacenter_master_repartition_dict, startup_nodes_by_server, startup_nodes_by_datacenter):
    global plan
    plan = dict()
    max_steps = 3
    solution_steps_chain = {'0': raw_topo}
    master_slave_steps_chain = dict()
    raw_topo_permut_dict = copy.deepcopy(solution_steps_chain)
    for i in range(0, max_steps):
        if debug:
            print(i)
        j = 0
        raw_topo_1_permutations = dict()
        for position, raw_topo_permut in raw_topo_permut_dict.iteritems():
            if debug:
                print("start position: ")
                pprint.pprint(raw_topo_permut, width=300)
                server_master_repartition_dict = server_master_repartition(server_list(startup_nodes_by_server), raw_topo_permut)
                print server_master_repartition_dict
                datacenter_master_repartition_dict = datacenter_master_repartition(datacenter_count(startup_nodes_by_datacenter), raw_topo_permut)
                print datacenter_master_repartition_dict
            # This only returns masters and slaves with node_status(master) == 'ok' or 'cluster still assessing' and node_status(slave) == 'ok' or 'cluster still assessing':
            master_slaves_dict = master_slaves_topo(raw_topo_permut)
            # generate all 1-permutation sets
            for master in master_slaves_dict:
                if debug:
                    print("master: " + str(master))
                for slave in master_slaves_dict[master]:
                    raw_topo_copy = copy.deepcopy(raw_topo_permut)
                    raw_topo_1_permutation = simul_failover(master, slave, raw_topo_copy)
                    if debug:
                        print("slave: " + str(slave))
                        server_master_repartition_dict = server_master_repartition(server_list(startup_nodes_by_server), raw_topo_1_permutation)
                        print server_master_repartition_dict
                        datacenter_master_repartition_dict = datacenter_master_repartition(datacenter_count(startup_nodes_by_datacenter), raw_topo_1_permutation)
                        print datacenter_master_repartition_dict
                        pprint.pprint(raw_topo_1_permutation, width=300)
                    j += 1
                    if not raw_topo_1_permutation in solution_steps_chain.values():
                        #print "not already stored"
                        if solver_check(raw_topo_1_permutation, startup_nodes_by_server, startup_nodes_by_datacenter):
                            print("Found a solution: ")
                            pprint.pprint(raw_topo_1_permutation, width=300)
                            # return the first step
                            if i == 0:
                                print("Sounds like a plan !")
                                print "only one step : " + str([master, slave])
                                plan['0'] = {'starting_topo': copy.deepcopy(raw_topo)}
                                plan['1'] = {'master': master, 'slave': slave, 'target_topo': raw_topo_1_permutation}
                                return master, slave
                            else:
                                #print("first step position: " + position)
                                first = position.split('.')[1]
                                #print("first step: ")
                                #pprint.pprint(solution_steps_chain['0.'+first], width=300)
                                #print("master: "+master_slave_steps_chain['0.'+first][0])
                                #print("slave: "+master_slave_steps_chain['0.'+first][1])
                                step_key = '0'
                                step_number = 1
                                print("Sounds like a plan !")
                                end_position = position+'.'+str(j)
                                solution_steps_chain[end_position] = raw_topo_1_permutation
                                master_slave_steps_chain[end_position] = [master, slave]
                                plan['0'] = {'starting_topo': copy.deepcopy(raw_topo)}
                                for step in end_position.split('.')[1:]:
                                    step_key += '.'+step
                                    print "step "+str(step_number) + ": " + str(master_slave_steps_chain[step_key])
                                    plan[str(step_number)] = {'master': master_slave_steps_chain[step_key][0], 'slave': master_slave_steps_chain[step_key][1], 'target_topo': solution_steps_chain[step_key]}
                                    step_number += 1
                                return master_slave_steps_chain['0.'+first][0], master_slave_steps_chain['0.'+first][1]
                        else:
                            if debug:
                                print "============== store permutation ============="
                            solution_steps_chain[position+'.'+str(j)] = raw_topo_1_permutation
                            master_slave_steps_chain[position+'.'+str(j)] = [master, slave]
                            raw_topo_1_permutations[position+'.'+str(j)] = raw_topo_1_permutation
        raw_topo_permut_dict = copy.deepcopy(raw_topo_1_permutations)
    return None, None


def solver_check(raw_topo_1_permutation, startup_nodes_by_server, startup_nodes_by_datacenter):
    server_master_repartition_dict = server_master_repartition(server_list(startup_nodes_by_server), raw_topo_1_permutation)
    datacenter_master_repartition_dict = datacenter_master_repartition(datacenter_count(startup_nodes_by_datacenter), raw_topo_1_permutation)
    if debug:
        print "solver_check"
        pprint.pprint(raw_topo_1_permutation, width=300)
        print server_master_repartition_dict
        print datacenter_master_repartition_dict
    name_server, master_count_server, master_total_count = detect_imbalance(server_master_repartition_dict)
    name_datacenter, master_count_datacenter, master_total_count = detect_imbalance(datacenter_master_repartition_dict)
    if name_server is None and name_datacenter is None:
        return True
    return False


def simul_failover(master, slave, raw_topo):
    raw_topo_copy = copy.deepcopy(raw_topo)
    for node in raw_topo:
        if node_name(node) == master:
            switch_role(node)
        elif node_name(node) == slave:
            switch_role(node)
    if raw_topo_copy == raw_topo:
        print("Failed")
    #print("raw_topo_copy: " + str(raw_topo_copy))
    #print("raw_topo: " + str(raw_topo))
    return raw_topo


def master_slaves_topo(raw_topo):
    master_slaves_dict = dict()
    for master in raw_topo:
        if node_role(master) == 'master' and node_status(master) in ['ok', 'cluster still assessing']:
            master_slaves_dict[node_name(master)] = list()
            for slave in raw_topo:
                if node_id(master) == masterid_of_slave(slave):
                    if node_role(slave) == 'slave' and node_status(slave) in ['ok', 'cluster still assessing']:
                        master_slaves_dict[node_name(master)].append(node_name(slave))
    return master_slaves_dict


def has_master(engine, raw_topo):
    for node in raw_topo:
        if node_role(node) == 'master' and node_ip(node) == engine:
            return True
    return False


def cluster_online(startup_nodes_list):
    for startup_node in startup_nodes_list:
        proc = Popen(["timeout", "1", redis_cli, "-h", startup_node['host'], "-p", startup_node['port'], "--raw", "CLUSTER", "INFO"], stdout=PIPE)
        result = proc.communicate()[0].split()
        if isinstance(result, list) and len(result) > 0 and result[0] == 'cluster_state:ok':
            return True
    return False


def cluster_topo(startup_nodes_list, startup_nodes_by_datacenter):
    for startup_node in startup_nodes_list:
        proc = Popen(["timeout", "1", redis_cli, "-h", startup_node['host'], "-p", startup_node['port'], "--raw", "CLUSTER", "NODES"], stdout=PIPE)
        result_str = proc.communicate()[0]
        if not isinstance(result_str, str) or result_str == '':
            continue
        result = result_str.strip('\n').split('\n')
        result = [string.split(" ") for string in result]
        result_bak = copy.deepcopy(result)
        for node in result:
            if len(node) < 9:
                node.append('-')
        if isinstance(result, list) and len(result) > 0:
            result = append_datacenter(result, startup_nodes_by_datacenter)
            i = 0
            tmp = ''
            for line in result_str.strip('\n').split('\n'):
                tmp += line
                if len(result_bak[i]) < 9:
                    tmp += " -"
                tmp += " " + str(node_datacenter(result[i])) + '\n'
                i += 1
            result_str = tmp
            return result, result_str
    return None, None


def append_datacenter(raw_topo, startup_nodes_by_datacenter):
    for node in raw_topo:
        datacenter_index = get_datacenter_for_node(node, startup_nodes_by_datacenter)
        node.append(datacenter_index)
    return raw_topo


def same_cluster(startup_nodes_list, raw_topo):
    if len(startup_nodes_list) != len(raw_topo):
        print('Found a different number of nodes.')
        return False
    for node in raw_topo:
        if node_name(node) not in [node['host'] + ':' + node['port'] for node in startup_nodes_list]:
            print(node_name(node) + ' found but unknown.')
            return False
    return True


def cluster_has_changed(current_cluster_topo, raw_topo):
    if len(current_cluster_topo) != len(raw_topo):
        print('Found a different number of nodes.')
        return True
    for node in raw_topo:
        found = False
        for node2 in current_cluster_topo:
            if node_name(node) == node_name(node2):
                found = True
                if node_role(node) != node_role(node2):
                    return True
                break
        if not found:
            return True
    return False


def datacenter_master_repartition(int_datacenter_count, raw_topo):
    datacenter_master_repartition_dict = dict()
    for i in range(0, int_datacenter_count):
        datacenter_master_repartition_dict[str(i)] = master_count_for_datacenter(i, raw_topo)
    return datacenter_master_repartition_dict


def server_master_repartition(servers, raw_topo):
    server_master_repartition_dict = dict()
    for server in servers:
        server_master_repartition_dict[server] = master_count_for_server(server, raw_topo)
    return server_master_repartition_dict


def datacenter_count(startup_nodes_by_datacenter):
    return len(startup_nodes_by_datacenter)


def server_count(startup_nodes_by_server):
    return len(startup_nodes_by_server)


def server_list(startup_nodes_by_server):
    return startup_nodes_by_server.keys()


def master_count_for_datacenter(datacenter_index, raw_topo):
    count = 0
    for node in raw_topo:
        if node_role(node) == 'master' and node_status(node) in ['ok', 'cluster still assessing'] and node_datacenter(node) == datacenter_index:
            count += 1
    return count


def master_count_for_server(server, raw_topo):
    count = 0
    for node in raw_topo:
        if node_role(node) == 'master' and node_status(node) in ['ok', 'cluster still assessing'] and node_ip(node) == server:
            count += 1
    return count


def masters_ok_count_for_cluster(raw_topo):
    count = 0
    for node in raw_topo:
        if node_role(node) == 'master' and node_status(node) == 'ok':
            count += 1
    return count


def masters_count_for_cluster(raw_topo):
    count = 0
    seen = list()
    for node in raw_topo:
        if node_role(node) == 'master' and node_name(node) not in seen:
            count += 1
            seen.append(node_name(node))
    return count


def node_datacenter(node):
    return node[9]


def node_role(node):
    if 'slave' in node[2]:
        return 'slave'
    return 'master'


def switch_role(node):
    if 'slave' in node[2]:
        node[2] = node[2].replace('slave', 'master')
    else:
        node[2] = node[2].replace('master', 'slave')


def master_status_of_slave(raw_topo, slave):
    return node_status_from_id(raw_topo, masterid_of_slave(slave))


def masterid_of_slave(slave):
    return slave[3]


def node_id(node):
    return node[0]


def node_name(node):
    return node[1]


def node_status(node):
    if node[2] in ['myself,slave', 'myself,master', 'slave', 'master'] and node[7] == 'connected':
        return 'ok'
    elif node[2] in ['slave,fail?', 'master,fail?']:
        return 'cluster still assessing'
    return 'failed'


def node_status_from_id(raw_topo, nodeid):
    for node in raw_topo:
        if nodeid == node_id(node):
            return node_status(node)


def node_name_from_id(raw_topo, nodeid):
    for node in raw_topo:
        if nodeid == node_id(node):
            return node_name(node)


def node_ip_from_id(raw_topo, nodeid):
    for node in raw_topo:
        if nodeid == node_id(node):
            return node_ip(node)


def node_datacenter_from_id(raw_topo, nodeid):
    for node in raw_topo:
        if nodeid == node_id(node):
            return node_datacenter(node)


def node_object_from_node_name(node_name):
    ip = node_name.split(':')[0]
    port = node_name.split(':')[1]
    return {"host": ip, "port": port}


def node_name_from_node_object(node_object):
    return node_object["host"]+":"+ node_object["port"]


def cluster_startup_topo():
    startup_nodes = args.REDIS_NODES

    # Clean the string
    startup_nodes = "".join(startup_nodes.split())

    startup_nodes = startup_nodes.split('/')
    startup_nodes_list = list()
    # TODO: startup_nodes_by_datacenter should be a dict
    startup_nodes_by_datacenter = list()
    startup_nodes_by_server = dict()
    for datacenter in startup_nodes:
        tmp = datacenter.split(',')
        for node in tmp:
            node_dict = node_object_from_node_name(node)
            ip = node_dict['host']
            port = node_dict['port']
            startup_nodes_list.append(node_dict)
            if ip in startup_nodes_by_server.keys():
                startup_nodes_by_server[ip].append(port)
            else:
                startup_nodes_by_server[ip] = [port]
        startup_nodes_by_datacenter.append(tmp)
    #print(startup_nodes_by_server)
    #print(startup_nodes_by_datacenter)
    #print(startup_nodes_list)
    return startup_nodes_list, startup_nodes_by_datacenter, startup_nodes_by_server


def get_datacenter_for_node(node, startup_nodes_by_datacenter):
    i = 0
    for datacenter in startup_nodes_by_datacenter:
        if node_name(node) in datacenter:
            return i
        i += 1
    return None


# ip, port of the slave that will replace his master
def failover_without_quorum(ip, port):
    print(redis_cli + " -h " + ip + " -p " + port + " --raw CLUSTER FAILOVER TAKEOVER")
    if not test:
        proc = Popen(["timeout", "1", redis_cli, "-h", ip, "-p", port, "--raw", "CLUSTER", "FAILOVER", "TAKEOVER"], stdout=PIPE)
        result = proc.communicate()[0].split()


# ip, port of the slave that will replace his master
def failover_with_quorum(ip, port):
    print(redis_cli + " -h " + ip + " -p " + port + " --raw CLUSTER FAILOVER")
    if not test:
        proc = Popen(["timeout", "1", redis_cli, "-h", ip, "-p", port, "--raw", "CLUSTER", "FAILOVER"], stdout=PIPE)
        result = proc.communicate()[0].split()


def node_ip(node):
    return node_object_from_node_name(node_name(node))['host']


def node_port(node):
    return node_object_from_node_name(node_name(node))['port']


def master_has_at_least_one_slave(raw_topo, master):
    for node in raw_topo:
        if node_id(master) == masterid_of_slave(node):
            return True
    return False


def get_one_slave(raw_topo, master):
    for node in raw_topo:
        if node_id(master) == masterid_of_slave(node):
            return node
    return None


class MyHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        global sleep_until
        global slave_only_engine
        global no_repartition_duration
        global cluster_state
        global request_active
        global debug
        if self.path == '/debug/enable':
            self.send_response(200)
            debug = True
        elif self.path == '/debug/disable':
            self.send_response(200)
            debug = False
        elif self.path == '/help':
            self.send_response(200)
            self.send_header('Content-type', 'text/plain')
            self.end_headers()
            self.wfile.write(api_help())
        elif self.path == '/cluster_status':
            self.send_response(200)
            self.send_header('Content-type', 'text/plain')
            self.end_headers()
            self.wfile.write(cluster_state)
        elif self.path == '/manager_status':
            self.send_response(200)
            self.send_header('Content-type', 'text/plain')
            self.end_headers()
            delta = time.mktime(time.gmtime()) - last_loop_epoch
            if delta > unresponsive_timeout:
                print("manager main loop is unresponsive!")
                answer = "failed"
                cluster_state = 'Unknown state'
                request_active = False
            else:
                answer = manager_status
            if sleep_until != 0:
                answer += " asleep"
            if no_repartition_until != 0:
                answer += " repartition disabled"
            self.wfile.write(answer)
        elif self.path == '/request_active':
            self.send_response(200)
            self.send_header('Content-type', 'text/plain')
            self.end_headers()
            if request_active:
                self.wfile.write("yes")
            else:
                self.wfile.write("no")
        elif "/sleep" in self.path:
            try:
                sleep_duration = int(self.path.split("=")[1])
                sleep_until = time.mktime(time.gmtime()) + sleep_duration
                self.send_response(200)
                self.send_header('Content-type', 'text/plain')
                self.end_headers()
                self.wfile.write("OK")
                cluster_state = 'manager disabled'
            except:
                self.send_response(400)
        elif "/prepare_for_reboot" in self.path:
            bad_request = False
            try:
                no_repartition_duration_req = int(self.path.split("duration=")[1])
                slave_only_engine_req = self.path.split("/")[2].split("&")[0]
                if not slave_only_engine_req in [node_ip(node) for node in raw_topo]:
                    self.send_response(400)
                    bad_request = True
            except:
                self.send_response(400)
                bad_request = True
            if not bad_request:
                if manager_status == 'passive':
                    self.send_response(200)
                    self.send_header('Content-type', 'text/plain')
                    self.end_headers()
                    self.wfile.write("CANNOT PROCEED: passive manager")
                elif manager_status == 'starting':
                    self.send_response(200)
                    self.send_header('Content-type', 'text/plain')
                    self.end_headers()
                    self.wfile.write("CANNOT PROCEED: manager is starting")
                elif time.mktime(time.gmtime()) - last_loop_epoch > 10:
                    self.send_response(500)
                    self.send_header('Content-type', 'text/plain')
                    self.end_headers()
                    self.wfile.write("CANNOT PROCEED: failed manager")
                elif no_repartition_duration != 0 and slave_only_engine != slave_only_engine_req:
                    print("A request has already been made to only have slaves on engine " + slave_only_engine + ".")
                    self.send_response(403)
                    self.send_header('Content-type', 'text/plain')
                    self.end_headers()
                    self.wfile.write("SIMILAR REQUEST ALREADY IN PROGRESS")
                elif no_repartition_duration != 0 and slave_only_engine == slave_only_engine_req and not has_master(slave_only_engine, raw_topo):
                    self.send_response(200)
                    self.send_header('Content-type', 'text/plain')
                    self.end_headers()
                    self.wfile.write("DONE")
                else:
                    slave_only_engine = slave_only_engine_req
                    no_repartition_duration = no_repartition_duration_req
                    self.send_response(200)
                    self.send_header('Content-type', 'text/plain')
                    self.end_headers()
                    self.wfile.write("WAIT")
        else:
            self.send_response(404)
    def log_message(self, format, *args):
        if debug:
            sys.stderr.write("%s - - [%s] %s\n" %
                 (self.address_string(),
                  self.log_date_time_string(),
                  format%args))


class WebThread(threading.Thread):
    def run(self):
        httpd.serve_forever()


if __name__ == "__main__":
    global cluster_state
    cluster_state = 'manager disabled'
    global manager_status
    manager_status = 'starting'
    global request_active
    request_active = False
    global sleep_until
    sleep_until = 0
    global no_repartition_until
    no_repartition_until = 0
    global no_repartition_duration
    no_repartition_duration = 0
    global slave_only_engine
    slave_only_engine = None
    global raw_topo
    raw_topo = None
    global last_loop_epoch
    global debug
    debug = False

    httpd = SocketServer.TCPServer(("", http_port), MyHandler, bind_and_activate=False)
    httpd.allow_reuse_address = True
    httpd.server_bind()
    httpd.server_activate()
    webserver_thread = WebThread()
    webserver_thread.start()
    
    print(api_help())
    try:
        main()
    except KeyboardInterrupt:
        print 'Interrupted'
        httpd.shutdown()
        httpd.server_close()
        sys.exit(0)
    except:
        print 'We crashed!'
        print traceback.format_exc()
        httpd.shutdown()
        httpd.server_close()
        sys.exit(0)

# vim: set ts=4 sw=4 sts=4 et :
