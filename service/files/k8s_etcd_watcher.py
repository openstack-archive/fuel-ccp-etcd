#!/usr/bin/env python

import argparse
import etcd
import json
import logging
import os
import requests
import socket
import urlparse

LOG_DATEFMT = "%Y-%m-%d %H:%M:%S"
LOG_FORMAT = "%(asctime)s.%(msecs)03d - %(levelname)s - %(message)s"
logging.basicConfig(format=LOG_FORMAT,
                    datefmt=LOG_DATEFMT,
                    level=logging.DEBUG)
LOG = logging.getLogger(__name__)

# List of k8s reasons that should trigger node deletion
REASONS = ("NodeControllerEviction", "Killing")


def _get_http_header(TOKEN_FILE):
    try:
        token = file(TOKEN_FILE, 'r').read()
    except IOError:
        exit('Unable to open token file')
    header = {'Authorization': "Bearer {}".format(token)}
    return header


def _get_etcd_client(etcd_service=None, etcd_port=None):
    srv = etcd_service or socket.gethostname()
    port = etcd_port or 2379
    c = etcd.client.Client(port=port, host=srv)
    return c


def _get_etcd_members(client):
    try:
        members = client.members
        return members
    except etcd.EtcdException as err:
        LOG.debug(err)
        return {}


def _get_etcd_member_id(client, name):
    members = _get_etcd_members(client)
    members = [k for k, v in members.iteritems() if v['name'] == name]
    if members:
        return members[0]
    else:
        return None


def _delete_etcd_member(client, name):
    # Since we have to do 2 calls (one to get id and another one to
    # delete etcd member, there might be a race from other watchers.
    # So we check twice...
    _id = _get_etcd_member_id(client, name)
    if _id:
        LOG.debug("Id of %s is %s", name, _id)
        # Waiting for https://github.com/jplana/python-etcd/pull/219
        #try:
        #    client.api_execute(client.version_prefix + '/members/%s' % _id,
        #                       client._MDELETE)
        #    return True
        #except Exception as e:
        #    LOG.debug("Delete failed with error %i", e)
        #    return False
        url = urlparse.urljoin(client.base_uri, '/v2/members/%s' % _id)
        r = requests.delete(url, verify=False)
        if r.status_code == 204:
            return True
        else:
            LOG.debug("Delete failed with error %i", r.status_code)
    else:
        LOG.debug("Node %s was not found in the cluster", name)
    return False


def _get_kubernetes_stream():
    TOKEN_FILE = "/var/run/secrets/kubernetes.io/serviceaccount/token"
    # CA_CERT = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
    K8S_HOST = os.environ["KUBERNETES_SERVICE_HOST"]
    K8S_PORT = os.environ["KUBERNETES_PORT_443_TCP_PORT"]
    API_URL = "https://" + K8S_HOST + ":" + K8S_PORT + "/api/v1/events"
    http_header = _get_http_header(TOKEN_FILE)
    # verify does not work with k8s 1.5
    LOG.debug("Listening for events from: %s", API_URL)
    response = requests.get(API_URL, headers=http_header,
                            verify=False, params={'watch': 'true'},
                            stream=True)
    return response


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-n', '--namespace', type=str, default='default',
                        help='Namespace to filter events')
    parser.add_argument('--reasons', nargs='+', type=str, default=REASONS,
                        help='Custom list of reasons to filter')
    args = parser.parse_args()
    while True:
        stream = _get_kubernetes_stream()
        for line in stream.iter_lines():
            event = json.loads(line)
            obj = event["object"]["involvedObject"]
            name = obj["name"]
            reason = event["object"]["reason"]
            if (obj["kind"] == "Pod") and (obj["namespace"] == args.namespace):
                LOG.info("Detected event: %s for pod: %s", reason, name)
                if reason in args.reasons:
                    etcd_c = _get_etcd_client()
                    deleted = _delete_etcd_member(etcd_c, name)
                    if not deleted:
                        LOG.info("Delete of %s from etcd failed", name)
                    else:
                        LOG.info("Node %s was deleted from etcd", name)

