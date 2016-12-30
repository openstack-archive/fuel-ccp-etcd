#!/usr/bin/env python

import functools
import logging
import requests
import socket
import subprocess
import time
import urlparse

LOG_DATEFMT = "%Y-%m-%d %H:%M:%S"
LOG_FORMAT = "%(asctime)s.%(msecs)03d - %(levelname)s - %(message)s"
logging.basicConfig(format=LOG_FORMAT,
                    datefmt=LOG_DATEFMT,
                    level=logging.DEBUG)
LOG = logging.getLogger(__name__)

ELECTOR_URL = 'http://127.0.0.1:4040/'
CONNECTION_ATTEMPTS = 5
CONNECTION_DELAY = 2


def retry(f):
    @functools.wraps(f)
    def wrap(*args, **kwargs):
        attempts = CONNECTION_ATTEMPTS
        delay = CONNECTION_DELAY
        while attempts > 1:
            try:
                return f(*args, **kwargs)
            except requests.exceptions.RequestException:
                LOG.warning('Retrying in %d seconds...', delay)
                time.sleep(delay)
                attempts -= 1
        return f(*args, **kwargs)
    return wrap


@retry
def _get_leader_pod_name(url):
    r = requests.get(url)
    if r.status_code == 200:
        name = r.json()['name']
        return name
    else:
        r.raise_for_status()


def start_etcd(name, ipaddr, bootstrap=False, initial_members=None):
    # TODO(amnk): configuration should not be hardcoded
    cmd_t = ("/usr/local/bin/etcd"
             " --name {0}"
             " --listen-peer-urls=http://{1}:2380"
             " --listen-client-urls=http://{1}:2379"
             " --advertise-client-urls=http://{1}:2379")
    if bootstrap:
        boot_opts = (" --initial-cluster={0}=http://{1}:2380"
                     " --initial-advertise-peer-urls=http://{1}:2380")
        cmd_t += boot_opts

    if initial_members:
        cmd_t += " --initial-cluster-state existing"
        cmd_t += " --initial-cluster=%s" % initial_members
    cmd = cmd_t.format(name, ipaddr)
    LOG.info("Launching etcd with %s" % cmd)
    # TODO(amnk): we probably should call subprocess without shell
    proc = subprocess.call(cmd, shell=False)


@retry
def _add_etcd_member(member_api, ipaddr):
    headers = {'content-type': 'application/json'}
    url_template = 'http://%s:2380'
    peer = url_template % ipaddr
    data = {'peerURLs': [peer]}
    r = requests.post(member_api, json=data, headers=headers)
    # https://coreos.com/etcd/docs/latest/members_api.html
    if r.status_code == 201:
        return peer
    elif r.status_code == 500:
        # Request failed, but might be processed later, not sure how to handle
        LOG.debug('Etcd cluster returned 500, might be busy...')
    else:
        r.raise_for_status()


@retry
def _get_cluster_members(member_url):
    # When adding new node to existing etcd cluster, etcd requires certain
    # format, e.g.:
    # <name>=<peerURL>,<name2>=<peerURL2>,...
    r = requests.get(member_url)
    if r.status_code == 200:
        peers = r.json()['members']
        l = []
        for m in peers:
            if len(m['name']) > 0:
                l.append("%s=%s" % (m['name'], m['peerURLs'][0]))
        return ",".join(l)
    else:
        r.raise_for_status()


if __name__ == "__main__":
    leader = _get_leader_pod_name(ELECTOR_URL)
    hostname = socket.gethostname()
    ipaddr = socket.gethostbyname(hostname)
    if leader == ipaddr:
        start_etcd(hostname, ipaddr, bootstrap=True)
    else:
        etcd_leader = "http://%s:2379" % leader
        leader_endpoint = urlparse.urljoin(etcd_leader, 'v2/members')
        peer = _add_etcd_member(leader_endpoint, ipaddr)
        members = _get_cluster_members(leader_endpoint)
        all_members = members + (',%s=%s' % (hostname, peer))
        start_etcd(hostname, ipaddr, initial_members=all_members)
