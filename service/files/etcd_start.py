#!/usr/bin/env python

import argparse
import functools
import logging
import requests
import socket
import subprocess
import time
import urlparse

from requests.exceptions import RequestException, ConnectionError
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
            except (RequestException, ConnectionError) as err:
                LOG.warning('Retrying in %d seconds because of %s', delay, err)
                time.sleep(delay)
                attempts -= 1
        return f(*args, **kwargs)
    return wrap


def _get_leader_pod_name(url):
    success = False
    while not success:
        try:
            r = requests.get(url)
            if r.status_code == 200:
                name = r.json()['name']
                success = True
        except ConnectionError:
            time.sleep(CONNECTION_DELAY)
            LOG.debug("Unable to contact leader at %s, sleeping" % url)
    return name


def start_etcd(name, ipaddr, client_port, server_port, token, bootstrap=False,
               initial_members=None):
    # TODO(amnk): configuration should not be hardcoded, and it should include
    # other options like datadir, snapshotting period, etc
    etcd_bin = ['/usr/local/bin/etcd']
    client_host = ETCD_HOST_TEMPLATE % (ipaddr, client_port)
    server_host = ETCD_HOST_TEMPLATE % (ipaddr, server_port)
    args = ['--name=%s' % name,
            '--listen-peer-urls=%s' % server_host,
            '--listen-client-urls=%s' % client_host,
            '--advertise-client-urls=%s' % client_host,
            '--initial-advertise-peer-urls=%s' % server_host,
            '--initial-cluster-token=%s' % token]
    if bootstrap:
        boot_opts = ["--initial-cluster=%s=%s" % (name, server_host)]
        args += boot_opts
    if initial_members:
        args += ["--initial-cluster-state=existing",
                 "--initial-cluster=%s" % initial_members]
    cmd = etcd_bin + args
    LOG.info("Launching etcd with %s" % cmd)
    proc = subprocess.call(cmd, shell=False)


@retry
def _add_etcd_member(etcd_api, ipaddr, server_port):
    headers = {'content-type': 'application/json'}
    url_template = ETCD_HOST_TEMPLATE
    peer = url_template % (ipaddr, server_port)
    data = {'peerURLs': [peer]}
    r = requests.post(etcd_api, json=data, headers=headers, verify=False)
    # https://coreos.com/etcd/docs/latest/members_api.html
    if r.status_code == 201:
        return peer
    elif r.status_code == 500:
        # Request failed, but might be processed later, not sure how to handle
        LOG.debug('Etcd cluster returned 500, might be busy...')
        r.raise_for_status()
    else:
        r.raise_for_status()


@retry
def _get_cluster_members(etcd_api):
    # When adding new node to existing etcd cluster, etcd requires certain
    # format, e.g.:
    # <name>=<peerURL>,<name2>=<peerURL2>,...
    r = requests.get(etcd_api, verify=False)
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
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--client-port', type=int, default=2379,
                        help='Etcd client port')
    parser.add_argument('-s', '--server-port', type=int, default=2380,
                        help='Etcd server port')
    parser.add_argument('-t', '--token', type=str, default='etcd-cluster',
                        help='Etcd cluster token')
    args = parser.parse_args()
    leader = _get_leader_pod_name(ELECTOR_URL)
    hostname = socket.gethostname()
    ipaddr = socket.gethostbyname(hostname)
    ETCD_HOST_TEMPLATE = 'http://%s:%d'
    if leader == ipaddr:
        # TODO(amnk): add recovery from complete disaster (e.g. restore data
        # from data-dir if it is available
        LOG.debug("I'm a leader, starting...")
        start_etcd(hostname, ipaddr, args.client_port, args.server_port,
                   args.token, bootstrap=True)
    else:
        etcd_leader = ETCD_HOST_TEMPLATE % (leader, args.client_port)
        leader_endpoint = urlparse.urljoin(etcd_leader, 'v2/members')
        peer = _add_etcd_member(leader_endpoint, ipaddr, args.server_port)
        members = _get_cluster_members(leader_endpoint)
        all_members = members + (',%s=%s' % (hostname, peer))
        LOG.debug("Leader is %s, joining cluster..." % etcd_leader)
        start_etcd(hostname, ipaddr, args.client_port, args.server_port,
                   args.token, initial_members=all_members)
