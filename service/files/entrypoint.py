#!/usr/bin/env python

import functools
import json
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

GLOBALS_PATH = '/etc/ccp/globals/globals.json'


def retry(f):
    @functools.wraps(f)
    def wrap(*args, **kwargs):
        attempts = config.connection_attempts
        delay = config.connection_delay
        while attempts > 1:
            try:
                return f(*args, **kwargs)
            except (RequestException, ConnectionError) as err:
                LOG.warning('Retrying in %d seconds because of %s', delay, err)
                time.sleep(delay)
                attempts -= 1
        return f(*args, **kwargs)
    return wrap


class Configuration():
    def __init__(self, config_file):
        LOG.info("Getting global variables from %s", config_file)
        values = {}
        with open(config_file) as f:
            global_conf = json.load(f)
        for key in ['etcd', 'namespace',  'security', 'cluster_domain']:
            values[key] = global_conf[key]
        self.etcd_binary = '/usr/local/bin/etcd'
        self.connection_delay = 2
        self.connection_attempts = 5
        self.client_port = int(values['etcd']['client_port']['cont'])
        self.server_port = int(values['etcd']['server_port']['cont'])
        self.tls = values['etcd']['tls']['enabled']
        self.election_timeout = int(values['etcd']['election_timeout'])
        self.heartbeat_interval = int(values['etcd']['heartbeat_interval'])
        self.token = values['etcd']['token']
        self.namespace = values['namespace']
        self.cluster_domain = values['cluster_domain']
        self.api_version = 'v2'
        self.cert_file = '/opt/ccp/etc/tls/etcd_server_certificate.pem'
        self.key_file = '/opt/ccp/etc/tls/etcd_server_key.pem'
        self.ca_file = '/opt/ccp/etc/tls/ca.pem'
        if self.tls:
            self.host_template = 'https://%s:%d'
        else:
            self.host_template = 'http://%s:%d'
        svc_template = "etcd.%s.svc.%s" % (self.namespace, self.cluster_domain)
        self.service = self.host_template % (svc_template, self.client_port)
        members_template = '%s/members/' % self.api_version
        self.members_api = urlparse.urljoin(self.service, members_template)


def start_etcd(name, ipaddr, config, bootstrap=False, initial_members=None):
    client_port = config.client_port
    server_port = config.server_port
    client_host = config.host_template % (ipaddr, client_port)
    server_host = config.host_template % (ipaddr, server_port)
    if config.tls:
        insecure_listener = ",http://%s:%s" % ('127.0.0.1', client_port)
    else:
        insecure_listener = ""
    args = ['--name=%s' % name,
            '--listen-peer-urls=%s' % server_host,
            '--listen-client-urls=%s' % client_host + insecure_listener,
            '--advertise-client-urls=%s' % client_host,
            '--initial-advertise-peer-urls=%s' % server_host,
            '--initial-cluster-token=%s' % config.token,
            '--heartbeat-interval=%s' % config.heartbeat_interval,
            '--election-timeout=%s' % config.election_timeout]
    if config.tls:
        args += ['--peer-auto-tls']
        args += ['--cert-file=%s' % config.cert_file]
        args += ['--key-file=%s' % config.key_file]
    if bootstrap:
        args += ["--initial-cluster=%s=%s" % (name, server_host)]
    if initial_members:
        args += ["--initial-cluster-state=existing",
                 "--initial-cluster=%s" % initial_members]
    cmd = [config.etcd_binary] + args
    LOG.info("Launching etcd with %s" % cmd)
    subprocess.check_call(cmd, shell=False)


@retry
def _add_etcd_member(members_api, peer_name):
    headers = {'content-type': 'application/json'}
    data = {'peerURLs': [peer_name]}
    r = requests.post(members_api, json=data, headers=headers, verify=False)
    # https://coreos.com/etcd/docs/latest/v2/members_api.html
    if r.status_code == 201:
        return peer_name
    elif r.status_code == 500:
        # Request failed, but might be processed later, not sure how to handle
        LOG.debug('Etcd cluster returned 500, might be busy...')
        r.raise_for_status()
    else:
        r.raise_for_status()


@retry
def _delete_etcd_member(members_api, name):
    # HTTP API needs id of the member to delete it
    # So first we get member id, then we delete it - 2 calls total.
    peers = _get_etcd_members(members_api)
    _id = _get_etcd_member_id(peers, name)
    LOG.debug("Deleting %s with id %s from etcd cluster..." % (name, _id))
    url = urlparse.urljoin(members_api, _id)
    r = requests.delete(url, verify=False)
    if r.status_code == 204:
        return [p for p in peers if p['name'] != name]
    else:
        LOG.debug("Delete failed with error %i", r.status_code)
        r.raise_for_status()


@retry
def _get_etcd_members(members_api):
    r = requests.get(members_api, verify=False)
    if r.status_code == 200:
        peers = r.json()['members']
        return peers
    else:
        r.raise_for_status()


def _etcd_members_as_string(peers):
    # <name>=<peerURL>,<name2>=<peerURL2>,...
    l = []
    for m in peers:
        if len(m['name']) > 0:
            l.append("%s=%s" % (m['name'], m['peerURLs'][0]))
    return ",".join(l)


def _get_etcd_member_id(peers, name):
    # Get member id from peers list
    members = [p['id'] for p in peers if p['name'] == name]
    if members:
        return members[0]
    else:
        return None


if __name__ == "__main__":
    config = Configuration(GLOBALS_PATH)
    hostname = socket.gethostname()
    ipaddr = socket.gethostbyname(hostname)
    etcd_members_api = config.members_api
    try:
        # The only reliable way to determine if etcd cluster exists is to query
        # service.
        peers = _get_etcd_members(etcd_members_api)
        members = _etcd_members_as_string(peers)
    except ConnectionError:
        LOG.debug("Noone seems to be alive, member list is empty")
        members = ""
    if not members:
        # TODO(amnk): add recovery from complete disaster (e.g. restore data
        # from data-dir if it is available
        LOG.debug("I'm a leader, starting...")
        start_etcd(hostname, ipaddr, config, bootstrap=True)
    else:
        peer_name = config.host_template % (ipaddr, config.server_port)
        member_name = ',%s=%s' % (hostname, peer_name)
        if hostname in members:
            # If we find our hostname in existing members, we are recovering
            # from some failure. Since we cannot guarantee having all needed
            # data on new node, we need to delete ourselve before joining.
            LOG.debug("Found myself in members...")
            new_peers = _delete_etcd_member(etcd_members_api, hostname)
            new_members = _etcd_members_as_string(new_peers)
        else:
            new_members = members
        LOG.debug("Adding myself to cluster %s..." % etcd_members_api)
        _add_etcd_member(etcd_members_api, peer_name)
        all_members = new_members + member_name
        LOG.debug("Joining %s" % members)
        start_etcd(hostname, ipaddr, config, initial_members=all_members)
