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
        hostname = socket.gethostname()
        self.etcd_binary = '/usr/local/bin/etcd'
        self.connection_delay = 2
        self.connection_attempts = 5
        self.client_port = int(values['etcd']['client_port']['cont'])
        self.server_port = int(values['etcd']['server_port']['cont'])
        self.tls = values['etcd']['tls']['enabled']
        self.token = values['etcd']['token']
        self.namespace = values['namespace']
        self.cluster_domain = values['cluster_domain']
        self.api_version = 'v2'
        if self.tls:
            self.host_template = 'https://%s:%d'
            self.cert_file = '/opt/ccp/etc/tls/etcd_server_certificate.pem'
            self.key_file = '/opt/ccp/etc/tls/etcd_server_key.pem'
            self.ca_file = '/opt/ccp/etc/tls/ca.pem'
            self.verify_connectivity = self.ca_file
        else:
            self.host_template = 'http://%s:%d'
            self.verify_connectivity = False
        fqdn_template = "%s.%s.svc.%s"
        svc = fqdn_template % ('etcd', self.namespace, self.cluster_domain)
        # Represents fqdn service endoint for etcd
        self.service = self.host_template % (svc, self.client_port)
        members_endpoint = '%s/members/' % self.api_version
        # URL to query when accessing etcd members api
        self.members_api = urlparse.urljoin(self.service, members_endpoint)
        # When joining etcd cluster, members list is special:
        # <name>=<peerURL>,<name2>=<peerURL2>,...
        self.name = "%s.%s" % (hostname, svc)
        self.peer_url = self.host_template % (self.name, self.server_port)
        self.member_name = "%s=%s" % (self.name, self.peer_url)
        self.arguments = values.get('etcd').get('additional_arguments', None)


def start_etcd(config, bootstrap=False, initial_members=None):
    name = config.name
    client_port = config.client_port
    server_port = config.server_port
    client_host = config.host_template % (name, client_port)
    server_host = config.host_template % (name, server_port)
    if config.tls:
        # We add insecure listener for checks
        insecure_listener = ",http://%s:%s" % ('127.0.0.1', client_port)
    else:
        insecure_listener = ""
    args = ['--name=%s' % name,
            '--listen-peer-urls=%s' % server_host,
            '--listen-client-urls=%s' % client_host + insecure_listener,
            '--advertise-client-urls=%s' % client_host,
            '--initial-advertise-peer-urls=%s' % server_host,
            '--initial-cluster-token=%s' % config.token]
    if config.tls:
        args += ['--peer-auto-tls']
        args += ['--cert-file=%s' % config.cert_file]
        args += ['--key-file=%s' % config.key_file]
    if bootstrap:
        args += ["--initial-cluster=%s=%s" % (name, server_host)]
    if initial_members:
        args += ["--initial-cluster-state=existing",
                 "--initial-cluster=%s" % initial_members]
    if config.arguments:
        LOG.debug("Additional arguments are %s" % config.arguments)
        custom = ["--%s=%s" % (k,v) for k,v in config.arguments.iteritems()]
        args += custom
    cmd = [config.etcd_binary] + args
    LOG.info("Launching etcd with %s" % cmd)
    subprocess.check_call(cmd, shell=False)


@retry
def _add_etcd_member(members_api, peer_url):
    headers = {'content-type': 'application/json'}
    data = {'peerURLs': [peer_url]}
    verify = config.verify_connectivity
    r = requests.post(members_api, json=data, headers=headers, verify=verify)
    # https://coreos.com/etcd/docs/latest/v2/members_api.html
    if r.status_code == 201:
        return peer_url
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
    verify = config.verify_connectivity
    r = requests.delete(url, verify=verify)
    if r.status_code == 204:
        return [p for p in peers if p['name'] != name]
    else:
        LOG.debug("Delete failed with error %i", r.status_code)
        r.raise_for_status()


@retry
def _get_etcd_members(members_api):
    verify = config.verify_connectivity
    r = requests.get(members_api, verify=verify)
    if r.status_code == 200:
        peers = r.json()['members']
        return peers
    else:
        r.raise_for_status()


def _etcd_members_as_string(peers):
    # <name>=<peerURL>,<name2>=<peerURL2>,...
    l = []
    for m in peers:
        if m['name']:
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
    etcd_members_api = config.members_api
    try:
        # The only reliable way to determine if etcd cluster exists is to query
        # service.
        peers = _get_etcd_members(etcd_members_api)
        members = _etcd_members_as_string(peers)
    except ConnectionError:
        LOG.debug("No one seems to be alive...")
        members = ""
    if not members:
        # TODO(amnk): add recovery from complete disaster (e.g. restore data
        # from data-dir if it is available
        LOG.debug("I'm a leader, starting...")
        start_etcd(config, bootstrap=True)
    else:
        if config.name in members:
            # If we find our hostname in existing members, we are recovering
            # from some failure. Since we cannot guarantee having all needed
            # data on new node, we need to delete ourselve before joining.
            LOG.debug("Found myself in members...")
            new_peers = _delete_etcd_member(etcd_members_api, config.name)
            new_members = _etcd_members_as_string(new_peers)
        else:
            new_members = members
        LOG.debug("Adding myself to cluster %s..." % etcd_members_api)
        _add_etcd_member(etcd_members_api, config.peer_url)
        all_members = new_members + ',' + config.member_name
        LOG.debug("Joining %s" % members)
        start_etcd(config, initial_members=all_members)
