#!/usr/bin/env python

import argparse
import os
import sys

import cm_api
from cm_api.api_client import ApiResource

parser = argparse.ArgumentParser(description='Restart a CM cluster service')

parser.add_argument('-m', '--cm_host', required=True,
                    help="hostname / fqdn of the CM UI ('cloudera.domain.com')")
parser.add_argument('-u', '--username', default=os.environ['USER'],
                    help="username for the CM API authentication ('johndoe')")
parser.add_argument('-p', '--password', required=True,
                    help="password for the CM API authentication ('Pa55w0rd')")
parser.add_argument('-l', '--cluster', required=False,
                    help="cluster to restart service in ('mycluster')")
parser.add_argument('-s', '--service', required=False,
                    help="service to restart ('hdfs')")

args = parser.parse_args()
print args

# https://cloudera.github.io/cm_api/docs/quick-start/
api = ApiResource(args.cm_host, username=args.username, password=args.password)

def select_from_items(item_type, items):
    print("\n".join(["Choose a %s" % item_type] + [i.lower() for i in items]))
    item = raw_input().lower()
    print "Input received: %s" % item
    if item in items:
        return item
    else:
        print 'Bad input received...'
        return select_from_items(item_type, items)

# Get the cluster
clusters = api.get_all_clusters()
cluster_names = [c.name.lower() for c in clusters]
cluster_name = ''
if args.cluster is not None:
    cluster_name = args.cluster.lower()
else:
    cluster_name = select_from_items('cluster', cluster_names)

for cluster in clusters:
    if cluster.name.lower() == cluster_name:

        # Get the service
        services = cluster.get_all_services()
        service_names = [s.name.lower() for s in services]
        service_name = ''
        if args.service is not None:
            service_name = args.service.lower()
        else:
            service_name = select_from_items('service', service_names)
        for service in services:
            if service.name.lower() == service_name:

                # Restart the service
                print "Attempting to restart '%s' in '%s'" % (service_name, cluster_name)
                # command = service.restart().wait()
                # print command
                sys.exit()
