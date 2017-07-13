#!/usr/bin/env python

import argparse
import os
import sys
import smtplib
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
parser.add_argument('-e', '--smtp_server', required=False,
                    help="smtp server to send e-mail notifications")

args = parser.parse_args()
print args

# https://cloudera.github.io/cm_api/docs/quick-start/
api = ApiResource(args.cm_host, username=args.username, password=args.password)

def select_from_items(item_type, items):
    print("\n".join(["\nChoose a %s" % item_type] + [i.lower() for i in items]))
    item = raw_input().lower()
    print "\nInput received: %s " % item
    if item in items:
        return item
    else:
        print 'Bad input received...\n\n'
        return select_from_items(item_type, items)


# Rolling restart method (using few defaults - Role types, role names. It will take all)
def rolling_restart(service_rr):
    print service_rr
    try:
        #if service_rr.name.lower() in ['hive', 'hue', 'oozie', 'spark']:
        #    raise AttributeError("Rolling Restart is not available for '%s' " % service.name.lower())
        batch_size = raw_input("Number of worker roles to restart together in a batch \n")
        fail_count_threshold = raw_input("Stop rolling restart if more than this number of worker batches fail \n")

        options = ["true", "false"]    # options for stale config
        stale_config_only = select_from_items('option to Restart roles on nodes with stale config?(Default is false) \n', options)   # Needs to be a user selection

        rr_command = service_rr.rolling_restart(batch_size, fail_count_threshold, None, stale_config_only, None, None, None).wait()
    except:
        print "WARNING: Rolling Restart is not available for '%s'. Exiting... " % service.name.lower()
        print "Executing Restart on service '%s' " % service.name.lower()
        rr_command = service.restart().wait()
    return rr_command

# Get SMTP server and email sender/receiver info
if args.smtp_server is not None:
    server = args.smtp_server.lower()
else:
    server = raw_input("Please enter a valid SMTP server to send out email updates : ")

print "\nPlease enter email details below to send out report"
from_email = raw_input("From: ")
to_email = raw_input("Send to: ")


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

        # Get the action to be performed on predefined actions
        defined_actions = ['Service restart', 'Deploy Client Configuration', 'Rolling Restart']

        for service in services:
            if service.name.lower() == service_name:

                action = select_from_items('action', [i.lower() for i in defined_actions])

                if action == defined_actions[0].lower():  # Restart the service
                    print "Executing Restart on service '%s'... \n" % service.name.lower()
                    command = service.restart().wait()

                elif action == defined_actions[1].lower():    # Deploy client config
                    try:
                        print "Deploying Client Configurations on service '%s'... \n" % service.name.lower()
                        command = service.deploy_client_config().wait()
                    except:
                        # Since DCC unavailable in oozie, hue, zookeeper
                        print "WARNING: Deploying Client configuration is not available for '%s'. Exiting...  \n" % service.name.lower()
                        sys.exit()

                elif action == defined_actions[2].lower():
                    print "Executing Rolling Restart on service '%s'... \n" % service.name.lower()
                    command = rolling_restart(service)

        job_succeeded = command.success

        message = "Subject: '%s' automation update \n\n" % action

        message += "\nCluster : %s" % cluster_name
        message += "\nService : %s" % service_name
        message += "\nAction Performed: %s" % action

        if job_succeeded:
            print "%s is successful. Please check your inbox for details" % action
            message += "\n\n\n %s is Successful" % action
        else:
            print "%s failed. Please check your inbox for details" % action
            message += "\n\n\n%s Failed" % action

        # Logic to send mail updates on the job status
        try:
            server = smtplib.SMTP(server, 25)    # SMTP server at port 25

            # from_email = 'anoop.mamgain@cerner.com'
            server.sendmail(from_email, to_email, message)    # Send mail
            server.quit()
        except:
            print "Invalid email information. Please check SMTP server info or mailing address"

        sys.exit()

