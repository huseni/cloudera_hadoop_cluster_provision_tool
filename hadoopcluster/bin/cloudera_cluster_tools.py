#!/usr/bin/env python

import os
import ConfigParser
import json
import pickle
import json
from cm_api.api_client import ApiResource
from cm_api.endpoints.types import ApiClusterTemplate
from cm_api.endpoints.cms import ClouderaManager
from cm_api.endpoints.services import ApiService
from cm_api.endpoints.services import ApiServiceSetupInfo


class DeployCloudEraCluster(object):
    """
    This class to define and setup the base properties of the cluster node for hadoop echo system
    """
    _cloudera_manager_host = None
    _port_number = None
    _user_name = None
    _password = None
    _version = 12

    def __init__(self, cloudera_manager_host, port_number, user_name, password, version):
        """
        Initialize the object to provision the cluster node for the hadoop parcel based provision
        :param cloudera_manager_host:
        :param port_number:
        :param user_name:
        :param password:
        :param version:
        """
        self._cloudera_manager_host = cloudera_manager_host
        self._port_number = port_number
        self._user_name = user_name
        self._password = password
        self._version = version # API version vary depending upon the job you want to perform. "1" if you want to check the cluster and 12 if you want to export the property of config
        self._cloudera_manager_oconnect = ApiResource(self._cloudera_manager_host, self._port_number, self._user_name,
                                                    self._password, version=self._version)

    def get_cluster_versions(self):
        """
        To get all the provisioned cluster versions against the Cloud era manager
        :return:
        """
        for cluster in self._cloudera_manager_oconnect:
            print("%s = %s" % (cluster.name, cluster.version))
        return cluster

    def get_cluster_services(self, cdh_version):
        """
        To get all the provisioned cluster services against the specific cluster
        :return:
        """
        for srv in cdh_version.get_all_services():
            print srv
            if srv.type == "HDFS":
                hdfs = srv
                print hdfs.name, hdfs.serviceState, hdfs.healthSummary
                print hdfs.serviceUrl
                for chk in hdfs.healthChecks:
                    print "%s --- %s" % (chk['name'], chk['summary'])

    def get_cluster_roles_info(self, cdh_version):
        """
        To get the details of all the roles for each cluster node
        :return:
        """
        for role in cdh_version.get_all_roles():
            if role.type == 'NAMENODE':
                namenode = role
        print "Role name: %s\nState: %s\nHealth: %s\nHost: %s" % (namenode.name, namenode.roleState, namenode.healthSummary, namenode.hostRef.hostId)

    def get_cdh_metrics_details(self, cdh_version):
        """
        To get the CDH metrics containing details about all the activities in the cluster node
        :param cdh_version:
        :return:
        """
        metrics = cdh_version.get_metrics()
        for metric in metrics:
            print "%s (%s)" % (metric.name, metric.unit)

    def start_service(self, cdh_service_name):
        """
        To start or stop the CDH service
        :param cdh_service_name:
        :return:
        """
        service = cdh_service_name.restart()
        print service.active

        service_status = service.wait()
        print "Active: %s. Success: %s" % (service_status.active, service_status.success)

    def restart_service(self, cdh_service_name, namenode):
        """
        To restart the service of the specific role
        :param cdh_service_name:
        :param namenode:
        :return:
        """
        commands = cdh_service_name.restart_roles(namenode.name)
        for command in commands:
            print command

    def configure_services(self, cdh_service_name):
        """
        To configure the specific services with available roles
        :return:
        """
        for name, config in cdh_service_name.get_config(view='full')[0].items():
            print "%s - %s - %s" % (name, config.relatedName, config.description)

    def export_cluster_template(self, template_filename, cluster_name):
        """
        To export the current cluster configuration into the given file.
        :param template_filename:
        :return:
        """
        cluster = self._cloudera_manager_oconnect.get_cluster(cluster_name)
        cdh_template = cluster.export()
        with open(template_filename, 'w') as outfile:
            json.dump(cdh_template.to_json_dict(), outfile, indent=4, sort_keys=True)

    def import_cluster_template(self, template_filename, cluster_name):
        """
        To import cluster template configuration into given cluster
        :param template_filename:
        :param cluster_name:
        :return:
        """
        cluster = self._cloudera_manager_oconnect.get_cluster(cluster_name)
        with open(template_filename) as data_file:
            data = json.load(data_file)
        template = ApiClusterTemplate(cluster).from_json_dict(data, cluster)
        cms = ClouderaManager(cluster)
        command = cms.import_cluster_template(template)
        print (command)

    def deploy_cloudera_manager_services(self):
        """
        To deploy the cloudera manager services
        :return:
        """
        varEnableConfigAlerts = True
        varServiceGroupName = "cloudera-scm"
        varServiceUserName = "cloudera-scm"
        varMgmtServiceConfig = {
            'enable_config_alerts': varEnableConfigAlerts,
            'process_groupname': varServiceGroupName,
            'process_username': varServiceUserName,
        }
        varManager = self._cloudera_manager_oconnect.get_cloudera_manager()
        varMgmt = varManager.create_mgmt_service(ApiServiceSetupInfo())

        # update the cloudera service config
        varMgmt.update_config(varMgmtServiceConfig)

        # Get the cloudera services configured
        services = varManager.get_service()

        varMgmt.create_role("ACTIVITYMONITOR-1", "ACTIVITYMONITOR", self._cloudera_manager_host)
        varMgmt.create_role("ALERTPUBLISHER-1", "ALERTPUBLISHER", self._cloudera_manager_host)
        varMgmt.create_role("EVENTSERVER-1", "EVENTSERVER", self._cloudera_manager_host)
        varMgmt.create_role("HOSTMONITOR-1", "HOSTMONITOR", self._cloudera_manager_host)
        varMgmt.create_role("SERVICEMONITOR-1", "SERVICEMONITOR", self._cloudera_manager_host)
        varMgmt.create_role("REPORTSMANAGER-1", "REPORTSMANAGER", self._cloudera_manager_host)

    def deploy_activity_monitor(self):
        """
        To deploy the Activity monitor services
        :return:
        """
        varActivityMonitorPassword = "DgJ8aA5kqg"

        varMgmt = self._cloudera_manager_oconnect.get_service()

        # config for the activity monitoring
        varActivityMonitorConfig = {
            'firehose_database_host': "pocd-cm581-dev-manager.poc-d.internal" + ":" + "7432",
            'firehose_database_user': "amon",
            'firehose_database_password': varActivityMonitorPassword,
            'firehose_database_type': "postgresql",
            'firehose_database_name': "amon",
            'firehose_heapsize': 268435456,
            'mgmt_log_dir': "/opt/cloudera/log/cloudera-scm-firehose",
            'oom_heap_dump_dir': "/tmp",
            'oom_heap_dump_enabled': False,
            'max_log_backup_index': 10,
            'max_log_size': 100,
            'log_threshold': "INFO",
            'enable_config_alerts': "true",
        }
        varRole = varMgmt.get_role("ACTIVITYMONITOR-1")
        varRole.update_config(varActivityMonitorConfig)

    def deploy_alert_publisher(self):
        """
        To deploy the alert publisher
        :return:
        """
        varMgmt = self._cloudera_manager_oconnect.get_service()
        varAlertPublisherConfig = {
            'alert_heapsize': 268435456,
            'mgmt_log_dir': "/opt/cloudera/log/cloudera-scm-alertpublisher",
            'oom_heap_dump_dir': "/tmp",
            'oom_heap_dump_enabled': False,
            'max_log_backup_index': 10,
            'max_log_size': 100,
            'log_threshold': "INFO",
            'enable_config_alerts': True,
        }
        varRole = varMgmt.get_role("ALERTPUBLISHER-1")
        varRole.update_config(varAlertPublisherConfig)

    def deploy_event_server(self):
        """
        To deploy event server
        :return:
        """
        varMgmt = self._cloudera_manager_oconnect.get_service()
        varEventServerConfig = {
            'event_server_heapsize': 268435456,
            'mgmt_log_dir': "/opt/cloudera/log/cloudera-scm-eventserver",
            'eventserver_index_dir': "/opt/cloudera/lib/cloudera-scm-eventserver",
            'oom_heap_dump_dir': "/tmp",
            'oom_heap_dump_enabled': False,
            'max_log_backup_index': 10,
            'max_log_size': 100,
            'log_threshold': "INFO",
            'enable_config_alerts': True,
        }
        varRole = varMgmt.get_role("EVENTSERVER-1")
        varRole.update_config(varEventServerConfig)

    def deploy_host_monitor(self):
        """
        To deploy host monitor
        :return:
        """
        varMgmt = self._cloudera_manager_oconnect.get_service()
        varHostMonitorConfig = {
            'firehose_heapsize': 268435456,
            'mgmt_log_dir': "/opt/cloudera/log/cloudera-scm-firehose",
            'firehose_storage_dir': "/opt/cloudera/lib/cloudera-host-monitor",
            'oom_heap_dump_dir': "/tmp",
            'oom_heap_dump_enabled': False,
            'max_log_backup_index': 10,
            'max_log_size': 100,
            'log_threshold': "INFO",
            'enable_config_alerts': True,
        }
        varRole = varMgmt.get_role("HOSTMONITOR-1")
        varRole.update_config(varHostMonitorConfig)

    def deploy_service_monitor(self):
        """
        To deploy the service monitor
        :return:
        """
        varMgmt = self._cloudera_manager_oconnect.get_service()
        varServiceMonitorConfig = {
            'firehose_heapsize': 268435456,
            'mgmt_log_dir': "/opt/cloudera/log/cloudera-scm-firehose",
            'firehose_storage_dir': "/opt/cloudera/lib/cloudera-service-monitor",
            'oom_heap_dump_dir': "/tmp",
            'oom_heap_dump_enabled': False,
            'max_log_backup_index': 10,
            'max_log_size': 100,
            'log_threshold': "INFO",
            'enable_config_alerts': True,
        }

        varRole = varMgmt.get_role("SERVICEMONITOR-1")
        varRole.update_config(varServiceMonitorConfig)

    def deploy_report_manager(self):
        """
        To deploy the service Report Manager
        :return:
        """
        varReportManagerPassword = "OJgPd78yu9"
        varMgmt = self._cloudera_manager_oconnect.get_service()
        varReportManagerConfig = {
            'headlamp_database_host': "pocd-cm581-dev-manager.poc-d.internal" + ":" + "7432",
            'headlamp_database_user': "rman",
            'headlamp_database_password': varReportManagerPassword,
            'headlamp_database_type': "postgresql",
            'headlamp_database_name': "rman",
            'headlamp_heapsize': 536870912,
            'mgmt_log_dir': "/opt/cloudera/log/cloudera-scm-headlamp",
            'headlamp_scratch_dir': "/opt/cloudera/lib/cloudera-scm-headlamp",
            'oom_heap_dump_dir': "/tmp",
            'oom_heap_dump_enabled': False,
            'max_log_backup_index': 10,
            'max_log_size': 100,
            'log_threshold': "INFO",
            'enable_config_alerts': True,
        }
        varRole = varMgmt.get_role("REPORTSMANAGER-1")
        varRole.update_config(varReportManagerConfig)

    def deploy_services(self):
        """
        To deploy all the cloudera manager services
        :return:
        """
        varMgmt = self._cloudera_manager_oconnect.get_service()
        varMgmt.start().wait()

    def create_hadoop_cluster(self):
        """
        To create hadoop cluster with multiple data and name nodes and configure different services
        :return:
        """
        varClusterName = "POC-D Cluster"
        varCDHVersion = "CDH5"
        varCDHFullVersion = "5.8.0"

        varCluster = varApiResource.create_cluster(varClusterName, varCDHVersion, varCDHFullVersion)
