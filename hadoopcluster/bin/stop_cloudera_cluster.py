#!/usr/bin/env python

import ConfigParser
from cm_api.api_client import ApiResource
from cm_api.endpoints.services import ApiService

# Prep for reading config props from external file
CONFIG = ConfigParser.ConfigParser()
CONFIG.read("clouderaconfig.ini")


# Set up environment-specific vars #

# This is the host that the Cloudera Manager server is running on
CM_HOST = CONFIG.get("CM", "cm.host")

# CM admin account info
ADMIN_USER = CONFIG.get("CM", "admin.name")
ADMIN_PASS = CONFIG.get("CM", "admin.password")

# Cluster Definition #
CLUSTER_NAME = CONFIG.get("CM", "cluster.name")
CDH_VERSION = "CDH5"


# Main function #
def main():
    API = ApiResource(CM_HOST, version=5, username=ADMIN_USER, password=ADMIN_PASS)
    print("Connected to CM host on " + CM_HOST)

    CLUSTER = API.get_cluster(CLUSTER_NAME)
    print "About to stop cluster."

    CLUSTER.stop().wait()
    print "Done stopping cluster."


if __name__ == "__main__":
    main()
