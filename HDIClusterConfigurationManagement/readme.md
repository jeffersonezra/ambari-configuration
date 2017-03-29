This tool is used to export configurations from a cluster and import it to the same or different cluster. If there are any changes in configuration, the updated services will be restarted if required.

#Prerequisite#
JDK 1.7 for development.

#Build#

mvn install

#Use#
**Command line**

java -cp HDInsight-Cluster-Configuration-Management-with-dependencies.jar com.microsoft.configuration.management.Main

**Operations**

  * *Download configuration*

    Downloads configurations of components specified in the config file. Some of the configurations are cluster specific. These are listed in the config file and will not be downloaded.

  * *Upload configuration*

    Uploads configurations of components speficied in the config file. This is a two step process. First, the cluster specific configurations specified in the config file are downloaded from the cluster to which the configuration has to be applied. It is then merged with the downloaded config and then applied to the cluster.


**Configuration file**

Sample configuration file is config.json

*ambariUrl*: Required. Ambari/Gateway URL of the cluster.

*ambariUsername*: Required. Ambari username.

*ambariPassword*: Required. Ambari password.

*tag*: Not required for download. Optional for upload. Ambari associates a tag with every new configuration so that it can reapplied later.

*services*: Required. HDInsight services whose configurations have to be downloaded/updated.

  * *configFiles*: Required

    Configurations that have to downloaded/applied.
  * *ignore*: Optional

    Cluster sensitive information in the config file that doesn't have to be downloaded. When applying configurations, these configurations will be first downloaded from the target cluster and applied with rest of the configurations.

*location*: Required. Location in file system to download configurations to and read configurations from during upload.
