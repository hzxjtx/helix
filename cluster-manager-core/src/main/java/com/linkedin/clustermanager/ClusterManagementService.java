package com.linkedin.clustermanager;

import java.util.List;

public interface ClusterManagementService
{
  List<String> getClusters();

  List<String> getInstancesInCluster(String clusterName);

  ZNRecord getInstanceConfig(String clusterName, String instanceName);

  List<String> getResourceGroupsInCluster(String clusterName);

  void addCluster(String clusterName, boolean overwritePrevRecord);

  void addResourceGroup(String clusterName, String resourceGroup, int numResources,
                        String stateModelRef);

  void addResourceGroup(String clusterName, String resourceGroup, int numResources,
                        String stateModelRef, String idealStateMode);

  void addInstance(String clusterName, ZNRecord instanceConfig);

  void dropInstance(String clusterName, ZNRecord instanceConfig);

  ZNRecord getResourceGroupIdealState(String clusterName, String dbName);

  void setResourceGroupIdealState(String clusterName, String resourceGroup, ZNRecord idealState);

  void enableInstance(String clusterName, String instanceName, boolean enabled);
  void enablePartition(String clusterName, String instanceName, String partition,
                       boolean enabled);

  void addStateModelDef(String clusterName, String stateModelDef, ZNRecord record);

  void dropResourceGroup(String clusterName, String resourceGroup);

  void addStat(String clusterName, String statName);
  
  void addAlert(String clusterName, String alertName);
  
  List<String> getStateModelDefs(String clusterName);

  ZNRecord getStateModelDef(String clusterName, String stateModelName);

  ZNRecord getResourceGroupExternalView(String clusterName, String resourceGroup);
}
