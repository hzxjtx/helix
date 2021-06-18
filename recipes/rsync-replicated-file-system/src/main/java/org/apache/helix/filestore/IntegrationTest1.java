package org.apache.helix.filestore;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.controller.HelixControllerMain;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.HelixConfigScope.ConfigScopeProperty;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.helix.tools.ClusterSetup;
import org.apache.helix.zookeeper.zkclient.IDefaultNameSpace;
import org.apache.helix.zookeeper.zkclient.ZkClient;
import org.apache.helix.zookeeper.zkclient.ZkServer;


public class IntegrationTest1 {

  public static void main(String[] args) throws InterruptedException {
    final String zkAddress = "172.20.3.83";
    final int    zkPort = 2181;
    try {
      String baseDir = "/tmp/IntegrationTest/";
      final String dataDir = baseDir + "zk/dataDir";
      final String logDir = baseDir + "/tmp/logDir";
      FileUtils.deleteDirectory(new File(dataDir));
      FileUtils.deleteDirectory(new File(logDir));

      IDefaultNameSpace defaultNameSpace = new IDefaultNameSpace() {
        @Override
        public void createDefaultNameSpace(ZkClient zkClient) {

        }
      };

      final String clusterName = "file-store-test";
      ClusterSetup setup = new ClusterSetup(zkAddress);
      // Set the configuration
      final String instanceName1 = "localhost_12001";
      addConfiguration(setup, baseDir, clusterName, instanceName1);
      final String instanceName2 = "localhost_12002";
      addConfiguration(setup, baseDir, clusterName, instanceName2);
      final String instanceName3 = "localhost_12003";
      addConfiguration(setup, baseDir, clusterName, instanceName3);

      listFiles(baseDir);
      System.out.println("Writing files a.txt and b.txt to current master " + baseDir
          + "localhost_12001" + "/filestore");
      FileUtils.writeStringToFile(new File(baseDir + "localhost_12001" + "/filestore/a.txt"),
          "some_data in a");
      FileUtils.writeStringToFile(new File(baseDir + "localhost_12001" + "/filestore/b.txt"),
          "some_data in b");
      listFiles(baseDir);

      Thread.sleep(10000);

      System.out.println("Writing files c.txt and d.txt to current master " + baseDir
          + "localhost_12001" + "/filestore");
      FileUtils.writeStringToFile(new File(baseDir + "localhost_12001" + "/filestore/c.txt"),
          "some_data in c");
      FileUtils.writeStringToFile(new File(baseDir + "localhost_12001" + "/filestore/d.txt"),
          "some_data in d");
      listFiles(baseDir);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
    }
  }

  private static void listFiles(String baseDir) {
    System.out.println("===============FILES===============================");
    String[] instances = new String[] {
        "localhost_12001", "localhost_12002", "localhost_12003"
    };
    for (String instance : instances) {
      String dir = baseDir + instance + "/filestore";
      String[] list = new File(dir).list();
      System.out.println(dir + ":" + ((list != null) ? Arrays.toString(list) : "NONE"));
    }
    System.out.println("===============FILES===============================");
  }

  private static void printStatus(final HelixManager manager) {
    System.out.println("CLUSTER STATUS");
    HelixDataAccessor helixDataAccessor = manager.getHelixDataAccessor();
    Builder keyBuilder = helixDataAccessor.keyBuilder();
    System.out.println("External View \n"
        + helixDataAccessor.getProperty(keyBuilder.externalView("repository")));
  }

  private static void addConfiguration(ClusterSetup setup, String baseDir, String clusterName,
      String instanceName) throws IOException {
    Map<String, String> properties = new HashMap<String, String>();
    HelixConfigScopeBuilder builder = new HelixConfigScopeBuilder(ConfigScopeProperty.PARTICIPANT);
    HelixConfigScope instanceScope =
        builder.forCluster(clusterName).forParticipant(instanceName).build();
    properties.put("change_log_dir", baseDir + instanceName + "/translog");
    properties.put("file_store_dir", baseDir + instanceName + "/filestore");
    properties.put("check_point_dir", baseDir + instanceName + "/checkpoint");
    setup.getClusterManagementTool().setConfig(instanceScope, properties);
    FileUtils.deleteDirectory(new File(properties.get("change_log_dir")));
    FileUtils.deleteDirectory(new File(properties.get("file_store_dir")));
    FileUtils.deleteDirectory(new File(properties.get("check_point_dir")));
    new File(properties.get("change_log_dir")).mkdirs();
    new File(properties.get("file_store_dir")).mkdirs();
    new File(properties.get("check_point_dir")).mkdirs();
  }
}
