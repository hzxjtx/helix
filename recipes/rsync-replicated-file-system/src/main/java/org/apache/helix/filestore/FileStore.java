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

import org.apache.commons.io.FileUtils;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.helix.participant.StateMachineEngine;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class FileStore {
  private final String _zkAddr;
  private final String _clusterName;
  private final String _serverId;
  private final String _baseDir;
  private HelixManager _manager = null;

  public FileStore(String zkAddr, String clusterName, String serverId, String baseDir) {
    _zkAddr = zkAddr;
    _clusterName = clusterName;
    _serverId = serverId;
    _baseDir = baseDir;
  }

  public void connect() {
    try {
      _manager =
          HelixManagerFactory.getZKHelixManager(_clusterName, _serverId, InstanceType.PARTICIPANT,
              _zkAddr);

      StateMachineEngine stateMach = _manager.getStateMachineEngine();
      FileStoreStateModelFactory modelFactory = new FileStoreStateModelFactory(_manager);
      stateMach.registerStateModelFactory(SetupCluster.DEFAULT_STATE_MODEL, modelFactory);
      _manager.connect();

      /**
       * init config properties.
       */
      addConfigurationScopeOnZK();
      // _manager.addExternalViewChangeListener(replicator);
      Thread.currentThread().join();
    } catch (InterruptedException e) {
      System.err.println(" [-] " + _serverId + " is interrupted ...");
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      disconnect();
    }
  }

  public void disconnect() {
    if (_manager != null) {
      _manager.disconnect();
    }
  }

  private void addConfigurationScopeOnZK() throws IOException {
    Map<String, String> properties = new HashMap<String, String>();
    HelixConfigScopeBuilder builder = new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.PARTICIPANT);
    HelixConfigScope instanceScope =
            builder.forCluster(_clusterName).forParticipant(_serverId).build();
    String baseInstanceDir = _baseDir + _serverId;
    properties.put("change_log_dir", baseInstanceDir + "/translog");
    properties.put("file_store_dir", baseInstanceDir + "/filestore");
    properties.put("check_point_dir", baseInstanceDir + "/checkpoint");
    HelixAdmin admin = new ZKHelixAdmin(_zkAddr);
    admin.setConfig(instanceScope, properties);

    /**
     * clean the directory and recreate.
     */
    FileUtils.deleteDirectory(new File(properties.get("change_log_dir")));
    FileUtils.deleteDirectory(new File(properties.get("file_store_dir")));
    FileUtils.deleteDirectory(new File(properties.get("check_point_dir")));
    new File(properties.get("change_log_dir")).mkdirs();
    new File(properties.get("file_store_dir")).mkdirs();
    new File(properties.get("check_point_dir")).mkdirs();
  }

  public static void main(String[] args) throws Exception {
    if (args.length < 2) {
      System.err
          .println("USAGE: java FileStore zookeeperAddress(e.g. localhost:2181) serverId(host_port)");
      System.exit(1);
    }

    String baseDir = "/tmp/IntegrationTest/";
    final String clusterName = SetupCluster.DEFAULT_CLUSTER_NAME;

    final String zkAddr = args[0];
    final String serverId = args[1];

    ZkClient zkclient = null;
    try {
      // start consumer
      final FileStore store = new FileStore(zkAddr, clusterName, serverId, baseDir);

      Runtime.getRuntime().addShutdownHook(new Thread() {
        @Override
        public void run() {
          System.out.println("Shutting down server:" + serverId);
          store.disconnect();
        }
      });
      store.connect();
    } finally {
      if (zkclient != null) {
        zkclient.close();
      }
    }
  }
}
