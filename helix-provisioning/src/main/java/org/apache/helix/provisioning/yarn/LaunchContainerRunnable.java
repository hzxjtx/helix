package org.apache.helix.provisioning.yarn;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.hadoop.yarn.api.ContainerManagementProtocol;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.util.Records;

/**
 * Thread to connect to the {@link ContainerManagementProtocol} and launch the container
 * that will execute the shell command.
 */
class LaunchContainerRunnable implements Runnable {

  /**
   * 
   */
  private final GenericApplicationMaster _genericApplicationMaster;

  // Allocated container
  Container container;

  NMCallbackHandler containerListener;

  /**
   * @param lcontainer Allocated container
   * @param containerListener Callback handler of the container
   * @param genericApplicationMaster TODO
   */
  public LaunchContainerRunnable(GenericApplicationMaster genericApplicationMaster, Container lcontainer, NMCallbackHandler containerListener) {
    _genericApplicationMaster = genericApplicationMaster;
    this.container = lcontainer;
    this.containerListener = containerListener;
  }

  @Override
  /**
   * Connects to CM, sets up container launch context 
   * for shell command and eventually dispatches the container 
   * start request to the CM. 
   */
  public void run() {
    GenericApplicationMaster.LOG.info("Setting up container launch container for containerid=" + container.getId());
    ContainerLaunchContext ctx = Records.newRecord(ContainerLaunchContext.class);

    // Set the environment
    //ctx.setEnvironment(shellEnv);

    // Set the local resources
    Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();

    ctx.setLocalResources(localResources);

    // Set the necessary command to execute on the allocated container
    Vector<CharSequence> vargs = new Vector<CharSequence>(5);

  
    List<String> commands = new ArrayList<String>();
   // commands.add(command.toString());
    ctx.setCommands(commands);

    // Set up tokens for the container too. Today, for normal shell commands,
    // the container in distribute-shell doesn't need any tokens. We are
    // populating them mainly for NodeManagers to be able to download any
    // files in the distributed file-system. The tokens are otherwise also
    // useful in cases, for e.g., when one is running a "hadoop dfs" command
    // inside the distributed shell.
    ctx.setTokens(_genericApplicationMaster.allTokens.duplicate());

    containerListener.addContainer(container.getId(), container);
    _genericApplicationMaster.nmClientAsync.startContainerAsync(container, ctx);
  }
}