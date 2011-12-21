package com.linkedin.clustermanager.controller.stages;

import java.util.Arrays;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.clustermanager.CMConstants;
import com.linkedin.clustermanager.Mocks;
import com.linkedin.clustermanager.PropertyType;
import com.linkedin.clustermanager.ZNRecord;
import com.linkedin.clustermanager.model.IdealState;
import com.linkedin.clustermanager.model.LiveInstance;
import com.linkedin.clustermanager.pipeline.StageContext;
import com.linkedin.clustermanager.tools.IdealStateCalculatorForStorageNode;

public class TestCompatibilityCheckStage extends BaseStageTest
{
  private void prepare(String controllerVersion, String participantVersion)
  {
    List<String> instances = Arrays.asList("localhost_0", "localhost_1",
                                           "localhost_2", "localhost_3", "localhost_4");
    int partitions = 10;
    int replicas = 1;

    // set ideal state
    String resourceGroupName = "testResourceGroup";
    ZNRecord record = IdealStateCalculatorForStorageNode.calculateIdealState(
        instances, partitions, replicas, resourceGroupName, "MASTER", "SLAVE");
    IdealState idealState = new IdealState(record);
    idealState.setStateModelDefRef("MasterSlave");
    accessor.setProperty(PropertyType.IDEALSTATES,
        idealState.getRecord(), resourceGroupName);

    // set live instances
    record = new ZNRecord("localhost_0");
    if (participantVersion != null)
    {
      record.setSimpleField(CMConstants.ZNAttribute.CLUSTER_MANAGER_VERSION.toString(), participantVersion);
    }
    LiveInstance liveInstance = new LiveInstance(record);
    liveInstance.setSessionId("session_0");

    accessor.setProperty(PropertyType.LIVEINSTANCES, record, "localhost_0");

    if (controllerVersion != null)
    {
      ((Mocks.MockManager)manager).setVersion(controllerVersion);
    }
    event.addAttribute("clustermanager", manager);
    runStage(event, new ReadClusterDataStage());
  }

  @Test
  public void testCompatible()
  {
    prepare("0.4.0", "0.4.0");
    CompatibilityCheckStage stage = new CompatibilityCheckStage();
    StageContext context = new StageContext();
    stage.init(context);
    stage.preProcess();
    try
    {
      stage.process(event);
    }
    catch (Exception e)
    {
      Assert.fail("Should not fail since versions are compatible");
    }
    stage.postProcess();
  }

  @Test
  public void testNullParticipantVersion()
  {
    prepare("0.4.0", null);
    CompatibilityCheckStage stage = new CompatibilityCheckStage();
    StageContext context = new StageContext();
    stage.init(context);
    stage.preProcess();
    try
    {
      stage.process(event);
    }
    catch (Exception e)
    {
      Assert.fail("Should not fail since only participant version is null");
    }
    stage.postProcess();
  }

  @Test
  public void testNullControllerVersion()
  {
    prepare(null, "0.4.0");
    CompatibilityCheckStage stage = new CompatibilityCheckStage();
    StageContext context = new StageContext();
    stage.init(context);
    stage.preProcess();
    try
    {
      stage.process(event);
      Assert.fail("Should fail since controller version is null");
    }
    catch (Exception e)
    {
      // OK
    }
    stage.postProcess();
  }

  @Test
  public void testControllerVersionLessThanParticipantVersion()
  {
    prepare("0.2.12", "0.3.4");
    CompatibilityCheckStage stage = new CompatibilityCheckStage();
    StageContext context = new StageContext();
    stage.init(context);
    stage.preProcess();
    try
    {
      stage.process(event);
      Assert.fail("Should fail since controller primary version is less than participant primary version");
    }
    catch (Exception e)
    {
      // OK
    }
    stage.postProcess();
  }

  @Test
  public void testIncompatible()
  {
    prepare("0.4.12", "0.3.4");
    CompatibilityCheckStage stage = new CompatibilityCheckStage();
    StageContext context = new StageContext();
    stage.init(context);
    stage.preProcess();
    try
    {
      stage.process(event);
      Assert.fail("Should fail since controller primary version is incompatible with participant primary version");
    }
    catch (Exception e)
    {
      // OK
    }
    stage.postProcess();
  }

}
