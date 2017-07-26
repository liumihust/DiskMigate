/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.hdfs.server.diskmigration.planner;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdfs.server.diskbalancer.datamodel.DiskBalancerDataNode;
import org.apache.hadoop.hdfs.server.diskbalancer.datamodel.DiskBalancerVolume;
import org.apache.hadoop.hdfs.server.diskbalancer.datamodel.DiskBalancerVolumeSet;
import org.apache.hadoop.hdfs.server.diskbalancer.planner.NodePlan;
import org.apache.hadoop.hdfs.server.diskbalancer.planner.Planner;
import org.apache.hadoop.hdfs.server.diskbalancer.planner.PlannerFactory;
import org.apache.hadoop.hdfs.server.diskbalancer.planner.Step;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by lm140765 on 2017/7/22.
 */

/**
 * DiskMigrate Planner is a planner that move all the data of some disk
 * to one disk that newly added to the DataNode
 */
public class DiskMigratePlanner implements Planner {
  public static final long MB = 1024L * 1024L;
  public static final long GB = MB * 1024L;
  public static final long TB = GB * 1024L;
  private static final Logger LOG =
      LoggerFactory.getLogger(DiskMigratePlanner.class);

  /**
   * Constructs a DiskMigrate planner.
   * @param node   - node on which this planner is operating upon
   */
  public DiskMigratePlanner(DiskBalancerDataNode node, String type) {
  }
  public DiskMigratePlanner(DiskBalancerDataNode node) {
  }


  /**
   * Computes a node plan for the given node.
   * @return NodePlan
   * @throws Exception
   */
  @Override
  public NodePlan plan(DiskBalancerDataNode node) throws Exception {
    long startTime = Time.monotonicNow();
    NodePlan plan = new NodePlan(PlannerFactory.DISKMIGRATE_PLANNER,
            node.getDataNodeName(), node.getDataNodePort());
    LOG.info("Starting plan for Node : {}:{}",
        node.getDataNodeName(), node.getDataNodePort());

    for (DiskBalancerVolumeSet vSet : node.getVolumeSets().values()) {
      migrateVolumeSet_multi(node, vSet, plan);
    }

    long endTime = Time.monotonicNow();
    String message = String
        .format("Compute Plan for Node : %s:%d took %d ms ",
            node.getDataNodeName(), node.getDataNodePort(),
            endTime - startTime);
    LOG.info(message);
    System.out.println("@@@@@number of steps: "+plan.getVolumeSetPlans().size());
    LOG.info("@@@@@number of steps: "+plan.getVolumeSetPlans().size());
    return plan;
  }


  /**
   * Computes Steps to make a DiskBalancerVolumeSet Balanced.
   * There are only one destination volume
   * @param node
   * @param vSet - DiskBalancerVolumeSet
   * @param plan - NodePlan
   */
  public void migrateVolumeSet_single(DiskBalancerDataNode node,
                               DiskBalancerVolumeSet vSet,
                               NodePlan plan)
      throws Exception {

    Preconditions.checkNotNull(vSet);
    Preconditions.checkNotNull(plan);
    Preconditions.checkNotNull(node);
    DiskBalancerVolumeSet currentSet = new DiskBalancerVolumeSet(vSet);
    if(currentSet.getVolumes() == null){
        LOG.info("empty volumeSet!");
        return;
    }else if(currentSet.getVolumes().size() <= 1){
        LOG.info("there are no volume or only one volume");
        return;
    }

    /*
    *!!!init the SortedQueue of the VolumeSet
    * through the Volumes and SortedQueue have the same elements
    * however, the the SortedQueue has't been init in VolumeSet
    * or,currentSet.getSortedQueue().first() will return null
     */
    removeSkipVolumes(currentSet);

    DiskBalancerVolume lowVolume = currentSet.getSortedQueue().first();
    for(DiskBalancerVolume highVolume : currentSet.getVolumes()){
      if(!highVolume.equals(lowVolume)){
        Step nextStep = computeMove(currentSet, lowVolume, highVolume);
        applyStep(nextStep, currentSet, lowVolume, highVolume);
        if (nextStep != null) {
          LOG.debug("Step : {} ",  nextStep.toString());
          plan.addStep(nextStep);
        }
      }
    }

    String message = String
        .format("Disk Volume set %s Type : %s plan completed.",
            currentSet.getSetID(),
            currentSet.getVolumes().get(0).getStorageType());

    plan.setNodeName(node.getDataNodeName());
    plan.setNodeUUID(node.getDataNodeUUID());
    plan.setTimeStamp(Time.now());
    plan.setPort(node.getDataNodePort());
    LOG.info(message);
  }


  /**
   * Computes Steps to make a DiskBalancerVolumeSet Balanced.
   * There are multi destination volumes
   * @param node
   * @param vSet - DiskBalancerVolumeSet
   * @param plan - NodePlan
   */
  public void migrateVolumeSet_multi(DiskBalancerDataNode node,
                               DiskBalancerVolumeSet vSet,
                               NodePlan plan)
          throws Exception {

    Preconditions.checkNotNull(vSet);
    Preconditions.checkNotNull(plan);
    Preconditions.checkNotNull(node);
    DiskBalancerVolumeSet currentSet = new DiskBalancerVolumeSet(vSet);
    if(currentSet.getVolumes() == null || currentSet.getVolumes().size() <= 1 ){
      LOG.info("skip the invalid volumeSet!");
      return;
    }

    /*
    *!!!init the SortedQueue of the VolumeSet
    * through the Volumes and SortedQueue have the same elements
    * however, the the SortedQueue has't been init in VolumeSet
    * or,currentSet.getSortedQueue().first() will return null
     */
    removeSkipVolumes(currentSet);
    Map<String,DiskBalancerVolume> id2volume = new HashMap<String,DiskBalancerVolume>();
    for(DiskBalancerVolume disk : currentSet.getVolumes()){
      id2volume.put(disk.getUuid(),disk);
    }
    Map<String, Map<String,Long>> old2new = MultiDiskMigration.calculate(currentSet);
    for(Map.Entry<String, Map<String,Long>> entry : old2new.entrySet()){
      DiskBalancerVolume oldDisk = id2volume.get(entry.getKey());
      for(Map.Entry<String,Long> newDisks : entry.getValue().entrySet()){
        DiskBalancerVolume newDisk = id2volume.get(newDisks.getKey());
        long bytesTOMigrate = newDisks.getValue();
        Step nextStep = new DiskMigrationStep(oldDisk, newDisk,
                bytesTOMigrate, currentSet.getSetID());
        plan.addStep(nextStep);
      }
    }

    String message = String
            .format("Disk Volume set %s Type : %s plan completed.",
                    currentSet.getSetID(),
                    currentSet.getVolumes().get(0).getStorageType());

    plan.setNodeName(node.getDataNodeName());
    plan.setNodeUUID(node.getDataNodeUUID());
    plan.setTimeStamp(Time.now());
    plan.setPort(node.getDataNodePort());
    LOG.info(message);
  }


  /**
   * Apply steps applies the current step on to a volumeSet so that we can
   * compute next steps until we reach the desired goals.
   *
   * @param nextStep   - nextStep or Null
   * @param currentSet - Current Disk BalancerVolume Set we are operating upon
   * @param lowVolume  - volume
   * @param highVolume - volume
   */
  private void applyStep(Step nextStep, DiskBalancerVolumeSet currentSet,
                         DiskBalancerVolume lowVolume,
                         DiskBalancerVolume highVolume) throws Exception {

    if (nextStep != null) {
      lowVolume.setUsed(lowVolume.getUsed() + nextStep.getBytesToMove());
      highVolume.setUsed(highVolume.getUsed() - nextStep.getBytesToMove());
    }

    // since the volume data changed , we need to recompute the DataDensity.
    currentSet.computeVolumeDataDensity();
    printQueue(currentSet.getSortedQueue());
  }


  /**
   * Computes a data move from the largest disk we have to smallest disk.
   *
   * @param currentSet - Current Disk Set we are working with
   * @param lowVolume  - Low(New) Volume
   * @param highVolume - High Data Capacity Volume
   * @return Step
   */
  private Step computeMove(DiskBalancerVolumeSet currentSet,
                           DiskBalancerVolume lowVolume,
                           DiskBalancerVolume highVolume) {
    // Compute how many bytes we should move.
    long maxHighVolumeCanGive = highVolume.getUsed();
    if (maxHighVolumeCanGive <= 0) {
      LOG.debug(" {} Skipping disk from computation. Minimum data size " +
          "achieved.", highVolume.getPath());
      skipVolume(currentSet, highVolume);
    }
    long bytesToMove = Math.min(maxHighVolumeCanGive,
            lowVolume.computeEffectiveCapacity()-lowVolume.getUsed());

    Step nextStep = null;
    if (bytesToMove > 0) {
      // Create a new step
      nextStep = new DiskMigrationStep(highVolume, lowVolume,
              bytesToMove, currentSet.getSetID());
      LOG.debug(nextStep.toString());
    }
    return nextStep;
  }


  /**
   * Skips this volume if needed.
   *
   * @param currentSet - Current Disk set
   * @param volume     - Volume
   */
  private void skipVolume(DiskBalancerVolumeSet currentSet,
                          DiskBalancerVolume volume) {
    if (LOG.isDebugEnabled()) {
      String message =
          String.format(
              "Skipping volume. Volume : %s " +
              "Type : %s Target " +
              "Number of bytes : %f lowVolume dfsUsed : %d. Skipping this " +
              "volume from all future balancing calls.", volume.getPath(),
              volume.getStorageType(),
              currentSet.getIdealUsed() * volume.getCapacity(),
              volume.getUsed());
      LOG.debug(message);
    }
    volume.setSkip(true);
  }


  /**
   * remove the skipped volume.
   *
   * @param currentSet - Current Disk set
   */
  private void removeSkipVolumes(DiskBalancerVolumeSet currentSet) {
      List<DiskBalancerVolume> volumeList = currentSet.getVolumes();
      Iterator<DiskBalancerVolume> Iterator = volumeList.iterator();
      while (Iterator.hasNext()) {
          DiskBalancerVolume vol = Iterator.next();
          if (vol.isSkip() || vol.isFailed()) {
              currentSet.removeVolume(vol);
          }
      }
      currentSet.computeVolumeDataDensity();
      printQueue(currentSet.getSortedQueue());
  }


  /**
   * This function is used only for debugging purposes to ensure queue looks
   * correct.
   *
   * @param queue - Queue
   */
  private void printQueue(TreeSet<DiskBalancerVolume> queue) {
    if (LOG.isDebugEnabled()) {
      String format =
          String.format(
              "First Volume : %s, DataDensity : %f, " +
              "Last Volume : %s, DataDensity : %f",
              queue.first().getPath(), queue.first().getVolumeDataDensity(),
              queue.last().getPath(), queue.last().getVolumeDataDensity());
      LOG.debug(format);
    }
  }
}