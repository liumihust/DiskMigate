
package org.apache.hadoop.hdfs.server.diskmigration.planner;
/**
 * Planner takes a DiskBalancerVolumeSet, threshold and
 * computes a series of steps that lead to an even data
 * distribution between volumes of this DiskBalancerVolumeSet.
 *
 * The main classes of this package are steps and planner.
 *
 * Here is a high level view of how planner operates:
 *
 * DiskBalancerVolumeSet current = volumeSet;
 *
 * while(current.isBalancingNeeded(thresholdValue)) {
 *
 *   // Creates a plan , like move 20 GB data from v1 -> v2
 *   Step step = planner.plan(current, thresholdValue);
 *
 *   // we add that to our plan
 *   planner.addStep(current, step);
 *
 *   // Apply the step to current state of the diskSet to
 *   //compute the next state
 *   current = planner.apply(current, step);
 * }
 *
 * //when we are done , return the list of steps
 * return planner;
 */
