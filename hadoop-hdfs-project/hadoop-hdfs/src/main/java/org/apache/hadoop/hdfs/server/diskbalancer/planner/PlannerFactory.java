/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdfs.server.diskbalancer.planner;

import org.apache.hadoop.hdfs.server.diskmigration.planner.DiskMigratePlanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hdfs.server.diskbalancer.datamodel
    .DiskBalancerDataNode;

/**
 * add by lm140765@alibaba-inc.com 2017/07/21
 * Returns a planner based on the user defined typeName.
 */
public final class PlannerFactory {
  private static final Logger LOG =
      LoggerFactory.getLogger(PlannerFactory.class);

  public static final String GREEDY_PLANNER = "greedy";
  public static final String DISKMIGRATE_PLANNER = "diskMigrate";

  /**
   *  Gets a planner object.
   *  add by lm140765@alibaba-inc.com 2017/07/21
   * @param plannerType - type of the planner.
   * @param node - DataNode.
   * @param threshold - percentage, only used for balancer
   * @return Planner
   */
  public static Planner getPlanner(String plannerType,
      DiskBalancerDataNode node, double threshold) {

    if (plannerType.equals(DISKMIGRATE_PLANNER)) {
      if (LOG.isDebugEnabled()) {
        String message = String
                .format("Creating a %s for Node : %s IP : %s ID : %s",
                DISKMIGRATE_PLANNER, node.getDataNodeName(), node.getDataNodeIP(),
                node.getDataNodeUUID());
        LOG.debug(message);
      }
      return new DiskMigratePlanner(node);

    }else {
      if (LOG.isDebugEnabled()) {
        String message = String
                .format("Creating a %s for Node : %s IP : %s ID : %s",
                GREEDY_PLANNER, node.getDataNodeName(), node.getDataNodeIP(),
                node.getDataNodeUUID());
        LOG.debug(message);
      }
      return new GreedyPlanner(threshold, node);
    }
  }

  private PlannerFactory() {
    // Never constructed
  }
}
