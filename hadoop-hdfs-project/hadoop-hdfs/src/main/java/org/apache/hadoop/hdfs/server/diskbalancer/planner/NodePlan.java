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

package org.apache.hadoop.hdfs.server.diskbalancer.planner;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * NodePlan is a set of volumeSetPlans.
 */
public class NodePlan {
  @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS,
      include = JsonTypeInfo.As.PROPERTY, property = "@class")
  private List<Step> volumeSetPlans;
  private String nodeName;
  private String nodeUUID;
  private int port;
  private long timeStamp;
  private String type;

  public void setType(String type){
    this.type = type;
  }
  public String getType(){
    return this.type;
  }


  /**
   * returns timestamp when this plan was created.
   *
   * @return long
   */
  public long getTimeStamp() {
    return timeStamp;
  }

  /**
   * Sets the timestamp when this plan was created.
   *
   * @param timeStamp
   */
  public void setTimeStamp(long timeStamp) {
    this.timeStamp = timeStamp;
  }

  /**
   * Constructs an Empty Node Plan.
   */
  public NodePlan() {
    volumeSetPlans = new LinkedList<>();
  }

  /**
   * Constructs an empty NodePlan.
   */
  public NodePlan(String type, String datanodeName, int rpcPort) {
    volumeSetPlans = new LinkedList<>();
    this.nodeName = datanodeName;
    this.port = rpcPort;
    this.type = type;
  }

  public NodePlan(String datanodeName, int rpcPort) {
    volumeSetPlans = new LinkedList<>();
    this.nodeName = datanodeName;
    this.port = rpcPort;
    this.type = PlannerFactory.GREEDY_PLANNER;
  }

  /**
   * Returns a Map of  VolumeSetIDs and volumeSetPlans.
   *
   * @return Map
   */
  public List<Step> getVolumeSetPlans() {
    return volumeSetPlans;
  }

  /**
   * Adds a step to the existing Plan.
   *
   * @param nextStep - nextStep
   */
  public void addStep(Step nextStep) {
    Preconditions.checkNotNull(nextStep);
    nextStep.setType(this.type);
    volumeSetPlans.add(nextStep);

  }

  /**
   * Sets Node Name.
   *
   * @param nodeName - Name
   */
  public void setNodeName(String nodeName) {
    this.nodeName = nodeName;
  }

  /**
   * Sets a volume List plan.
   *
   * @param volumeSetPlans - List of plans.
   */
  public void setVolumeSetPlans(List<Step> volumeSetPlans) {
    this.volumeSetPlans = volumeSetPlans;
  }

  /**
   * Returns the DataNode URI.
   *
   * @return URI
   */
  public String getNodeName() {
    return nodeName;
  }

  /**
   * Sets the DataNodeURI.
   *
   * @param dataNodeName - String
   */
  public void setURI(String dataNodeName) {
    this.nodeName = dataNodeName;
  }

  /**
   * Gets the DataNode RPC Port.
   *
   * @return port
   */
  public int getPort() {
    return port;
  }

  /**
   * Sets the DataNode RPC Port.
   *
   * @param port - int
   */
  public void setPort(int port) {
    this.port = port;
  }

  /**
   * Parses a Json string and converts to NodePlan.
   *
   * @param json - Json String
   * @return NodePlan
   * @throws IOException
   */
  public static NodePlan parseJson(String json) throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    return mapper.readValue(json, NodePlan.class);
  }

  /**
   * Returns a Json representation of NodePlan.
   *
   * @return - json String
   * @throws IOException
   */
  public String toJson() throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    JavaType planType = mapper.constructType(NodePlan.class);
    return mapper.writerFor(planType)
        .writeValueAsString(this);
  }

  /**
   * gets the Node UUID.
   *
   * @return Node UUID.
   */
  public String getNodeUUID() {
    return nodeUUID;
  }

  /**
   * Sets the Node UUID.
   *
   * @param nodeUUID - UUID of the node.
   */
  public void setNodeUUID(String nodeUUID) {
    this.nodeUUID = nodeUUID;
  }
}
