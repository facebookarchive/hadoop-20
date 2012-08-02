/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.corona;

import org.apache.hadoop.util.CoronaSerializer;
import org.codehaus.jackson.JsonGenerator;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ResourceRequestInfo {
  ResourceRequest request;
  List<RequestedNode> nodes;
  Set<String> excludedHosts = new HashSet<String>();

  public ResourceRequestInfo(ResourceRequest request,
                             List<RequestedNode> nodes) {
    this.request = request;
    this.nodes = nodes;
    if (request.getExcludeHosts() != null) {
      excludedHosts.addAll(request.getExcludeHosts());
    }
  }

  /**
   * Constructor to reconstruct the ResourceRequestInfo object from the
   * file which stores the state on the disk.
   *  @param coronaSerializer The CoronaSerializer instance being used to read
   *                         the JSON from disk
   * @throws IOException
   */
  public ResourceRequestInfo(CoronaSerializer coronaSerializer)
    throws IOException {
    // Expecting the START_OBJECT token for ResourceRequestInfo
    coronaSerializer.readStartObjectToken("ResourceRequestInfo");

    coronaSerializer.readField("request");
    this.request = coronaSerializer.readValueAs(ResourceRequest.class);

    // Expecting the END_OBJECT token for ResourceRequestInfo
    coronaSerializer.readEndObjectToken("ResourceRequestInfo");

    // Restoring the excludedHosts
    if (request.getExcludeHosts() != null) {
      excludedHosts.addAll(request.getExcludeHosts());
    }

    // The list of RequestedNodes, nodes, will be restored later once the
    // topologyCache object is created
  }

  public int getId() {
    return request.id;
  }

  public ResourceType getType() {
    return request.type;
  }

  public Set<String> getExcludeHosts() {
    return excludedHosts;
  }

  public List<String> getHosts() {
    return request.hosts;
  }

  public ComputeSpecs getSpecs() {
    return request.getSpecs();
  }

  public List<RequestedNode> getRequestedNodes() {
    return nodes;
  }

  /**
   * This method writes the ResourceRequestInfo instance to disk
   * @param jsonGenerator The JsonGenerator instance being used to write the
   *                      JSON to disk
   * @throws IOException
   */
  public void write(JsonGenerator jsonGenerator) throws IOException {
    // We neither need the list of RequestedNodes, nodes, nor excludedHosts,
    // because we can reconstruct them from the request object
    jsonGenerator.writeStartObject();
    jsonGenerator.writeObjectField("request", request);
    jsonGenerator.writeEndObject();
  }
}
