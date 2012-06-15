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

package org.apache.hadoop.util;

/**
 * This interface is for JMX use.
 * The build information of the hadoop jars will be visible through the fields
 * of this interface
 */
public interface VersionInfoMBean {
  /**
   * Returns the string with Hadoop version of the form "Hadoop $VERSION"
   */
  public String version();
  /**
   * Returns the subversion url that was used to build the distribution
   */
  public String subversion();
  /**
   * Returns the string with the name of the builder and the date of the build
   */
  public String compiledby();
}
