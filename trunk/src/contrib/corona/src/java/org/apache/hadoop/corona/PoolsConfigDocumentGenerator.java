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

import org.w3c.dom.Document;

/**
 * Interface for generating a pools config document.
 */
public interface PoolsConfigDocumentGenerator {
  /**
   * Do any initialization as necessary with the Corona configuration.
   * Guaranteed to be called prior to generatePoolConfig().  The configuration
   * should be used to get the proper config filename to generate.
   *
   * @param conf Corona configuration
   */
  void initialize(CoronaConf conf);

  /**
   * Generate the pool config xml document to be parsed by
   * {@link ConfigManager}.
   *
   * @return Document that contains the pools config or null, if nothing to
   *         use
   */
  Document generatePoolsDocument();
}
