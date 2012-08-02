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

import org.apache.hadoop.corona.CoronaConf;
import org.codehaus.jackson.JsonEncoding;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;
import org.codehaus.jackson.impl.DefaultPrettyPrinter;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class JsonUtils {
  /**
   * This is a helper method that reads a JSON token using a JsonParser
   * instance, and throws an exception if the next token is not START_OBJECT.
   *
   * @param jsonParser The JsonParser instance to be used
   * @param parentFieldName The name of the field
   * @throws IOException
   */
  public static void readStartObjectToken(JsonParser jsonParser,
                                          String parentFieldName)
    throws IOException {
    readToken(jsonParser, parentFieldName, JsonToken.START_OBJECT);
  }

  /**
   * This is a helper method that reads a JSON token using a JsonParser
   * instance, and throws an exception if the next token is not START_ARRAY.
   *
   * @param jsonParser The JsonParser instance to be used
   * @param parentFieldName The name of the field
   * @throws IOException
   */
  public static void readStartArrayToken(JsonParser jsonParser,
                                         String parentFieldName)
    throws IOException {
    readToken(jsonParser, parentFieldName, JsonToken.START_ARRAY);
  }

  /**
   * This is a helper method that reads a JSON token using a JsonParser
   * instance, and throws an exception if the next token is not END_OBJECT.
   *
   * @param jsonParser The JsonParser instance to be used
   * @param parentFieldName The name of the field
   * @throws IOException
   */
  public static void readEndObjectToken(JsonParser jsonParser,
                                          String parentFieldName)
    throws IOException {
    readToken(jsonParser, parentFieldName, JsonToken.END_OBJECT);
  }

  /**
   * This is a helper method that reads a JSON token using a JsonParser
   * instance, and throws an exception if the next token is not END_ARRAY.
   *
   * @param jsonParser The JsonParser instance to be used
   * @param parentFieldName The name of the field
   * @throws IOException
   */
  public static void readEndArrayToken(JsonParser jsonParser,
                                         String parentFieldName)
    throws IOException {
    readToken(jsonParser, parentFieldName, JsonToken.END_ARRAY);
  }

  /**
   * This is a helper method that reads a JSON token using a JsonParser
   * instance, and throws an exception if the next token is not the same as
   * the token we expect.
   *
   * @param jsonParser The JsonParser instance to be used
   * @param parentFieldName The name of the field
   * @param expectedToken The expected token
   * @throws IOException
   */
  public static void readToken(JsonParser jsonParser,
                               String parentFieldName,
                               JsonToken expectedToken)
    throws IOException {
    JsonToken currentToken = jsonParser.nextToken();
    if (currentToken != expectedToken) {
      throw new IOException("Expected a " + expectedToken.toString() +
                            " token when reading the value of the field: " +
                            parentFieldName +
                            " but found a " +
                            currentToken.toString() + " token");
    }
  }

  /**
   * This is a helper method which creates a JsonGenerator instance, for writing
   * the state of the ClusterManager to the state file. The JsonGenerator
   * instance writes to a compressed file if we have the compression flag
   * turned on.
   *
   * @param conf The CoronaConf instance to be used
   * @return The JsonGenerator instance to be used
   * @throws IOException
   */
  public static JsonGenerator createJsonGenerator(CoronaConf conf)
    throws IOException {
    OutputStream outputStream = new FileOutputStream(conf.getCMStateFile());
    if (conf.getCMCompressStateFlag()) {
      outputStream = new GZIPOutputStream(outputStream);
    }
    ObjectMapper mapper = new ObjectMapper();
    JsonGenerator jsonGenerator =
      new JsonFactory().createJsonGenerator(outputStream, JsonEncoding.UTF8);
    jsonGenerator.setCodec(mapper);
    if (!conf.getCMCompressStateFlag()) {
      jsonGenerator.setPrettyPrinter(new DefaultPrettyPrinter());
    }
    return jsonGenerator;
  }

  /**
   * This is a helper method which creates a JsonParser instance, for reading
   * back the state of the ClusterManager from the state file. The JsonParser
   * instance reads from a compressed file if we have the compression flag
   * turned on.
   *
   * @param conf The CoronaConf instance to be used
   * @return The JsonParser instance to be used
   * @throws IOException
   */
  public static JsonParser createJsonParser(CoronaConf conf)
    throws IOException {
    InputStream inputStream = new FileInputStream(conf.getCMStateFile());
    if (conf.getCMCompressStateFlag()) {
      inputStream = new GZIPInputStream(inputStream);
    }
    ObjectMapper mapper = new ObjectMapper();
    mapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES,
      false);
    JsonFactory jsonFactory = new JsonFactory();
    jsonFactory.setCodec(mapper);
    return jsonFactory.createJsonParser(inputStream);
  }

  /**
   * This is a helper method, which is used to throw an exception when we
   * encounter an unexpected field name.
   *
   * @param fieldName Name of the field
   * @param expectedFieldName Name of the expected field
   * @throws IOException
   */
  public static void foundUnknownField(String fieldName,
                                       String expectedFieldName)
    throws IOException {
    throw new IOException("Found an unexpected field: " + fieldName +
      ", instead of field: " + expectedFieldName);
  }

  /**
   * The method reads a field from the JSON stream, and checks if the
   * field read is the same as the expect field.
   *
   * @param jsonParser The JsonParser instance to be used
   * @param expectedFieldName The field name which is expected next
   * @throws IOException
   */
  public static void readField(JsonParser jsonParser,
                               String expectedFieldName) throws IOException {
    readToken(jsonParser, expectedFieldName, JsonToken.FIELD_NAME);
    String fieldName = jsonParser.getCurrentName();
    if (!fieldName.equals(expectedFieldName)) {
      foundUnknownField(fieldName, expectedFieldName);
    }
  }
}
