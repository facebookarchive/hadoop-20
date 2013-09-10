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

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.Collections;
import java.util.List;
import java.util.LinkedList;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.util.CoronaSerializer;
import org.apache.thrift.*;
import org.apache.thrift.protocol.*;
import org.apache.thrift.transport.*;
import org.codehaus.jackson.JsonGenerator;

/**
 * Notification state maintained on behalf of each session
 *
 * The notifications are dispatched in a serial order. Failed notifications
 * are retried a configurable number of times.
 */
public class SessionNotificationCtx implements Configurable {
  /** Class logger */
  public final static Log LOG = LogFactory.getLog(SessionNotificationCtx.class);

  CoronaConf conf;
  /**
   * Time (in milliseconds) when to make the next RPC call
   * -1 means to make the call immediately
   */
  long nextDispatchTime = -1;
  /**
   * Number of retries that have been made for the first call
   * in the list.
   */
  int numRetries = 0;
  /** Current retry interval */
  int currentRetryInterval;
  /** Starting retry interval */
  int retryIntervalStart;
  /** Multiplier between successive retry intervals */
  int retryIntervalFactor;
  /** Max number of retries */
  int retryCountMax;

  /**
   * List of pending calls for this session
   */
  List<TBase> pendingCalls =
      Collections.synchronizedList(new LinkedList<TBase>());

  final String                  handle;
  final String                  host;
  final int                     port;
  TTransport                    transport = null;
  SessionDriverService.Client   client = null;
  final Session                 session;

  public SessionNotificationCtx(Session session, String handle, String host, int port) {
    this.session = session;
    this.handle = handle;
    this.host = host;
    this.port = port;
  }

  /**
   * Constructor for SessionNotificationCtx, used when we are reading back the
   * ClusterManager state from the disk
   *
   * @param coronaSerializer The CoronaSerializer instance, which will be used
   *                         to read JSON from disk
   * @throws IOException
   */
  public SessionNotificationCtx(SessionManager sessionManager, CoronaSerializer coronaSerializer)
    throws IOException {
    coronaSerializer.readStartObjectToken("CoronaSerializer");

    coronaSerializer.readField("handle");
    this.handle = coronaSerializer.readValueAs(String.class);

    coronaSerializer.readField("host");
    this.host = coronaSerializer.readValueAs(String.class);

    coronaSerializer.readField("port");
    this.port = coronaSerializer.readValueAs(Integer.class);

    coronaSerializer.readField("numPendingCalls");
    int numPendingCalls = coronaSerializer.readValueAs(Integer.class);

    coronaSerializer.readField("pendingCalls");
    coronaSerializer.readStartArrayToken("pendingCalls");
    for (int i = 0; i < numPendingCalls; i++) {
      coronaSerializer.readStartObjectToken("pendingCall");

      coronaSerializer.readField("callType");
      String callType = coronaSerializer.readValueAs(String.class);

      coronaSerializer.readField("call");
      TBase call = null;
      if (callType.equals(SessionDriverService.grantResource_args.
        class.getName())) {
        call = coronaSerializer.
          readValueAs(SessionDriverService.grantResource_args.class);
      } else if (callType.equals(SessionDriverService.revokeResource_args.
        class.getName())) {
        call = coronaSerializer.
          readValueAs(SessionDriverService.revokeResource_args.class);
      } else if (callType.equals(SessionDriverService.processDeadNode_args.
        class.getName())) {
        call = coronaSerializer.
          readValueAs(SessionDriverService.processDeadNode_args.class);
      } else {
        throw new IOException("Unknown Class: " + callType);
      }

      coronaSerializer.readEndObjectToken("pendingCall");
      pendingCalls.add(call);
    }
    coronaSerializer.readEndArrayToken("pendingCalls");

    coronaSerializer.readEndObjectToken("CoronaSerializer");

    // check if the session is valid at the end of the parsing
    try {
      this.session = sessionManager.getSession(this.handle);
    } catch (InvalidSessionHandle e) {
      throw new IOException(e.getMessage());
    }

  }

  /**
   * Used to write the state of the SessionNotificationCtx instance to disk,
   * when we are persisting the state of the ClusterManager
   * @param jsonGenerator The JsonGenerator instance being used to write JSON
   *                      to disk
   * @throws IOException
   */
  public void write(JsonGenerator jsonGenerator) throws IOException {
    jsonGenerator.writeStartObject();

    jsonGenerator.writeStringField("handle", handle);

    jsonGenerator.writeStringField("host", host);

    jsonGenerator.writeNumberField("port", port);

    jsonGenerator.writeNumberField("numPendingCalls", pendingCalls.size());

    jsonGenerator.writeFieldName("pendingCalls");
    jsonGenerator.writeStartArray();
    for (TBase call : pendingCalls) {
      jsonGenerator.writeStartObject();

      // TBase is an abstract class. While reading back, we want to know
      // what kind of object we actually wrote. Jackson does provide two methods
      // to do it automatically, but one of them adds types at a lot of places
      // where we don't need it, and hence our parsing would be required to be
      // changed. The other required adding an annotation to the TBase class,
      // which we can't do, since it is auto-generated by Thrift.
      String callType = call.getClass().getName();
      jsonGenerator.writeStringField("callType", callType);

      jsonGenerator.writeObjectField("call", call);

      jsonGenerator.writeEndObject();
    }
    jsonGenerator.writeEndArray();

    jsonGenerator.writeEndObject();
  }

  private void init () throws TException {
    if (transport == null) {
      TSocket socket = new TSocket(host, port);
      socket.setTimeout(conf.getCMSoTimeout());
      transport = socket;
      client = new SessionDriverService.Client(new TBinaryProtocol(transport));
      transport.open();
    }
  }

  public String getSessionHandle() {
    return handle;
  }
  
  private void dispatchCall(TBase call) throws TException {
    if (call instanceof  SessionDriverService.grantResource_args) {
      SessionDriverService.grantResource_args args = ( SessionDriverService.grantResource_args)call;
      if (!args.handle.equals(handle))
        throw new RuntimeException ("Call handle: " + args.handle +
                                    " does not match session handle: " + handle);

      LOG.info("Dispatching grant " + args.handle + " " + args.granted.size());
      client.grantResource(args.handle, args.granted);
      //session.setResourceGrant(args.granted);
    } else if (call instanceof  SessionDriverService.revokeResource_args) {
      SessionDriverService.revokeResource_args args = ( SessionDriverService.revokeResource_args)call;
      if (!args.handle.equals(handle))
        throw new RuntimeException ("Call handle: " + args.handle +
                                    " does not match session handle: " + handle);

      LOG.info("Dispatching revoke " + args.handle + " " + args.revoked.size());
      client.revokeResource(args.handle, args.revoked, false);
    } else if (call instanceof SessionDriverService.processDeadNode_args) {
      SessionDriverService.processDeadNode_args args =
        (SessionDriverService.processDeadNode_args) call;
      if (!args.handle.equals(handle))
        throw new RuntimeException ("Call handle: " + args.handle +
                                    " does not match session handle: " + handle);
      client.processDeadNode(args.handle, args.node);
      LOG.info("Dispatching deadnode  " + args.handle);
    } else {
      throw new RuntimeException("Unknown Class: " + call.getClass().getName());
    }
  }

  /**
   * make callbacks to the sessiondriver. if the function returns false, then
   * the session should be discarded
   *
   * @return true on success. false if no calls can be made to session anymore
   */
  public boolean makeCalls(long now) {
    if (now < nextDispatchTime)
      return true;

    // we make calls in a loop until all pending calls are drained
    // if any call hits an error - we stop
    while (!pendingCalls.isEmpty()) {

      TBase call = pendingCalls.get(0);
      try {
        // initialize the client/transport unless already done
        init();

        // make one thrift call
        dispatchCall(call);

        // if we made a call successfully, reset any retry state
        nextDispatchTime = -1;
        numRetries = 0;
        currentRetryInterval = retryIntervalStart;

        pendingCalls.remove(0);

      } catch (TException e) {
        boolean logged = false;
        if (e instanceof TTransportException) {
          TTransportException tte = (TTransportException) e;
          Throwable cause =  tte.getCause();
          if (cause != null && cause instanceof SocketTimeoutException) {
            // Got a socket timeout while waiting for a response from the
            // client. The session is stuck.
            logged = true;
            LOG.error("Call to session: " + handle + " for call: " +
              call.getClass().getName() + ", numRetry: " + numRetries +
              "(retryCountMax=" + retryCountMax + ")" +
              " failed with SocketTimeoutException, will retry it");
          }
        }
        if (!logged) {
          LOG.warn("Call to session: " + handle + " for call: " +
            call.getClass().getName() + ", numRetry: " + numRetries +
            "(retryCountMax=" + retryCountMax + ")" +
            " failed with TException", e);
        }

        // close the transport/client on any exception
        // will be reopened on next try
        close();

        if (numRetries > retryCountMax)
          return false;

        numRetries++;
        nextDispatchTime = now + currentRetryInterval;
        currentRetryInterval *= retryIntervalFactor;

        // no more calls for now
        return true;
      }
    }

    close();

    return true;
  }

  public void close() {
    if (transport != null) {
      transport.close();
      transport = null;
      client = null;
    }
  }

  public int getNumPendingCalls() {
    return pendingCalls.size();
  }

  public void addCall(TBase call) {
    pendingCalls.add(call);
  }

  public void setConf(Configuration _conf) {
    this.conf = (CoronaConf) _conf;
    retryIntervalFactor = conf.getNotifierRetryIntervalFactor();
    retryCountMax = conf.getNotifierRetryMax();
    retryIntervalStart = conf.getNotifierRetryIntervalStart();
  }


  public Configuration getConf() {
    return conf;
  }
}
