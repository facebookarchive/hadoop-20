package org.apache.hadoop.corona;

import java.util.Collections;
import java.util.List;
import java.util.LinkedList;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;

import org.apache.thrift.*;
import org.apache.thrift.protocol.*;
import org.apache.thrift.transport.*;

/**
 * Notification state maintained on behalf of each session
 *
 * The notifications are dispatched in a serial order. Failed notifications are retried
 * a configurable number of times.
 */
public class SessionNotificationCtx implements Configurable {

  public final static Log LOG = LogFactory.getLog(SessionNotificationCtx.class);

  CoronaConf conf;

  /**
   * Time (in milliseconds) when to make the next RPC call
   * -1 means to make the call immediately
   */
  long          nextDispatchTime = -1;

  /**
   * Number of retries that have been made for the first call 
   * in the list
   */
  short         numRetries = 0;

  /**
   * current retry interval
   */
  int           currentRetryInterval;

  /**
   * starting retry interval
   */
  int           retryIntervalStart;

  /**
   * multiplier between successive retry intervals
   */
  int           retryIntervalFactor;

  /**
   * max number of retries
   */
  int           retryCountMax;


  /**
   * List of pending calls for this session
   */
  List<TBase> pendingCalls = Collections.synchronizedList(new LinkedList<TBase> ());

  final String                  handle;
  final String                  host;
  final int                     port;
  TTransport                    transport = null;
  SessionDriverService.Client   client = null;

  public SessionNotificationCtx (String handle, String host, int port) {
    this.handle = handle;
    this.host = host;
    this.port = port;
  }

  private void init () throws TException {
    if (transport == null) {
      transport = new TSocket(host, port);
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

      //LOG.info ("Granting " + args.granted.size() + " resources to session: " + args.handle);
      client.grantResource(args.handle, args.granted);
    } else if (call instanceof  SessionDriverService.revokeResource_args) {
      SessionDriverService.revokeResource_args args = ( SessionDriverService.revokeResource_args)call;
      if (!args.handle.equals(handle))
        throw new RuntimeException ("Call handle: " + args.handle +
                                    " does not match session handle: " + handle);

      //LOG.info ("Revoking " + args.revoked.size() + " resources to session: " + args.handle);
      client.revokeResource(args.handle, args.revoked, false);
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

        LOG.warn("Call to session: " + handle + " for call: " + call.getClass().getName() +
                  ", numRetry: " + numRetries + "(retryCountMax=" + retryCountMax + ")" +
                  " failed with TException", e);

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

    return true;
  }

  public void close() {
    if (transport != null) {
      transport.close();
      transport = null;
      client = null;
    }
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
