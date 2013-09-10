#!/usr/local/bin/thrift -java

namespace java org.apache.hadoop.hdfs.notifier
namespace php hdfsnotifier
namespace py hdfsnotifier

/**
 * The notification sent from the server to the client when a namespace
 * event occured.
 */
struct NamespaceNotification {
  1: string path,
  2: byte type,
  3: i64 txId
}

/**
 * Emitted when an operation in the HDFS namespace occured. If a client
 * subscribed to the server for a particular event, a notification 
 * (see NamespaceNotification) will be sent.
 */
struct NamespaceEvent {
  1: string path,
  2: byte type
}

// Raised by the server when receiving commands from an unknown client
exception InvalidClientIdException {
  1: string message
}

// Raised by the client when receiving commands/notifications from an
// unknown server
exception InvalidServerIdException {
  1: string message
}

// Raised by the client when he receives the registerServer command from
// another server than he was expecting.
exception InvalidTokenException {
  1: string message
}

// Raised by the server when the client wants to receive notifications
// that are too old (they aren't buffered anymore by the server)
exception TransactionIdTooOldException {
  1: string message
}

// Raised by the server when he isn't ready to give notifications yet
exception RampUpException {
  1: string message
}

// Raised by the server when he failed to connect to the client
exception ClientConnectionException {
  1: string message
}

// Raised by the server when the client tries to unsubscribe, though
// he didn't subscribed earlier
exception ClientNotSubscribedException {
  1: string message
}


service ClientHandler
{
  // Called by the server to raise a notification
  void handleNotification(1:NamespaceNotification notification, 2:string serverId)
      throws (1:InvalidServerIdException invalidServerId),

  // Called periodically by the server to inform the client that he is
  // still alive
  void heartbeat(1:string serverId)
      throws (1:InvalidServerIdException invalidServerId),

  // Called by the server to associate the client with its id
  void registerServer(1:i64 clientId, 2:string serverId, 3:i64 token)
      throws (1:InvalidTokenException invalidToken),
}


service ServerHandler
{
  void subscribe(1:i64 clientId, 2:NamespaceEvent subscribedEvent, 3:i64 txId)
      throws (1:InvalidClientIdException invalidClientId,
              2:TransactionIdTooOldException transactionIdTooOld),

  void unsubscribe(1:i64 clientId, 2:NamespaceEvent subscribedEvent)
      throws (1:InvalidClientIdException invalidClientId,
              2:ClientNotSubscribedException clientNotSubscribed),

  void registerClient(1:string host, 2:i32 port, 3:i64 token)
      throws (1:RampUpException rampUp,
              2:ClientConnectionException clientConnection),

  void unregisterClient(1:i64 clientId)
      throws (1:InvalidClientIdException invalidClientId),
}
