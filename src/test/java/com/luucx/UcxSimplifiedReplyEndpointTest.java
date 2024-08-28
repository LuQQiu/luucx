package com.luucx;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.openucx.jucx.UcxCallback;
import org.openucx.jucx.UcxException;
import org.openucx.jucx.UcxUtils;
import org.openucx.jucx.ucp.*;
import org.openucx.jucx.ucs.UcsConstants;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.*;

@Ignore
public class UcxSimplifiedReplyEndpointTest {

  private static final int SERVER_PORT = 19851;
  private Server server;
  private Client client;
  private String serverHost;

  @Before
  public void setup() throws Exception {
    System.out.println("Setting up test...");
    serverHost = InetAddress.getLocalHost().getHostName();
    server = new Server(serverHost, SERVER_PORT);
    new Thread(server::start).start();
    Thread.sleep(1000); // Give the server time to start

    client = new Client();
    client.connect(serverHost, SERVER_PORT);
    System.out.println("Test setup complete.");
  }
@After
public void tearDown() {
  System.out.println("Tearing down test...");
  if (client != null) {
    client.close();
  }
  if (server != null) {
    server.stop();
  }
  System.out.println("Test tear down complete.");
}

@Test
public void testSimpleConnection() throws Exception {
  System.out.println("Starting simple connection test...");
  // Just test that we can connect without sending any messages
  Thread.sleep(2000); // Wait a bit to ensure connection is established
  System.out.println("Simple connection test complete.");
}

static class Server {
  private final UcpContext context;
  private final UcpWorker worker;
  private final Set<UcpEndpoint> activeEndpoints = ConcurrentHashMap.newKeySet();
  private final String host;
  private final int port;
  private volatile boolean running = true;

  private static final byte AM_ID_MESSAGE = 0;

  public Server(String serverHost, int port) {
    System.out.println("Initializing server...");
    this.port = port;
    this.host = serverHost;
    this.context = new UcpContext(new UcpParams().requestAmFeature());
    this.worker = context.newWorker(new UcpWorkerParams());
    System.out.println("Server initialized.");
  }

public void start() {
  System.out.println("Starting server...");
  try {
    UcpListener listener = worker.newListener(new UcpListenerParams()
        .setSockAddr(new InetSocketAddress(host, port))
        .setConnectionHandler(this::handleNewConnection));

    System.out.println("Server listening on port " + port);

    setupActiveMessageHandlers();

    while (running) {
      try {
        worker.progress();
      } catch (UcxException e) {
        System.out.println("Exception in worker progress: " + e.getMessage());
      }
      Thread.sleep(10);
    }
  } catch (Exception e) {
    System.out.println("Error in server: " + e.getMessage());
    e.printStackTrace();
  }
}

private void handleNewConnection(UcpConnectionRequest connectionRequest) {
  System.out.println("Handling new connection...");
  try {
    UcpEndpoint endpoint = worker.newEndpoint(new UcpEndpointParams()
        .setConnectionRequest(connectionRequest)
        .setPeerErrorHandlingMode()
        .setErrorHandler((ep, status, errorMsg) -> {
          System.out.println("Endpoint error: " + errorMsg);
          activeEndpoints.remove(ep);
        }));

    activeEndpoints.add(endpoint);
    System.out.println("New client connected. Active endpoints: " + activeEndpoints.size());
  } catch (UcxException e) {
    System.out.println("Error handling new connection: " + e.getMessage());
    e.printStackTrace();
  }
}

private void setupActiveMessageHandlers() {
  System.out.println("Setting up active message handlers...");
  worker.setAmRecvHandler(AM_ID_MESSAGE, (headerAddress, headerSize, amData, replyEp) -> {
    System.out.println("Received message on server");
    return UcsConstants.STATUS.UCS_OK;
  }, UcpConstants.UCP_AM_FLAG_WHOLE_MSG);
}

public void stop() {
  System.out.println("Stopping server...");
  running = false;
  activeEndpoints.forEach(UcpEndpoint::close);
  worker.close();
  context.close();
  System.out.println("Server stopped.");
}
}

static class Client {
private final UcpContext context;
private final UcpWorker worker;
private UcpEndpoint endpoint;
private static final byte AM_ID_MESSAGE = 0;

public Client() {
  System.out.println("Initializing client...");
  this.context = new UcpContext(new UcpParams().requestAmFeature());
  this.worker = context.newWorker(new UcpWorkerParams());
  setupActiveMessageHandlers();
  System.out.println("Client initialized.");
}

private void setupActiveMessageHandlers() {
  System.out.println("Setting up client active message handlers...");
  worker.setAmRecvHandler(AM_ID_MESSAGE, (headerAddress, headerSize, amData, replyEp) -> {
    System.out.println("Received message on client");
    return UcsConstants.STATUS.UCS_OK;
  }, UcpConstants.UCP_AM_FLAG_WHOLE_MSG);
}

public void connect(String serverAddress, int serverPort) throws Exception {
  System.out.println("Client connecting to server...");
  InetSocketAddress socketAddress = new InetSocketAddress(serverAddress, serverPort);
  UcpEndpointParams epParams = new UcpEndpointParams()
      .setSocketAddress(socketAddress)
      .setPeerErrorHandlingMode();
  this.endpoint = worker.newEndpoint(epParams);

  System.out.println("Client connected to server");

  // Start a worker progress thread
  new Thread(() -> {
    while (!Thread.currentThread().isInterrupted()) {
      try {
        worker.progress();
        Thread.sleep(10);
      } catch (Exception e) {
        System.out.println("Error in client worker thread: " + e.getMessage());
        e.printStackTrace();
      }
    }
  }).start();
}

public void close() {
  System.out.println("Closing client...");
  if (endpoint != null) {
    endpoint.close();
  }
  worker.close();
  context.close();
  System.out.println("Client closed.");
}
}
}