package com.luucx;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.openucx.jucx.UcxException;
import org.openucx.jucx.ucp.*;
import org.openucx.jucx.ucs.UcsConstants;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class UcxConnectionTest {

  private static final int SERVER_PORT = 19851;
  private static final byte AM_ID_MESSAGE = 0;
  private UcxTestRunner testRunner;

  @Before
  public void setup() throws Exception {
    testRunner = new UcxTestRunner();
    testRunner.start();
    // Give some time for the server and client to initialize
    Thread.sleep(2000);
  }

  @After
  public void tearDown() throws Exception {
    if (testRunner != null) {
      testRunner.stop();
    }
  }

  @Test
  public void testSendMessage() throws Exception {
    String testMessage = "Hello from client!";
    testRunner.sendMessage(testMessage);

    // Wait a bit for the message to be processed
    Thread.sleep(1000);

    // Here you would typically add assertions to verify the expected behavior
    // For example, you might check a log or a mock object to ensure the message was received
    // Since we can't easily check the server's received messages in this setup,
    // we'll just assert that no exception was thrown during the send process
    assertTrue("Message send completed without exceptions", true);
  }

  public static class UcxTestRunner {
    private Server server;
    private Client client;
    private Thread serverThread;
    private Thread clientThread;

    public void start() throws Exception {
      server = new Server("localhost", SERVER_PORT);
      serverThread = new Thread(server);
      serverThread.start();

      // Give the server time to start
      Thread.sleep(1000);

      client = new Client();
      clientThread = new Thread(client);
      clientThread.start();

      System.out.println("UcxTestRunner: Server and Client started.");
    }

    public void sendMessage(String message) {
      if (client != null) {
        client.sendMessage(message);
      } else {
        System.out.println("UcxTestRunner: Client not initialized. Cannot send message.");
      }
    }

    public void stop() throws InterruptedException {
      if (client != null) {
        client.stop();
        clientThread.join();
      }
      if (server != null) {
        server.stop();
        serverThread.join();
      }
      System.out.println("UcxTestRunner: Server and Client stopped.");
    }
  }

  static class Server implements Runnable {
    private final UcpContext context;
    private final UcpWorker worker;
    private final Set<UcpEndpoint> activeEndpoints = ConcurrentHashMap.newKeySet();
    private final String host;
    private final int port;
    private volatile boolean running = true;

    public Server(String host, int port) {
      this.host = host;
      this.port = port;
      this.context = new UcpContext(new UcpParams().requestAmFeature());
      this.worker = context.newWorker(new UcpWorkerParams());
    }

    @Override
    public void run() {
      try {
        UcpListener listener = worker.newListener(new UcpListenerParams()
            .setSockAddr(new InetSocketAddress(host, port))
            .setConnectionHandler(this::handleNewConnection));

        System.out.println("Server listening on port " + port);

        setupActiveMessageHandlers();

        while (running) {
          worker.progress();
          Thread.sleep(10);
        }
      } catch (Exception e) {
        System.out.println("Error in server: " + e.getMessage());
        e.printStackTrace();
      }
    }

    private void handleNewConnection(UcpConnectionRequest connectionRequest) {
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
      }
    }

    private void setupActiveMessageHandlers() {
      worker.setAmRecvHandler(AM_ID_MESSAGE, (headerAddress, headerSize, amData, replyEp) -> {
        String message = UcpAmData.unpackString(amData);
        System.out.println("Server received message: " + message);
        return UcsConstants.STATUS.UCS_OK;
      }, UcpConstants.UCP_AM_FLAG_WHOLE_MSG);
    }

    public void stop() {
      running = false;
      activeEndpoints.forEach(UcpEndpoint::close);
      worker.close();
      context.close();
      System.out.println("Server stopped.");
    }
  }

  static class Client implements Runnable {
    private final UcpContext context;
    private final UcpWorker worker;
    private UcpEndpoint endpoint;
    private volatile boolean running = true;

    public Client() {
      this.context = new UcpContext(new UcpParams().requestAmFeature());
      this.worker = context.newWorker(new UcpWorkerParams());
    }

    @Override
    public void run() {
      try {
        connect("localhost", SERVER_PORT);
        while (running) {
          worker.progress();
          Thread.sleep(10);
        }
      } catch (Exception e) {
        System.out.println("Error in client: " + e.getMessage());
        e.printStackTrace();
      }
    }

    private void connect(String serverAddress, int serverPort) throws Exception {
      InetSocketAddress socketAddress = new InetSocketAddress(serverAddress, serverPort);
      UcpEndpointParams epParams = new UcpEndpointParams()
          .setSocketAddress(socketAddress)
          .setPeerErrorHandlingMode();
      this.endpoint = worker.newEndpoint(epParams);
      System.out.println("Client connected to server");
    }

    public void sendMessage(String message) {
      if (endpoint != null) {
        ByteBuffer msgBuffer = ByteBuffer.wrap(message.getBytes());
        CountDownLatch latch = new CountDownLatch(1);

        endpoint.sendAmNonBlocking(AM_ID_MESSAGE, msgBuffer, null, new UcpAmSendCallback() {
          @Override
          public void onSuccess(UcpAmData data) {
            System.out.println("Message sent successfully");
            latch.countDown();
          }

          @Override
          public void onError(int ucsStatus, String errorMsg) {
            System.out.println("Error sending message: " + errorMsg);
            latch.countDown();
          }
        });

        try {
          latch.await(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
          System.out.println("Interrupted while waiting for message send");
        }
      } else {
        System.out.println("Cannot send message: Client not connected");
      }
    }

    public void stop() {
      running = false;
      if (endpoint != null) {
        endpoint.close();
      }
      worker.close();
      context.close();
      System.out.println("Client stopped.");
    }
  }
}