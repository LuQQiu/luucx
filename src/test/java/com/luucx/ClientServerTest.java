package com.luucx;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.openucx.jucx.UcxCallback;
import org.openucx.jucx.UcxException;
import org.openucx.jucx.UcxUtils;
import org.openucx.jucx.ucp.*;
import org.openucx.jucx.ucs.UcsConstants;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.*;
import org.junit.Ignore;

@Ignore
public class ClientServerTest {

  private static final int SERVER_PORT = 19851;
  private Server server;
  private Client client;
  
  private String serverHost;

  @Before
  public void setup() throws Exception {
    serverHost = InetAddress.getLocalHost().getHostName();
    server = new Server(serverHost, SERVER_PORT);
    new Thread(server::start).start();
    Thread.sleep(1000); // Give the server time to start

    client = new Client();
    
    client.connect(serverHost, SERVER_PORT);
  }

  @After
  public void tearDown() {
    client.close();
    server.stop();
  }

  @Test
  public void testClientServerCommunication() throws Exception {
    String testMessage = "Hello, Server!";
    CountDownLatch messageLatch = new CountDownLatch(1);
    AtomicReference<String> receivedMessage = new AtomicReference<>();

    server.setMessageHandler((clientId, message) -> {
      receivedMessage.set(message);
      messageLatch.countDown();
    });

    client.sendMessage(testMessage);

    assertTrue("Timed out waiting for message", messageLatch.await(5, TimeUnit.SECONDS));
    assertEquals("Server did not receive the correct message", testMessage, receivedMessage.get());
  }

  static class Server {
    private final UcpContext context;
    private final UcpWorker worker;
    private final ConcurrentHashMap<Long, UcpEndpoint> clientEndpoints = new ConcurrentHashMap<>();
    private final AtomicLong nextClientId = new AtomicLong(1);
    private final String host;
    private final int port;
    private volatile boolean running = true;
    private MessageHandler messageHandler;

    private static final byte AM_ID_ASSIGN = 0;
    private static final byte AM_ID_MESSAGE = 1;

    public Server(String serverHost, int port) {
      this.port = port;
      this.host = serverHost;
      this.context = new UcpContext(new UcpParams().requestAmFeature());
      this.worker = context.newWorker(new UcpWorkerParams());
    }

    public void start() {
      try {
        UcpListener listener = worker.newListener(new UcpListenerParams()
            .setSockAddr(new InetSocketAddress(host, port))
            .setConnectionHandler(this::handleNewConnection));

        System.out.println("Server listening on port " + port);

        while (running) {
          try {
            worker.progress();
          } catch (UcxException e) {
            System.out.println("Exception happens at " + System.currentTimeMillis());
          }
          Thread.sleep(10);
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    private void handleNewConnection(UcpConnectionRequest connectionRequest) {
      try {
        long clientId = nextClientId.getAndIncrement();
        UcpEndpoint endpoint = worker.newEndpoint(new UcpEndpointParams()
            .setConnectionRequest(connectionRequest)
            .setPeerErrorHandlingMode());

        clientEndpoints.put(clientId, endpoint);

        ByteBuffer headerBuffer = ByteBuffer.allocateDirect(Long.BYTES).putLong(clientId);
        headerBuffer.flip();

        endpoint.sendAmNonBlocking(AM_ID_ASSIGN,
            UcxUtils.getAddress(headerBuffer), headerBuffer.remaining(),
            0L, 0L,  // No data, ID is in header
            UcpConstants.UCP_AM_FLAG_WHOLE_MSG,
            new UcxCallback() {
              @Override
              public void onSuccess(UcpRequest request) {
                System.out.println("Successfully sent client ID: " + clientId);
              }

              @Override
              public void onError(int ucsStatus, String errorMsg) {
                System.err.println("Failed to send client ID: " + errorMsg);
              }
            });

        System.out.println("New client connected. Assigned ID: " + clientId);
      } catch (UcxException e) {
        e.printStackTrace();
      }
    }
    
    public void stop() {
      running = false;
      clientEndpoints.values().forEach(UcpEndpoint::close);
      worker.close();
      context.close();
    }

    public void setMessageHandler(MessageHandler handler) {
      this.messageHandler = handler;
    }

    interface MessageHandler {
      void onMessage(long clientId, String message);
    }
  }

  static class Client {
    private final UcpContext context;
    private final UcpWorker worker;
    private UcpEndpoint endpoint;
    private final AtomicReference<Long> clientId = new AtomicReference<>(null);
    private final CountDownLatch idReceivedLatch = new CountDownLatch(1);

    private static final byte AM_ID_ASSIGN = 0;
    private static final byte AM_ID_MESSAGE = 1;

    public Client() {
      this.context = new UcpContext(new UcpParams().requestAmFeature());
      this.worker = context.newWorker(new UcpWorkerParams());
      setupActiveMessageHandlers();
    }

    private void setupActiveMessageHandlers() {
      worker.setAmRecvHandler(AM_ID_ASSIGN, (headerAddress, headerSize, amData, replyEp) -> {
        System.out.println("Received AM_ID_ASSIGN message");
        if (amData.isDataValid()) {
          System.out.println("AM data is valid, length: " + amData.getLength());
          if (amData.getLength() == Long.BYTES) {
            ByteBuffer idBuffer = UcxUtils.getByteBufferView(amData.getDataAddress(), Long.BYTES);
            long receivedId = idBuffer.getLong();
            clientId.set(receivedId);
            idReceivedLatch.countDown();
            System.out.println("Received and set client ID: " + receivedId);
          } else {
            System.out.println("Unexpected data length: " + amData.getLength());
          }
        } else {
          System.out.println("AM data is not valid");
        }
        return UcsConstants.STATUS.UCS_OK;
      }, UcpConstants.UCP_AM_FLAG_WHOLE_MSG);
    }

    public void connect(String serverAddress, int serverPort) throws Exception {
        try {
          InetSocketAddress socketAddress = new InetSocketAddress(serverAddress, serverPort);

          if (socketAddress.isUnresolved()) {
            throw new UnknownHostException("Failed to resolve: " + serverAddress);
          }

          System.out.println("Attempting to connect to: " + socketAddress);
          UcpEndpointParams epParams = new UcpEndpointParams()
              .setSocketAddress(socketAddress)
              .setPeerErrorHandlingMode();
          this.endpoint = worker.newEndpoint(epParams);
          System.out.println("Endpoint created, waiting for client ID");

          // Progress the worker to establish the connection and process incoming messages
          long startTime = System.currentTimeMillis();
          int progressCount = 0;
          while (!idReceivedLatch.await(10, TimeUnit.MILLISECONDS)) {
            worker.progress();
            progressCount++;
            if (progressCount % 100 == 0) {
              System.out.println("Still waiting for client ID, progress count: " + progressCount);
            }
            if (System.currentTimeMillis() - startTime > 60000) {
              System.out.println("Timeout waiting for client id at " + System.currentTimeMillis());
              throw new RuntimeException("Timed out waiting for client ID");
            }
          }
          System.out.println("Client connected and received ID: " + clientId.get() + " after " + progressCount + " progress calls");
        } catch (UnknownHostException e) {
          throw new RuntimeException("Failed to connect: " + e.getMessage(), e);
        }
      }


    public void sendMessage(String message) throws Exception {
      Long id = clientId.get();
      if (id == null) {
        throw new IllegalStateException("Client ID not received yet");
      }

      ByteBuffer headerBuffer = ByteBuffer.allocateDirect(Long.BYTES).putLong(id);
      headerBuffer.flip();

      ByteBuffer dataBuffer = ByteBuffer.wrap(message.getBytes());

      endpoint.sendAmNonBlocking(AM_ID_MESSAGE,
          UcxUtils.getAddress(headerBuffer), headerBuffer.remaining(),
          UcxUtils.getAddress(dataBuffer), dataBuffer.remaining(),
          UcpConstants.UCP_AM_FLAG_WHOLE_MSG,
          null);  // No callback
    }

    public void close() {
      if (endpoint != null) {
        endpoint.close();
      }
      worker.close();
      context.close();
    }
  }
}