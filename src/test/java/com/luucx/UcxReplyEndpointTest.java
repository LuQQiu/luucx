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
public class UcxReplyEndpointTest {

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
  public void testBidirectionalCommunication() throws Exception {
    String clientMessage = "Hello, Server!";
    String expectedServerResponse = "Message received: " + clientMessage;
    CountDownLatch serverReceivedLatch = new CountDownLatch(1);
    CountDownLatch clientReceivedLatch = new CountDownLatch(1);
    AtomicReference<String> serverReceivedMessage = new AtomicReference<>();
    AtomicReference<String> clientReceivedMessage = new AtomicReference<>();

    server.setMessageHandler((replyEp, message) -> {
      serverReceivedMessage.set(message);
      serverReceivedLatch.countDown();
    });

    client.setMessageHandler((message) -> {
      clientReceivedMessage.set(message);
      clientReceivedLatch.countDown();
    });

    client.sendMessage(clientMessage);

    assertTrue("Server didn't receive the message in time", serverReceivedLatch.await(5, TimeUnit.SECONDS));
    assertEquals("Server received incorrect message", clientMessage, serverReceivedMessage.get());

    assertTrue("Client didn't receive the response in time", clientReceivedLatch.await(5, TimeUnit.SECONDS));
    assertEquals("Client received incorrect response", expectedServerResponse, clientReceivedMessage.get());
  }

  static class Server {
    private final UcpContext context;
    private final UcpWorker worker;
    private final Set<UcpEndpoint> activeEndpoints = ConcurrentHashMap.newKeySet();
    private final String host;
    private final int port;
    private volatile boolean running = true;
    private MessageHandler messageHandler;

    private static final byte AM_ID_MESSAGE = 0;

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
        e.printStackTrace();
      }
    }

    private void setupActiveMessageHandlers() {
      worker.setAmRecvHandler(AM_ID_MESSAGE, (headerAddress, headerSize, amData, replyEp) -> {
        if (amData.isDataValid()) {
          ByteBuffer dataBuffer = UcxUtils.getByteBufferView(amData.getDataAddress(), (int)amData.getLength());
          String message = new String(dataBuffer.array(), dataBuffer.position(), dataBuffer.remaining());
          if (messageHandler != null) {
            messageHandler.onMessage(replyEp, message);
          }

          // Send a response
          String response = "Message received: " + message;
          ByteBuffer responseBuffer = ByteBuffer.wrap(response.getBytes());
          replyEp.sendAmNonBlocking(AM_ID_MESSAGE,
              0L, 0L, // No header
              UcxUtils.getAddress(responseBuffer), responseBuffer.remaining(),
              UcpConstants.UCP_AM_FLAG_WHOLE_MSG,
              null);
        }
        return UcsConstants.STATUS.UCS_OK;
      }, UcpConstants.UCP_AM_FLAG_WHOLE_MSG);
    }

    public void stop() {
      running = false;
      activeEndpoints.forEach(UcpEndpoint::close);
      worker.close();
      context.close();
    }

    public void setMessageHandler(MessageHandler handler) {
      this.messageHandler = handler;
    }

    interface MessageHandler {
      void onMessage(UcpEndpoint replyEp, String message);
    }
  }

  static class Client {
    private final UcpContext context;
    private final UcpWorker worker;
    private UcpEndpoint endpoint;
    private MessageHandler messageHandler;
    private static final byte AM_ID_MESSAGE = 0;

    public Client() {
      this.context = new UcpContext(new UcpParams().requestAmFeature());
      this.worker = context.newWorker(new UcpWorkerParams());
      setupActiveMessageHandlers();
    }

    private void setupActiveMessageHandlers() {
      worker.setAmRecvHandler(AM_ID_MESSAGE, (headerAddress, headerSize, amData, replyEp) -> {
        if (amData.isDataValid()) {
          ByteBuffer dataBuffer = UcxUtils.getByteBufferView(amData.getDataAddress(), (int)amData.getLength());
          String message = new String(dataBuffer.array(), dataBuffer.position(), dataBuffer.remaining());
          if (messageHandler != null) {
            messageHandler.onMessage(message);
          }
        }
        return UcsConstants.STATUS.UCS_OK;
      }, UcpConstants.UCP_AM_FLAG_WHOLE_MSG);
    }

    public void connect(String serverAddress, int serverPort) throws Exception {
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
            e.printStackTrace();
          }
        }
      }).start();
    }

    public void sendMessage(String message) throws Exception {
      ByteBuffer dataBuffer = ByteBuffer.wrap(message.getBytes());
      endpoint.sendAmNonBlocking(AM_ID_MESSAGE,
          0L, 0L, // No header
          UcxUtils.getAddress(dataBuffer), dataBuffer.remaining(),
          UcpConstants.UCP_AM_FLAG_WHOLE_MSG,
          new UcxCallback() {
            @Override
            public void onSuccess(UcpRequest request) {
              System.out.println("Message sent successfully");
            }
            @Override
            public void onError(int ucsStatus, String errorMsg) {
              System.err.println("Failed to send message: " + errorMsg);
            }
          });
    }

    public void setMessageHandler(MessageHandler handler) {
      this.messageHandler = handler;
    }

    public void close() {
      if (endpoint != null) {
        endpoint.close();
      }
      worker.close();
      context.close();
    }

    interface MessageHandler {
      void onMessage(String message);
    }
  }
}