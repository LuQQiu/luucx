package com.luucx;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.openucx.jucx.UcxException;
import org.openucx.jucx.UcxUtils;
import org.openucx.jucx.ucp.*;
import org.openucx.jucx.ucs.UcsConstants;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.*;

public class UcxConnectionTest {

    private static final int SERVER_PORT = 19851;
    private static final byte AM_ID_CONNECTION = 0;
    private static final byte AM_ID_REQUEST = 1;
    private static final byte AM_ID_RESPONSE = 2;
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
    public void testConnectionAndDataExchange() throws Exception {
        // Wait for the connection to be established
        Thread.sleep(3000);

        assertTrue("Client should be connected", testRunner.client.isConnected());

        testRunner.client.sendDataRequest();

        // Wait for the response
        Thread.sleep(1000);

        assertTrue("Client should have received a response", testRunner.client.hasReceivedResponse());
    }

    public static class UcxTestRunner {
        Server server;
        Client client;
        private Thread serverThread;
        private Thread clientThread;

        public void start() throws Exception {
            server = new Server(InetAddress.getLocalHost().getHostName(), SERVER_PORT);
            serverThread = new Thread(server);
            serverThread.start();

            // Give the server time to start
            Thread.sleep(1000);

            client = new Client();
            clientThread = new Thread(client);
            clientThread.start();

            System.out.println("UcxTestRunner: Server and Client started.");
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
        private final Map<String, UcpEndpoint> activeEndpoints = new ConcurrentHashMap<>();
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
                            activeEndpoints.values().remove(ep);
                        }));

                String clientAddress = connectionRequest.getClientAddress().toString();
                activeEndpoints.put(clientAddress, endpoint);
                System.out.println("New client connected from " + clientAddress + ". Active endpoints: " + activeEndpoints.size());

                // Send connection success message
                String successMessage = "Connection successful from " + host;
                ByteBuffer msgBuffer = ByteBuffer.wrap(successMessage.getBytes());
                long dataAddress = UcxUtils.getAddress(msgBuffer);
                endpoint.sendAmNonBlocking(AM_ID_CONNECTION, 0, 0, dataAddress, msgBuffer.remaining(), 0, null);
            } catch (UcxException e) {
                System.out.println("Error handling new connection: " + e.getMessage());
            }
        }

        private void setupActiveMessageHandlers() {
            worker.setAmRecvHandler(AM_ID_REQUEST, (headerAddress, headerSize, amData, replyEp) -> {
                long dataAddress = amData.getDataAddress();
                long dataLength = amData.getLength();
                ByteBuffer buffer = UcxUtils.getByteBufferView(dataAddress, (int)dataLength);
                String message = new String(buffer.array());
                System.out.println("Server received request: " + message);

                // Send response
                String response = "response";
                ByteBuffer responseBuffer = ByteBuffer.wrap(response.getBytes());
                long responseAddress = UcxUtils.getAddress(responseBuffer);
                replyEp.sendAmNonBlocking(AM_ID_RESPONSE, 0, 0, responseAddress, responseBuffer.remaining(), 0, null);

                return UcsConstants.STATUS.UCS_OK;
            }, UcpConstants.UCP_AM_FLAG_WHOLE_MSG);
        }

        public void stop() {
            running = false;
            activeEndpoints.values().forEach(UcpEndpoint::close);
            worker.close();
            context.close();
            System.out.println("Server stopped.");
        }
    }

    static class Client implements Runnable {
        private final UcpContext context;
        private final UcpWorker worker;
        private final Map<String, EndpointInfo> endpoints = new ConcurrentHashMap<>();
        private volatile boolean running = true;
        private final AtomicBoolean receivedResponse = new AtomicBoolean(false);

        public Client() {
            this.context = new UcpContext(new UcpParams().requestAmFeature());
            this.worker = context.newWorker(new UcpWorkerParams());
        }

        @Override
        public void run() {
            try {
                connect(InetAddress.getLocalHost().getHostName(), SERVER_PORT);
                setupActiveMessageHandlers();
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
            UcpEndpoint endpoint = worker.newEndpoint(epParams);
            endpoints.put(serverAddress, new EndpointInfo(endpoint, ConnectionStatus.CONNECTING));
            System.out.println("Client connecting to server");
        }

        private void setupActiveMessageHandlers() {
            worker.setAmRecvHandler(AM_ID_CONNECTION, (headerAddress, headerSize, amData, replyEp) -> {
                long dataAddress = amData.getDataAddress();
                long dataLength = amData.getLength();
                ByteBuffer buffer = UcxUtils.getByteBufferView(dataAddress, (int)dataLength);
                String message = new String(buffer.array());
                System.out.println("Client received connection confirmation: " + message);

                String serverAddress = message.split("from ")[1];
                EndpointInfo info = endpoints.get(serverAddress);
                if (info != null) {
                    info.status = ConnectionStatus.CONNECTED;
                }

                return UcsConstants.STATUS.UCS_OK;
            }, UcpConstants.UCP_AM_FLAG_WHOLE_MSG);

            worker.setAmRecvHandler(AM_ID_RESPONSE, (headerAddress, headerSize, amData, replyEp) -> {
                long dataAddress = amData.getDataAddress();
                long dataLength = amData.getLength();
                ByteBuffer buffer = UcxUtils.getByteBufferView(dataAddress, (int)dataLength);
                String message = new String(buffer.array());
                System.out.println("Client received response: " + message);
                receivedResponse.set(true);
                return UcsConstants.STATUS.UCS_OK;
            }, UcpConstants.UCP_AM_FLAG_WHOLE_MSG);
        }

        public void sendDataRequest() throws UnknownHostException {
            String serverAddress = InetAddress.getLocalHost().getHostName();
            EndpointInfo info = endpoints.get(serverAddress);
            if (info != null && info.status == ConnectionStatus.CONNECTED) {
                String request = "request";
                ByteBuffer msgBuffer = ByteBuffer.wrap(request.getBytes());
                long dataAddress = UcxUtils.getAddress(msgBuffer);
                info.endpoint.sendAmNonBlocking(AM_ID_REQUEST, 0, 0, dataAddress, msgBuffer.remaining(), 0, null);
                System.out.println("Client sent data request");
            } else {
                System.out.println("Cannot send request: Client not connected");
            }
        }

        public boolean isConnected() {
            return endpoints.values().stream()
                    .anyMatch(info -> info.status == ConnectionStatus.CONNECTED);
        }

        public boolean hasReceivedResponse() {
            return receivedResponse.get();
        }

        public void stop() {
            running = false;
            endpoints.values().forEach(info -> info.endpoint.close());
            worker.close();
            context.close();
            System.out.println("Client stopped.");
        }

        private enum ConnectionStatus {
            CONNECTING, CONNECTED
        }

        private static class EndpointInfo {
            UcpEndpoint endpoint;
            ConnectionStatus status;

            EndpointInfo(UcpEndpoint endpoint, ConnectionStatus status) {
                this.endpoint = endpoint;
                this.status = status;
            }
        }
    }
}