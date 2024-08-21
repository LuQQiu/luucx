package com.luucx;

import org.junit.Test;
import org.openucx.jucx.UcxException;
import org.openucx.jucx.ucp.*;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.*;

public class UcxBidirectionalEndpointTest {

    @Test
    public void testBidirectionalEndpointCreation() throws Exception {
        // Create contexts and workers
        UcpContext workerContext = new UcpContext(new UcpParams().requestTagFeature());
        UcpContext clientContext = new UcpContext(new UcpParams().requestTagFeature());

        UcpWorker worker = workerContext.newWorker(new UcpWorkerParams());
        UcpWorker clientWorker = clientContext.newWorker(new UcpWorkerParams());

        // Get worker's address (simulates ETCD storage and retrieval)
        ByteBuffer workerAddress = worker.getAddress();

        // Set up connection handler for the worker
        AtomicReference<UcpEndpoint> workerToClientEndpoint = new AtomicReference<>();
        UcpListenerParams listenerParams = new UcpListenerParams()
            .setConnectionHandler(connectionRequest -> {
                try {
                    workerToClientEndpoint.set(worker.newEndpoint(new UcpEndpointParams()
                        .setConnectionRequest(connectionRequest)
                        .setPeerErrorHandlingMode()));
                    System.out.println("Worker created endpoint to client");
                } catch (UcxException e) {
                    e.printStackTrace();
                }
            });
        UcpListener listener = worker.newListener(listenerParams);

        // Client creates endpoint to worker
        UcpEndpoint clientToWorkerEndpoint = clientWorker.newEndpoint(new UcpEndpointParams()
            .setUcpAddress(workerAddress)
            .setPeerErrorHandlingMode());
        System.out.println("Client created endpoint to worker");

        // Wait for the worker to create its endpoint
        while (workerToClientEndpoint.get() == null) {
            worker.progress();
            clientWorker.progress();
        }

        // Test bidirectional communication
        ByteBuffer sendBuffer = ByteBuffer.allocateDirect(1024);
        ByteBuffer recvBuffer = ByteBuffer.allocateDirect(1024);

        // Client sends to worker
        sendBuffer.putInt(0, 42);
        clientToWorkerEndpoint.sendTaggedNonBlocking(sendBuffer, 1, null);
        UcpRequest recvRequest = worker.recvTaggedNonBlocking(recvBuffer, 1, 0, null);
        
        while (!recvRequest.isCompleted()) {
            worker.progress();
            clientWorker.progress();
        }
        assertEquals(42, recvBuffer.getInt(0));
        System.out.println("Worker received message from client: " + recvBuffer.getInt(0));

        // Worker sends to client
        sendBuffer.putInt(0, 24);
        workerToClientEndpoint.get().sendTaggedNonBlocking(sendBuffer, 2, null);
        recvRequest = clientWorker.recvTaggedNonBlocking(recvBuffer, 2, 0, null);
        
        while (!recvRequest.isCompleted()) {
            worker.progress();
            clientWorker.progress();
        }
        assertEquals(24, recvBuffer.getInt(0));
        System.out.println("Client received message from worker: " + recvBuffer.getInt(0));

        // Clean up
        clientToWorkerEndpoint.close();
        workerToClientEndpoint.get().close();
        listener.close();
        worker.close();
        clientWorker.close();
        workerContext.close();
        clientContext.close();
    }
}