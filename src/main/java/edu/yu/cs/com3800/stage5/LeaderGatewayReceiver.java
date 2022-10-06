package edu.yu.cs.com3800.stage5;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.yu.cs.com3800.LoggingServer;
import edu.yu.cs.com3800.Message;
import edu.yu.cs.com3800.Util;

public class LeaderGatewayReceiver extends Thread implements LoggingServer {
    private Map<Long, Socket> pendingResponses;
    private LinkedBlockingQueue<Message> incomingMessages;
    private Logger logger;
    private Map<Long, Message> requestMessageMap;
    private ServerSocket serverSocket;

    public LeaderGatewayReceiver(Map<Long, Socket> pendingResponses, LinkedBlockingQueue<Message> incomingMessages,
            ZooKeeperPeerServerImpl leader, Map<Long, Message> requestMessageMap, ServerSocket serverSocket) {
        this.logger = initializeLogging(LeaderGatewayReceiver.class.getName(), false);
        this.pendingResponses = pendingResponses;
        this.incomingMessages = incomingMessages;
        this.requestMessageMap = requestMessageMap;
        this.serverSocket = serverSocket;
        try {
            serverSocket.setSoTimeout(0);
        } catch (SocketException e) {
            logger.severe("Could not set socket accept timeout");
        }
        setDaemon(true);
    }

    @Override
    public void run() {
        Socket socket = null;
        logger.info("Attempting to create serverSocket to listen for gateway connections");
        try {
            while (!isInterrupted()) {
                logger.fine("Attempting to accept a gatewayConnection");
                socket = serverSocket.accept();
                logger.fine("Success. Attempting to read gatewayRequest");
                Message request = new Message(Util.readAllBytesFromNetwork(socket.getInputStream()));
                logger.info("Received message from " + request.getSenderHost()
                        + request.getSenderPort());
                pendingResponses.put(request.getRequestID(), socket);
                requestMessageMap.put(request.getRequestID(), request);
                logger.fine("Success inserting request ID and socket to map");
                boolean done = false;
                logger.fine("attempting to insert request in leader receiver queue");
                while (!done) {
                    done = incomingMessages.offer(request);
                }
                logger.info("successfully passed message to leader");
            }
        } catch (IOException e) {
            logger.log(Level.SEVERE, "Failed to initialize sockets correctly, see exception for details", e);
        } finally {
            if (serverSocket != null) {
                try {
                    serverSocket.close();
                } catch (IOException e) {
                    logger.severe(
                            "Failed to close serversocket upon thread exit, if promoted to leader, got a problem");
                }
            }

            /*if (socket != null) {
                try {
                    socket.close();
                } catch (IOException e) {
                    logger.severe("Failed to close gateway socket upon thread exit");
                }
            }*/
        }
    }
}