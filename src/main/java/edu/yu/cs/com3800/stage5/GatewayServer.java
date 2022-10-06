package edu.yu.cs.com3800.stage5;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import edu.yu.cs.com3800.LoggingServer;
import edu.yu.cs.com3800.Message;
import edu.yu.cs.com3800.Message.MessageType;
import edu.yu.cs.com3800.stage5.GossipThread.GossipEntry;
import edu.yu.cs.com3800.Util;

public class GatewayServer extends Thread implements LoggingServer {
    private HttpServer http;
    private Logger logger;
    private volatile InetSocketAddress leader;
    private Map<Long, InetSocketAddress> peerIDtoAddress;
    private AtomicLong requestId;
    private int observerCount;
    private InetSocketAddress myAddress;
    private GatewayPeerServerImpl peerServer;

    public GatewayServer(String host, int port, Map<Long, InetSocketAddress> peerIDtoAddress, int observerCount) {
        this.logger = initializeLogging(GatewayServer.class.getName() + "-port-" + port, false);
        this.peerIDtoAddress = peerIDtoAddress;
        this.leader = null;
        this.requestId = new AtomicLong(1);
        this.observerCount = observerCount;
        this.myAddress = new InetSocketAddress(host, port);
        this.peerServer = new GatewayPeerServerImpl(port + 2, -1L, -1L, peerIDtoAddress,
                this.observerCount);
        peerServer.start();
        try {
            this.http = HttpServer.create(new InetSocketAddress(port), 0);
        } catch (IOException e) {
            logger.log(Level.SEVERE, "Could not create httpserver ", e);
        }
        this.http.setExecutor(Executors.newCachedThreadPool());
        this.http.createContext("/compileandrun", new ServerHttpHandler());
        this.http.createContext("/getclusterdata",new ClusterDataHandler());
        this.http.start();

    }

    private class ClusterDataHandler implements HttpHandler{

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            Map<Long, GossipEntry> gossipMap = peerServer.getGossipMap();
            StringBuilder sb = new StringBuilder();
            sb.append("Cluster Data:\n");
            boolean allLooking = false;
            InetSocketAddress currentLeader = leader;
            if(currentLeader==null){
                allLooking=true;
            }
            for(GossipEntry ge : gossipMap.values()){
                if(ge.isFailed() || ge.getId()<0){
                    continue;
                }
                sb.append("<ID: " + ge.getId()+", State: ");
                if(allLooking){
                    sb.append("LOOKING");
                }
                else if(peerServer.getAddressByPeerID(currentLeader)==ge.getId()){
                    sb.append("LEADING");
                }
                else{
                    sb.append("FOLLOWING");
                }
                sb.append(">\n");
            }
            sb.append("\n");
            exchange.sendResponseHeaders(200, sb.toString().getBytes().length);
            exchange.getResponseBody().write(sb.toString().getBytes());
        }
        
    }

    private InetSocketAddress getAddress() {
        return this.myAddress;
    }

    public void shutdown() {
        logger.severe("Shutdown Called");
        this.http.stop(0);
        this.interrupt();
    }

    @Override
    public void run() {
        while (!isInterrupted()) {
            if (leader == null) {
                while (peerServer.getCurrentLeader() == null)
                    ;
                this.leader = peerIDtoAddress.get(peerServer.getCurrentLeader().getProposedLeaderID());
            }
            if (peerServer.isPeerDead(leader)) {
                leader = null;
            }
        }
        peerServer.shutdown();
        logger.severe("Exiting run method");
    }

    public class ServerHttpHandler implements HttpHandler {
        ThreadLocal<Logger> local = new ThreadLocal<Logger>() {
            @Override
            protected Logger initialValue() {
                return initializeLogging(
                        ServerHttpHandler.class.getName() + "-name-" + Thread.currentThread().getName(),
                        false);
            }
        };

        @Override
        public void handle(HttpExchange exchange) {
            Logger localLogger = local.get();
            localLogger.log(Level.INFO,
                    "Server received request on " + exchange.getHttpContext().getPath() + " from port "
                            + exchange.getRemoteAddress().getPort());

            Map<String, List<String>> headers = exchange.getRequestHeaders();
            byte[] requestBody;
            try {
                requestBody = exchange.getRequestBody().readAllBytes();
            } catch (IOException e2) {
                localLogger.log(Level.SEVERE, "could not get request body", e2);
                try {
                    exchange.sendResponseHeaders(500, -1);
                } catch (IOException e) {
                    localLogger.log(Level.SEVERE, "Could not open send 500 to client", e);
                }
                return;
            }
            localLogger.log(Level.INFO, "Request body: " + (new String(requestBody)));
            List<String> contentTypeHeader = headers.get("Content-Type");

            if (contentTypeHeader == null || contentTypeHeader.size() < 1
                    || !contentTypeHeader.get(0).equals("text/x-java-source")) {
                localLogger.info("Content type was incorrect, sending 400 to client");
                try {
                    exchange.sendResponseHeaders(400, -1);
                } catch (IOException e) {
                    localLogger.log(Level.SEVERE, "Could not open send 400 to client", e);
                }
                return;
            }
            localLogger.info("leader is " + leader);

            // sendand get response;
            long currentRequestID = requestId.getAndIncrement();
            byte[] response = sendAndGetResponse(currentRequestID, requestBody);
            Message responseMessage = new Message(response);
            localLogger.info("Got response for " + responseMessage.getRequestID());
            int responseCode = responseMessage.getErrorOccurred() ? 400 : 200;
            int responseLength = responseMessage.getMessageContents().length;
            localLogger.info("Attempting to send response headers for requestID " + currentRequestID);
            try {
                localLogger.info("Response headers should be: " + responseCode + " " + responseLength);
                exchange.sendResponseHeaders(responseCode, responseLength);
            } catch (IOException e) {
                localLogger.log(Level.SEVERE, "Could not send response headers to client, sending 500 to client", e);
                try {
                    exchange.sendResponseHeaders(500, -1);
                } catch (IOException e1) {
                    localLogger.log(Level.SEVERE, "Could not send 500 code to client", e1);
                }
                return;
            }
            localLogger.info("Attempting to write response body for requestID: " + currentRequestID);
            try {
                localLogger.info("Response headers should be: " + new String(responseMessage.getMessageContents()));
                exchange.getResponseBody().write(responseMessage.getMessageContents());
            } catch (IOException e) {
                localLogger.log(Level.SEVERE, "Could not write response body to client, sending 500 to client", e);
                try {
                    exchange.sendResponseHeaders(500, -1);
                } catch (IOException e1) {
                    localLogger.log(Level.SEVERE, "Could not send 500 code to client", e1);
                }
                return;
            }

            exchange.close();

            localLogger.log(Level.INFO, "Finished processing requestId: " + currentRequestID);
        }

        public byte[] sendAndGetResponse(long currentRequestID, byte[] requestBody) {
            Logger localLogger = local.get();
            Socket leaderSocket = null;
            boolean received = false;
            byte[] response = null;
            while (!received) {
                localLogger.info("Waiting for leader");
                while (leader == null || peerServer.isPeerDead(leader))
                    ;
                InetSocketAddress currentLeader = leader;
                if(currentLeader==null){
                    continue;
                }
                localLogger.info("Leader found " + currentLeader);
                try {
                    leaderSocket = new Socket(currentLeader.getAddress(), currentLeader.getPort() + 2);
                } catch (IOException e) {
                    localLogger.log(Level.SEVERE, "Could not open socket to leader looping", e);
                    continue;
                }
                Message work = new Message(MessageType.WORK, requestBody, getAddress().getHostName(),
                        getAddress().getPort(), currentLeader.getAddress().getHostAddress(), currentLeader.getPort() + 2,
                        currentRequestID);
                localLogger.info("assigned request ID of " + currentRequestID);
                try {
                    leaderSocket.getOutputStream().write(work.getNetworkPayload());
                } catch (IOException e) {
                    localLogger.log(Level.SEVERE, "Could not write to leader socket looping", e);
                    continue;
                }

                localLogger.info("Attempting to read leader response for requestID " + currentRequestID);
                try {
                    while (response == null && !peerServer.isPeerDead(currentLeader)) {
                        response = Util.failableReadAllBytesFromNetwork(leaderSocket.getInputStream());
                    }
                    if (peerServer.isPeerDead(currentLeader)) {
                        logger.info("leader failed, going to resend");
                        continue;
                    }
                    received = true;
                } catch (IOException e) {
                    localLogger.log(Level.SEVERE, "Could not read leader response resending request", e);
                    continue;
                }
            }
            return response;
        }
    }
}
