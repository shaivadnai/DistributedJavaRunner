package edu.yu.cs.com3800.stage5;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import edu.yu.cs.com3800.LoggingServer;
import edu.yu.cs.com3800.Message;
import edu.yu.cs.com3800.Message.MessageType;

public class GossipThread extends Thread implements LoggingServer {
    private LinkedBlockingQueue<Message> incomingMessages;
    private LinkedBlockingQueue<Message> outgoingMessages;
    protected Map<Long, GossipEntry> gossipMap;
    private long heartbeat = 1;
    private ZooKeeperPeerServerImpl server;
    private Logger logger;
    final static private int gossipTimePeriod = 3000;
    final static private int failTimeout = gossipTimePeriod * 15;
    final static private int cleanupTimeout = failTimeout * 2;
    private Set<Long> deadPeers;
    private Set<Long> failedPeers;
    private ConcurrentLinkedQueue<GossipMessage> messageRecord;
    private HttpServer http;
    private Logger heartbeatMessagelogger;
    int gossipMessageCount = 0;

    public GossipThread(ZooKeeperPeerServerImpl server, LinkedBlockingQueue<Message> incomingMessages,
            LinkedBlockingQueue<Message> outgoingMessages) {
        this.gossipMap = new ConcurrentHashMap<>();
        this.heartbeatMessagelogger = initializeLogging("HeartbeatMessageLog-Server-"+server.getServerId());
        this.messageRecord = new ConcurrentLinkedQueue<>();
        this.deadPeers = ConcurrentHashMap.newKeySet();
        this.failedPeers = ConcurrentHashMap.newKeySet();
        this.incomingMessages = incomingMessages;
        this.outgoingMessages = outgoingMessages;
        this.server = server;
        this.logger = initializeLogging(GossipThread.class.getCanonicalName() + "-port-" + server.getUdpPort());
        try {
            this.http = HttpServer.create(new InetSocketAddress(server.getUdpPort() + 5), 0);
        } catch (IOException e) {
            logger.log(Level.SEVERE, "Could not create httpserver ", e);
        }
        this.http.createContext("/getgossipmessages", new GossipHttpHandler());

        this.http.start();
        initialGossipMapLoad();
        setDaemon(true);
    }
  

    public class GossipHttpHandler implements HttpHandler {
        ThreadLocal<Logger> local = new ThreadLocal<Logger>() {
            @Override
            protected Logger initialValue() {
                return initializeLogging(
                        GossipHttpHandler.class.getName() + "-name-" + Thread.currentThread().getName(),
                        false);
            }
        };

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            Logger localLogger = local.get();
            localLogger.log(Level.INFO,
                    "Server received request on " + exchange.getHttpContext().getPath() + " from port "
                            + exchange.getRemoteAddress().getPort());

            StringBuilder response = new StringBuilder();
            for (GossipMessage gm : messageRecord) {
                response.append(gm.toString());
                response.append("\n\n");
            }
            int responseCode = 200;
            int responseLength = response.length();
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
            localLogger.info("Attempting to write response body");
            try {
                exchange.getResponseBody().write(response.toString().getBytes());
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

            localLogger.log(Level.INFO, "Finished processing request");
        }
    }

    private void initialGossipMapLoad() {
        logger.info("Performing initial Gossip Map Load");
        for (Long id : server.peerIDtoAddress.keySet()) {
            gossipMap.put(id, new GossipEntry(id, -1));
        }
    }

    public boolean isFailed(long id) {
        if (failedPeers.contains(id) || deadPeers.contains(id)) {
            return true;
        }
        GossipEntry ge = gossipMap.get(id);
        if (ge == null) {
            return false;
        }
        return ge.isFailed;
    }

    @Override
    public void run() {
        while (!server.shutdown) {
            sendGossip();
            gossipMap.put(server.getServerId(), new GossipEntry(server.getServerId(), heartbeat++));
            Message message = null;
            while (message == null) {
                if (server.shutdown) {
                    return;
                }
                try {
                    message = incomingMessages.poll(gossipTimePeriod, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    logger.log(Level.SEVERE, "Interrupted during poll", e);
                }
                if (message == null) {
                    logger.fine("Got no Gossip, looping");
                    sendGossip();
                }
            }
            GossipMessage gmRecord = new GossipMessage(message);
            heartbeatMessagelogger.log(Level.INFO, gmRecord.toString());
            logger.info("Got gossip message from queue");
            logger.info("Updating Map");
            updateGossipMap(
                    server.addressToPeerID.get(new InetSocketAddress(message.getSenderHost(), message.getSenderPort())),
                    bytesToGossips(message.getMessageContents()));
            logger.info("Marking failed nodes");
            markFailedNodes();
            logger.info("Cleaning failed nodes");
            cleanupFailedNodes();
            try {
                sleep(gossipTimePeriod);
            } catch (InterruptedException e) {
                logger.log(Level.SEVERE, "Sleep Interrupted", e);
            }
        }
        logger.severe("Shutting Down");
    }

    private void sendGossip() {
        InetSocketAddress randomTarget = server.getRandomPeer();
        outgoingMessages
                .offer(new Message(MessageType.GOSSIP, buildGossipPayload(), server.getAddress().getHostName(),
                        server.getUdpPort(), randomTarget.getHostName(), randomTarget.getPort()));
    }

    private void updateGossipMap(long sourceId, List<GossipEntry> gossips) {
        for (GossipEntry e : gossips) {
            if (e.id == server.getServerId()) {
                continue;
            }
            GossipEntry old = gossipMap.get(e.getId());
            if (old == null) {
                logger.warning("unrecognized server " + e.id + " with heartbeat " + e.heartBeat);
            } else {
                if (old.update(e, gossipMap)) {
                    logger.fine("updated server " + e.id + " with heartbeat " + e.heartBeat);
                    SimpleDateFormat sdf = new SimpleDateFormat("hh:mm:ss");
                    logger.fine(server.getServerId() + " : updated " + e.getId()
                            + "'s heartbeat sequence to " + e.getHeartbeat() + " based on " + sourceId
                            + " at node time " + sdf.format(new java.util.Date())); //heartbeat
                }
            }
        }
    }

    private void cleanupFailedNodes() {
        for (Long l : failedPeers) {
            GossipEntry ge = gossipMap.get(l);
            if (ge.shouldCleanup()) {
                gossipMap.remove(ge.getId());
                failedPeers.remove(l);
                deadPeers.add(l);
                logger.fine("cleaned entry " + l);
            }
        }
    }

    private void markFailedNodes() {
        for (Entry<Long, GossipEntry> e : gossipMap.entrySet()) {
            if (failedPeers.contains(e.getKey()) || deadPeers.contains(e.getKey())) {
                continue;
            }
            if (e.getValue().isFailed()) {
                failedPeers.add(e.getKey());
                logger.fine("Marked failed " + e.getKey());
                logger.fine(server.getServerId() + " : no heartbeat from server "
                        + e.getKey() + " - server failed");
            }
        }
    }

    private byte[] buildGossipPayload() {
        gossipMap.put(server.getServerId(), new GossipEntry(server.getServerId(), heartbeat));
        ByteBuffer bb = ByteBuffer.allocate((gossipMap.size() - failedPeers.size()) * GossipEntry.BYTES);
        for (Entry<Long, GossipEntry> e : gossipMap.entrySet()) {
            if (e.getValue().isFailed) {
                continue;
            }
            bb.put(e.getValue().getBytes());
        }
        return bb.array();
    }

    private List<GossipEntry> bytesToGossips(byte[] bytes) {
        List<GossipEntry> list = new LinkedList<>();
        ByteBuffer bb = ByteBuffer.wrap(bytes);
        for (int i = 0; i < bytes.length; i += 16) {
            list.add(new GossipEntry(bb.getLong(), bb.getLong()));
        }
        return list;
    }

    public static class GossipEntry {
        final static public int BYTES = 16;
        private long heartBeat;
        private long lastTime;
        private boolean isFailed;
        private long id;

        public GossipEntry(long id, long heartBeat) {
            this.lastTime = System.currentTimeMillis();
            this.isFailed = false;
            this.id = id;
            this.heartBeat = heartBeat;
        }

        public GossipEntry(byte[] bytes) {
            ByteBuffer bb = ByteBuffer.wrap(bytes);
            this.id = bb.getLong();
            this.heartBeat = bb.getLong();
        }

        public byte[] getBytes() {
            ByteBuffer bb = ByteBuffer.allocate(Long.BYTES * 2);
            bb.putLong(id);
            bb.putLong(heartBeat);
            return bb.array();
        }

        public boolean isFailed() {
            if (isFailed) {
                return isFailed;
            }
            return checkFailed();
        }

        private boolean checkFailed() {
            if (System.currentTimeMillis() - lastTime > failTimeout) {
                return setFailed();
            }
            return false;
        }

        private boolean setFailed() {
            isFailed = true;
            return isFailed;
        }

        public long getId() {
            return id;
        }

        public boolean shouldCleanup() {
            if (isFailed && System.currentTimeMillis() - lastTime > cleanupTimeout) {
                return true;
            }
            return false;
        }

        public boolean update(GossipEntry entry, Map<Long, GossipEntry> gossipMap) {
            if (!this.isFailed && entry.heartBeat > this.heartBeat) {
                gossipMap.put(entry.getId(), new GossipEntry(entry.getId(), entry.getHeartbeat()));
                return true;
            }
            return false;
        }

        public long getHeartbeat() {
            return this.heartBeat;
        }

        @Override
        public String toString() {
            return "ID:" + this.id + " Heartbeat:" + this.heartBeat;
        }
    }

    public class GossipMessage {
        private long receiptTime;
        private Message message;

        public GossipMessage(Message message) {
            this.message = message;
            this.receiptTime = System.currentTimeMillis();
        }

        @Override
        public String toString() {
            SimpleDateFormat sdf = new SimpleDateFormat("MMM dd, yyyy HH:mm:ss");
            Date date = new Date(receiptTime);
            return "Received at: " + sdf.format(date) + "\n" + message.toString();
        }
    }

    public Map<Long,GossipEntry> getGossipMap(){
        return gossipMap;
    }
}
