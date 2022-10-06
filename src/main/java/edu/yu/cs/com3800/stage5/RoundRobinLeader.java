package edu.yu.cs.com3800.stage5;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.yu.cs.com3800.LoggingServer;
import edu.yu.cs.com3800.Message;
import edu.yu.cs.com3800.Util;
import edu.yu.cs.com3800.ZooKeeperPeerServer;
import edu.yu.cs.com3800.Message.MessageType;

public class RoundRobinLeader extends Thread implements LoggingServer {
    private LinkedBlockingQueue<Message> tcpIncomingMessages;
    private ZooKeeperPeerServerImpl server;
    private boolean shutDown;
    private Logger logger;
    private Map<Long, Socket> requestSocketMap;
    private Map<Long, Message> requestMessageMap;
    private Map<InetSocketAddress, Set<Long>> followerWorkMap;
    private Iterator<InetSocketAddress> iterator;
    private Set<InetSocketAddress> workers;
    private ThreadPoolExecutor pool;
    private LeaderGatewayReceiver tcpReceiver;
    private Map<Long, Message> oldCompletedWork;
    private ThreadLocal<Logger> local;

    private int timeout = 200;

    public RoundRobinLeader(ZooKeeperPeerServerImpl leader,
            Set<InetSocketAddress> workers, Message oldMessage, boolean failOver, ServerSocket serverSocket) {
        this.oldCompletedWork = new ConcurrentHashMap<>();
        this.local = new ThreadLocal<Logger>() {
            @Override
            protected Logger initialValue() {
                return initializeLogging(
                        LeaderClusterCommunication.class.getName() + "-Leader " + server.getServerId() + "-"
                                + Thread.currentThread().getName(),
                        false);
            }
        };
        this.workers = workers;
        this.server = leader;
        this.shutDown = false;
        this.logger = initializeLogging(RoundRobinLeader.class.getName() + "-port-" + leader.getUdpPort()
                + "-id-" + leader.getServerId(), false);
        this.requestSocketMap = new ConcurrentHashMap<>();
        this.followerWorkMap = new ConcurrentHashMap<>();
        this.requestMessageMap = new ConcurrentHashMap<>();
        setDaemon(true);
        this.tcpIncomingMessages = new LinkedBlockingQueue<>();

        if (failOver) {
            if (oldMessage != null) {
                this.oldCompletedWork.put(oldMessage.getRequestID(), oldMessage);
                logger.fine("Got " + oldMessage.getRequestID() + " already completed from follower turned leader");
            }
            getOldWork();
        }
        this.pool = (ThreadPoolExecutor) Executors.newFixedThreadPool(10);
        this.tcpReceiver = new LeaderGatewayReceiver(requestSocketMap, tcpIncomingMessages, leader, requestMessageMap,
                serverSocket);
        tcpReceiver.start();
    }

    public void shutDown() {
        this.shutDown = true;
        this.pool.shutdownNow();
        this.tcpReceiver.interrupt();
    }

    @Override
    public void run() {
        while (this.server.getPeerState() == ZooKeeperPeerServer.ServerState.LEADING && !shutDown) {
            checkAndReassignWork();
            Message message = null;
            int tempTimeout = timeout * 2;
            while (message == null) {
                if (shutDown) {
                    return;
                }
                try {
                    message = tcpIncomingMessages.poll(tempTimeout, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    logger.log(Level.SEVERE, "Message Queue poll was interrupted", e);
                }
                if (message == null) {
                    logger.finest("No message received, waiting");
                    /*
                     * if (tempTimeout << 1 < 60000) {
                     * tempTimeout <<= 1;
                     * }
                     */
                    checkAndReassignWork();
                }
            }
            logger.info("Got a message from Queue");
            switch (message.getMessageType()) {
                case WORK:
                    logger.info("Received WORK from Gateway at "
                            + new InetSocketAddress(message.getSenderHost(), message.getSenderPort()));

                    sendWorkToCluster(message);
                    break;
                default:
                    break;
            }
        }
        logger.severe("Exiting Run Method");
    }

    private void getOldWork() {
        logger.info("Getting Old Work");
        for (InetSocketAddress address : workers) {
            if (server.isPeerDead(address) || server.getAddressByPeerID(address) < 0) {
                continue;
            }
            InetSocketAddress target = new InetSocketAddress(address.getAddress(), address.getPort() + 2);
            Message getOldWork = new Message(MessageType.NEW_LEADER_GETTING_LAST_WORK, new byte[1],
                    server.getAddress().getHostName(), server.getTcpPort(), target.getHostName(), target.getPort());
            Message response = null;
            Socket socket;
            try {
                socket = new Socket(target.getHostName(), target.getPort());
                if (socket.isConnected()) {
                    socket.getOutputStream().write(getOldWork.getNetworkPayload());
                    response = new Message(Util.readAllBytesFromNetwork(socket.getInputStream()));
                }
            } catch (IOException e) {
                logger.log(Level.INFO, target + " was not a follower", e);
                continue;
            }
            if (response.getRequestID() != 0) {
                this.oldCompletedWork.put(response.getRequestID(), response);
                logger.fine("There was old work");
                logger.fine("Got " + response.getRequestID() + " already completed");
            }
        }
        logger.info("Finished getting completed work");
    }

    private void checkAndReassignWork() {
        for (InetSocketAddress address : followerWorkMap.keySet()) {
            if (server.isPeerDead(address)) {
                logger.log(Level.INFO, address + " is down, redistributing");
                for (Long requestID : followerWorkMap.get(address)) {
                    sendWorkToCluster(requestMessageMap.get(requestID));
                    logger.log(Level.FINEST, "resent " + requestID);
                }
                followerWorkMap.remove(address);
            }
        }
    }

    private void sendWorkToCluster(Message message) {
        if (message == null) {
            return;
        }
        InetSocketAddress target = getNextWorker(message);
        logger.info("Sending " + message.getRequestID() + " to follower at: " + target);
        Message workMessage = new Message(MessageType.WORK, message.getMessageContents(),
                server.getAddress().getHostName(), server.getTcpPort(), target.getHostName(), target.getPort(),
                message.getRequestID());
        Set<Long> set = followerWorkMap.getOrDefault(target, ConcurrentHashMap.newKeySet());
        set.add(workMessage.getRequestID());
        followerWorkMap.put(target, set);
        try {
            this.pool.execute(
                    new LeaderClusterCommunication(new InetSocketAddress(target.getAddress(), target.getPort() + 2),
                            workMessage, requestSocketMap, followerWorkMap, requestMessageMap,
                            server,
                            oldCompletedWork, local));
        } catch (RejectedExecutionException e) {
            logger.fine("Pool was already");
        }
    }

    private InetSocketAddress getNextWorker(Message message) {
        InetSocketAddress nextWorker = null;
        do {
            if (iterator == null || !iterator.hasNext()) {
                iterator = workers.iterator();
            }
            nextWorker = iterator.next();
        } while (server.isPeerDead(nextWorker)
                || nextWorker.equals(new InetSocketAddress(message.getSenderHost(), message.getSenderPort() + 2)));
        return nextWorker;
    }
}
