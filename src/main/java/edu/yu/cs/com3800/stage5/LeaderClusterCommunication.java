package edu.yu.cs.com3800.stage5;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.yu.cs.com3800.LoggingServer;
import edu.yu.cs.com3800.Message;
import edu.yu.cs.com3800.Util;

public class LeaderClusterCommunication implements LoggingServer, Runnable {
    private InetSocketAddress target;
    private Message message;
    private Map<Long, Socket> pendingResponses;
    private Map<InetSocketAddress, Set<Long>> followerWorkMap;
    private Map<Long, Message> requestMessageMap;
    private ZooKeeperPeerServerImpl server;
    private Map<Long, Message> oldCompletedWork;
    private ThreadLocal<Logger> local;

    public LeaderClusterCommunication(InetSocketAddress target,
            Message message, Map<Long, Socket> pendingResponses,
            Map<InetSocketAddress, Set<Long>> followerWorkMap, Map<Long, Message> requestMessageMap,
            ZooKeeperPeerServerImpl server, Map<Long, Message> oldCompletedWork, ThreadLocal<Logger> local) {
        this.target = target;
        this.message = message;
        this.pendingResponses = pendingResponses;
        this.followerWorkMap = followerWorkMap;
        this.requestMessageMap = requestMessageMap;
        this.server = server;
        this.oldCompletedWork = oldCompletedWork;
        this.local = local;
    }

    @Override
    public void run() {
        Logger logger = local.get();
        Socket socket = null;
        OutputStream os = null;
        boolean completed = false;
        if (oldCompletedWork.containsKey(message.getRequestID())) {
            logger.info("Request: " + message.getRequestID() + " was already completed, sending response");
            try {
                socket = this.pendingResponses.get(message.getRequestID());
                Message oldMessage = oldCompletedWork.get(message.getRequestID());
                Message newMessage = new Message(oldMessage.getMessageType(), oldMessage.getMessageContents(),
                        server.getAddress().getHostName(), server.getUdpPort(), socket.getInetAddress().getHostName(),
                        socket.getPort(), oldMessage.getRequestID(), oldMessage.getErrorOccurred());
                socket.getOutputStream().write(newMessage.getNetworkPayload());
                logger.info("Request: " + message.getRequestID() + " response sent");
                return;
            } catch (IOException e) {
                logger.log(Level.SEVERE, "Could not send old completed work back to gateway", e);
            }
        }

        while (!server.isPeerDead(new InetSocketAddress(target.getHostString(), target.getPort() - 2)) && !completed) {
            try {
                logger.info(
                        "sending requestID " + message.getRequestID() + " to " + target.getAddress()
                                + target.getPort());
                logger.finest("Attempting to open socket");
                socket = new Socket(target.getAddress(), target.getPort());
                logger.finest("Opened Socket successfully, getting output stream");
                os = socket.getOutputStream();
                logger.finest("Got output stream successfully, writing");
                os.write(message.getNetworkPayload());
                logger.finest("Sent work successfully, awaiting response from follower");
                Message response = null;
                while (!Thread.currentThread().isInterrupted()
                        && !server.isPeerDead(new InetSocketAddress(target.getHostString(), target.getPort() - 2))) {
                    byte[] bytes = Util.failableReadAllBytesFromNetwork(socket.getInputStream());
                    if (bytes != null) {
                        response = new Message(bytes);
                        break;
                    }
                }
                socket.close();
                if (response == null) {
                    return;
                }
                socket = pendingResponses.get(response.getRequestID());
                logger.finest("received response for " + response.getRequestID() + ", sending to gateway");
                os = socket.getOutputStream();
                logger.finest("Got output stream successfully, writing");


                response = new Message(response.getMessageType(), response.getMessageContents(), server.getAddress().getHostName(), server.getUdpPort(), socket.getInetAddress().getHostName(), socket.getPort(), response.getRequestID(), response.getErrorOccurred());
                os.write(response.getNetworkPayload());
                logger.info("Sent response for requestID " + response.getRequestID());
                Set<Long> set = followerWorkMap.get(new InetSocketAddress(target.getHostName(), target.getPort() - 2));
                if (set == null) {
                    return;
                }
                set.remove(message.getRequestID());
                requestMessageMap.remove(message.getRequestID());
                completed = true;
                /*
                 * if (socket != null) {
                 * try {
                 * socket.close();
                 * } catch (IOException e) {
                 * logger.log(Level.SEVERE, "socket close failed", e);
                 * }
                 * }
                 */
            } catch (IOException e) {
                logger.log(Level.SEVERE,
                        "most recent operation failed, see finest logs - follower probably went down " + target, e);
            } finally {
                /*
                 * if (socket != null) {
                 * try {
                 * socket.close();
                 * } catch (IOException e) {
                 * logger.log(Level.SEVERE, "socket close failed", e);
                 * }
                 * }
                 */
            }
        }
    }

}
