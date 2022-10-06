package edu.yu.cs.com3800.stage5;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.yu.cs.com3800.JavaRunner;
import edu.yu.cs.com3800.LoggingServer;
import edu.yu.cs.com3800.Message;
import edu.yu.cs.com3800.Message.MessageType;
import edu.yu.cs.com3800.ZooKeeperPeerServer.ServerState;
import edu.yu.cs.com3800.Util;

public class JavaRunnerFollower extends Thread implements LoggingServer {
    private ZooKeeperPeerServerImpl myFollower;
    private int myPort;
    private boolean shutDown;
    private Logger logger;
    static final int timeout = 200;
    private ServerSocket mySocket;
    private Message completedWork = null;

    public JavaRunnerFollower(ZooKeeperPeerServerImpl myFollower, ServerSocket mySocket) {
        this.myFollower = myFollower;
        this.myPort = myFollower.getTcpPort();
        this.shutDown = false;
        this.logger = initializeLogging(JavaRunnerFollower.class.getName() + "-on-port-" + this.myPort
                + "-on-id-" + myFollower.getServerId(), false);
        setDaemon(true);
        this.mySocket = mySocket;
        try {
            this.mySocket.setSoTimeout(150);
        } catch (SocketException e) {
            logger.log(Level.SEVERE, "Could not set socket timeout", e);
        }
    }

    public void shutDown() {
        this.shutDown = true;
    }

    @Override
    public void run() {
        while (!shutDown) {
            Socket leader = null;
            Message message = null;

            while (myFollower.getPeerState() == ServerState.LOOKING)
                logger.info("Waiting for new leader to be elected");

            while (leader == null && !this.shutDown && myFollower.getPeerState() == ServerState.FOLLOWING) {
                try {
                    leader = mySocket.accept();
                    message = new Message(Util.readAllBytesFromNetwork(leader.getInputStream()));
                } catch (InterruptedIOException e) {
                    logger.finest("server socket timed out while waiting to accept connection - looping");
                } catch (IOException e) {
                    logger.log(Level.SEVERE, "Could not accept Leader connection", e);
                    continue;
                }
            }

            if (this.shutDown || myFollower.getPeerState()!=ServerState.FOLLOWING) {
                continue;
            }
            if (message.getMessageType() == MessageType.NEW_LEADER_GETTING_LAST_WORK) {
                logger.info("Sending completedwork");
                try {
                    if (completedWork == null) {
                        completedWork = new Message(MessageType.COMPLETED_WORK, new byte[0],
                                myFollower.getAddress().getHostName(),
                                myFollower.getTcpPort(), message.getReceiverHost(), message.getReceiverPort(),
                                0, true);
                        logger.info("There was no completed work");
                    }
                    logger.info("Sending over " + completedWork.getRequestID());
                    leader.getOutputStream().write(completedWork.getNetworkPayload());
                    completedWork = null;
                } catch (IOException e) {
                    this.logger.log(Level.SEVERE,
                            "Could not write to response to Leader socket when asked for last work",
                            e);
                }
                continue;
            }

            String response = null;

            logger.info("Received WORK message from " + message.getSenderHost() + message.getSenderPort()
                    + " with RequestID "
                    + message.getRequestID());
            try {
                logger.log(Level.INFO, "Compiling request src code");
                response = new JavaRunner().compileAndRun(new ByteArrayInputStream(message.getMessageContents()));
                completedWork = new Message(MessageType.COMPLETED_WORK, response.getBytes(),
                        myFollower.getAddress().getHostName(),
                        myFollower.getTcpPort(), message.getReceiverHost(), message.getReceiverPort(),
                        message.getRequestID(), false);
                logger.log(Level.INFO, "Compileandrun succesful");
                logger.log(Level.INFO, "Response body: " + response);
            } catch (Exception e) {
                logger.log(Level.FINE, "Compileandrun unsuccesful");
                String stackTrace = Util.getStackTrace(e);
                response = e.getMessage() + "\n" + stackTrace;
                logger.log(Level.INFO, "Response body: " + response);
                completedWork = new Message(MessageType.COMPLETED_WORK, response.getBytes(),
                        myFollower.getAddress().getHostName(),
                        myFollower.getTcpPort(), message.getReceiverHost(), message.getReceiverPort(),
                        message.getRequestID(), true);
            }
            logger.info("Sending response to leader");
            boolean sentToLeader = false;
            while(!sentToLeader && myFollower.getPeerState()==ServerState.FOLLOWING && !myFollower.isPeerDead(new InetSocketAddress(message.getSenderHost(), message.getSenderPort()-2))){
                OutputStream os = null;
                try {
                    os = leader.getOutputStream();
                    os.write(completedWork.getNetworkPayload());
                    completedWork = null;
                    sentToLeader = true;
                    logger.info("Send successful");
                } catch (IOException e) {
                    this.logger.log(Level.SEVERE, "Could not write to response to Leader socket looping",
                            e);
                }
            }
            if(!sentToLeader){
                try {
                    leader.close();
                } catch (IOException e) {
                    logger.log(Level.SEVERE,"Could not close socket",e);
                }
                logger.info("Leader went down before could send response, looping and waiting");
            }
        }
        logger.severe("Exiting Run Method - Shutdown");
    }

    public Message getCompletedWork() {
        return this.completedWork;
    }
}
