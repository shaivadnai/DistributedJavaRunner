package edu.yu.cs.com3800;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.yu.cs.com3800.Message.MessageType;
import edu.yu.cs.com3800.ZooKeeperPeerServer.ServerState;

public class ZooKeeperLeaderElection implements LoggingServer {
    /**
     * time to wait once we believe we've reached the end of leader election.
     */
    private final static int finalizeWait = 200;
    private LinkedBlockingQueue<Message> incomingMessages;
    private Map<Long, ElectionNotification> votes;
    private ZooKeeperPeerServer myPeerServer;
    private long proposedLeader;
    private long proposedEpoch;
    private Logger logger;

    /**
     * Upper bound on the amount of time between two consecutive notification
     * checks. This impacts the amount of time to get the system up again after long
     * partitions. Currently 60 seconds.
     */
    private final static int maxNotificationInterval = 60000;

    public ZooKeeperLeaderElection(ZooKeeperPeerServer server, LinkedBlockingQueue<Message> incomingMessages,
            Logger logger) {
        this.incomingMessages = incomingMessages;
        this.myPeerServer = server;
        this.proposedLeader = server.getServerId();
        this.proposedEpoch = server.getPeerEpoch();
        this.votes = new HashMap<>();
        if (logger == null) {
            this.logger = initializeLogging(ZooKeeperLeaderElection.class.getName() + "-port-"
                    + myPeerServer.getUdpPort() + "-id-" + myPeerServer.getServerId(), false);
        } else {
            this.logger = logger;
        }
    }

    public Logger getLogger() {
        return this.logger;
    }

    private synchronized Vote getCurrentVote() {
        return new Vote(this.proposedLeader, this.proposedEpoch);
    }

    public synchronized Vote lookForLeader() {
        // send initial notifications to other peers to get things started
        sendNotifications();
        logger.info("Sent initial vote for myself");

        // Loop, exchanging notifications with other servers until we find a leader
        while (this.myPeerServer.getPeerState() == ZooKeeperPeerServer.ServerState.LOOKING
                || this.myPeerServer.getPeerState() == ServerState.OBSERVER) {
            Message message = null;
            ElectionNotification n = null;
            int timeout = finalizeWait * 2;

            while (message == null || isInvalidNotification(n)) {
                // Remove next notification from queue, timing out after 2 times the termination
                // time
                try {
                    message = incomingMessages.poll(timeout, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    logger.log(Level.SEVERE, "Message Queue Poll was interrupted", e);
                }

                // if no notifications received..
                if (message == null) {
                    // ..resend notifications to prompt a reply from others..
                    sendNotifications();
                    logger.info("No notifications received, resending notifications");
                    // .and implement exponential back-off
                    if (timeout << 1 > maxNotificationInterval)
                        timeout = timeout << 1;
                    continue;
                }

                n = getNotificationFromMessage(message);

                logger.info("Got notification from " + n.getSenderID() + " who is " + n.getState() + " and voting for "
                        + n.getProposedLeaderID());
            }

            switch (n.getState()) {
                // if the sender is also looking
                case LOOKING:
                    // if the received message has a vote for a leader which supersedes mine
                    if (this.supersedesCurrentVote(n.getProposedLeaderID(), n.getPeerEpoch())) {
                        // change my vote and tell all my peers what my new vote is.
                        changeVote(n);
                        logger.info("changed my vote to " + n.getProposedLeaderID());
                    }

                    // keep track of the votes I received and who I received them from.
                    votes.put(n.getSenderID(), n);

                    // if I have enough votes to declare my currently proposed leader as the leader:

                    if (haveEnoughVotes(votes, new Vote(this.proposedLeader, this.proposedEpoch))) {
                        // first check if there are any new votes for a higher ranked possible leader
                        // before I declare a leader. If so, continue in my election loop
                        logger.info("There are enough votes for my current proposed leader " + proposedLeader
                                + ". Checking if next message is for higher leader");
                        if (!isHigherVote()) {
                            // If not, set my own state to either LEADING (if I won the election) or
                            // FOLLOWING (if someone lese won the election) and exit the election
                            logger.info("There was no higher server in the queue, accepting election");
                            return acceptElectionWinner(n);
                        }
                        logger.info("There was a higher vote in the queue");
                    }
                    break;

                // if the sender is following a leader already or thinks it is the leader
                case FOLLOWING:
                case LEADING:
                    votes.put(n.getSenderID(), n);
                    // IF: see if the sender's vote allows me to reach a conclusion based on the
                    // election epoch that I'm in, i.e. it gives the majority to the vote of the
                    // FOLLOWING or LEADING peer whose vote I just received.
                    if (haveEnoughVotes(votes, n)) {
                        logger.info("There are enough votes for my current proposed leader " + n.getProposedLeaderID()
                                + " accepting election");
                        // if so, accept the election winner.
                        // As, once someone declares a winner, we are done. We are not worried about /
                        // accounting for misbehaving peers.
                        acceptElectionWinner(n);
                    }
                default:
                    break;
            }
        }
        return getCurrentVote();
    }

    private boolean isHigherVote() {
        ElectionNotification n = null;
        try {
            while ((n = getNotificationFromMessage(
                    this.incomingMessages.poll(finalizeWait*2, TimeUnit.MILLISECONDS))) != null) {
                if (this.proposedLeader < n.getProposedLeaderID()) {
                    this.incomingMessages.put(new Message(MessageType.ELECTION, buildMsgContent(n), "localhost", 1000,
                            "localhost", 1000));
                    return true;
                } else {
                    n = null;
                }
            }
        } catch (InterruptedException e) {
            logger.log(Level.SEVERE, "Incoming Messages Queue was interrupted", e);
        }
        return false;
    }

    private void changeVote(ElectionNotification n) {
        this.proposedEpoch = n.getPeerEpoch();
        this.proposedLeader = n.getProposedLeaderID();
        sendNotifications();
    }

    private boolean isInvalidNotification(ElectionNotification n) {
        if (n == null) {
            return false;
        }
        return this.myPeerServer.isPeerDead(n.getSenderID()) && this.myPeerServer.isPeerDead(n.getProposedLeaderID());
    }

    private void sendNotifications() {
        byte[] message;
        if (myPeerServer.getPeerState() == ServerState.OBSERVER) {
            message = buildMsgContent(new ElectionNotification(-1, this.myPeerServer.getPeerState(), -1, -1));
        } else {
            message = buildMsgContent(new ElectionNotification(this.proposedLeader, this.myPeerServer.getPeerState(),
                    this.myPeerServer.getServerId(), this.myPeerServer.getPeerEpoch()));
        }
        myPeerServer.sendBroadcast(MessageType.ELECTION, message);
    }

    private Vote acceptElectionWinner(ElectionNotification n) {
        // set my state to either LEADING or FOLLOWING
        Vote vote = new Vote(n.getProposedLeaderID(), n.getPeerEpoch());
        this.proposedEpoch = n.getPeerEpoch();
        this.proposedLeader = n.getProposedLeaderID();
        if (vote.getProposedLeaderID() == myPeerServer.getServerId()) {
            myPeerServer.setPeerState(ServerState.LEADING);
        } else if (myPeerServer.getPeerState() == ServerState.OBSERVER) {
            // do nothing if I'm the observer
        } else {
            myPeerServer.setPeerState(ServerState.FOLLOWING);
        }
        this.incomingMessages.clear();
        return vote;
    }

    /*
     * We return true if one of the following three cases hold: 1- New epoch is
     * higher 2- New epoch is the same as current epoch, but server id is higher.
     */
    protected boolean supersedesCurrentVote(long newId, long newEpoch) {
        return (newEpoch > this.proposedEpoch) || ((newEpoch == this.proposedEpoch) && (newId > this.proposedLeader));
    }

    /**
     * Termination predicate. Given a set of votes, determines if have sufficient
     * support for the proposal to declare the end of the election round. Who voted
     * for who isn't relevant, we only care that each server has one current vote
     */
    protected boolean haveEnoughVotes(Map<Long, ElectionNotification> votes, Vote proposal) {
        int count = 0;
        if (this.proposedLeader == proposal.getProposedLeaderID()
                && this.myPeerServer.getPeerState() != ServerState.OBSERVER) {
            count++;
        }
        for (ElectionNotification n : votes.values()) {
            if (this.myPeerServer.isPeerDead(n.getSenderID()) || this.proposedEpoch > n.getPeerEpoch()) {
                continue;
            }
            if (proposal.getProposedLeaderID() == n.getProposedLeaderID()) {
                count++;
            }
            if (count >= myPeerServer.getQuorumSize()) {
                return true;
            }
        }
        return false;
    }

    public static byte[] buildMsgContent(ElectionNotification notification) {
        if (notification == null) {
            return null;
        }
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES * 3 + Character.BYTES);
        buffer.putLong(notification.getProposedLeaderID());
        buffer.putChar(notification.getState().getChar());
        buffer.putLong(notification.getSenderID());
        buffer.putLong(notification.getPeerEpoch());
        return buffer.array();
    }

    public static ElectionNotification getNotificationFromMessage(Message received) {
        if (received == null) {
            return null;
        }
        ByteBuffer buffer = ByteBuffer.wrap(received.getMessageContents());
        long proposedid = buffer.getLong();
        char state = buffer.getChar();
        long senderid = buffer.getLong();
        long epoch = buffer.getLong();
        return new ElectionNotification(proposedid, ServerState.getServerState(state), senderid, epoch);
    }
}