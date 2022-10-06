package edu.yu.cs.com3800.stage5;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.yu.cs.com3800.Message;
import edu.yu.cs.com3800.Message.MessageType;
import edu.yu.cs.com3800.UDPMessageReceiver;
import edu.yu.cs.com3800.UDPMessageSender;
import edu.yu.cs.com3800.Util;
import edu.yu.cs.com3800.Vote;
import edu.yu.cs.com3800.ZooKeeperLeaderElection;
import edu.yu.cs.com3800.ZooKeeperPeerServer;

public class ZooKeeperPeerServerImpl extends Thread implements ZooKeeperPeerServer {
    private final InetSocketAddress myAddress;
    private final int myPort;
    volatile protected ServerState state;
    protected volatile boolean shutdown;
    private LinkedBlockingQueue<Message> udpOutgoingMessages;
    protected LinkedBlockingQueue<Message> incomingElectionMessages;
    private LinkedBlockingQueue<Message> incomingHeartbeatMessages;

    private Long id;
    private long peerEpoch;
    protected volatile Vote currentLeader;
    protected Map<Long, InetSocketAddress> peerIDtoAddress;
    private Logger electionLogger;

    protected UDPMessageSender udpSender;
    protected UDPMessageReceiver udpReceiver;

    protected GossipThread gossipThread;
    private ServerSocket serverSocket;
    protected final Logger logger;

    private JavaRunnerFollower followerWorker;
    private RoundRobinLeader leaderWorker;

    Map<InetSocketAddress, Long> addressToPeerID;

    private int observerCount;

    public ZooKeeperPeerServerImpl(int myPort, long peerEpoch, Long id, Map<Long, InetSocketAddress> peerIDtoAddress,
            int observerCount) {
        this.id = id;
        this.myPort = myPort;
        this.peerEpoch = peerEpoch;
        this.peerIDtoAddress = peerIDtoAddress;
        this.addressToPeerID = new ConcurrentHashMap<>();
        for (Entry<Long, InetSocketAddress> e : peerIDtoAddress.entrySet()) {
            addressToPeerID.put(e.getValue(), e.getKey());
        }
        this.electionLogger = null;
        this.myAddress = new InetSocketAddress("127.0.0.1", this.myPort);
        this.state = ServerState.LOOKING;
        this.observerCount = observerCount;
        this.currentLeader = new Vote(getServerId(), getPeerEpoch());

        this.udpOutgoingMessages = new LinkedBlockingQueue<>();
        this.incomingElectionMessages = new LinkedBlockingQueue<>();
        this.incomingHeartbeatMessages = new LinkedBlockingQueue<>();
        this.logger = initializeLogging(ZooKeeperPeerServerImpl.class.getName() + "-on-port-" + this.myPort
                + "-on-id-(" + getServerId() + ")", false);

        this.udpSender = new UDPMessageSender(this.udpOutgoingMessages, this.myPort);
        try {
            this.udpReceiver = new UDPMessageReceiver(this.incomingElectionMessages, this.incomingHeartbeatMessages,
                    this.myAddress, this.myPort, this);
        } catch (IOException e1) {
            logger.log(Level.SEVERE, "Receiver Worker could not be constructed", e1);
        }
        try {
            this.serverSocket = new ServerSocket(getTcpPort());
        } catch (IOException e1) {
            logger.log(Level.SEVERE, "Could not create tcp serversocket", e1);
        }
        this.gossipThread = new GossipThread(this, incomingHeartbeatMessages, udpOutgoingMessages);
        gossipThread.start();
    }

    @Override
    public void shutdown() {
        this.shutdown = true;
        this.logger.info("Server received shutdown signal");
        this.udpSender.shutdown();
        this.udpReceiver.shutdown();
        if (followerWorker != null) {
            followerWorker.shutDown();
        }
        if (leaderWorker != null) {
            leaderWorker.shutDown();
        }
    }

    @Override
    public void run() {
        // step 1: create and run thread that sends broadcast messages
        // step 2: create and run thread that listens for messages sent to this server
        // step 3: main server loop

        // reversed steps 1 and two in the hope it would reduce the number of missed udp
        // messages
        udpReceiver.start();
        logger.info("Started UDP Receiver");
        udpSender.start();
        logger.info("Started UDP Sender");

        try {
            while (!this.shutdown) {
                switch (getPeerState()) {
                    case LOOKING:
                        logger.info("Entered looking case");
                        peerEpoch++;
                        ZooKeeperLeaderElection election = new ZooKeeperLeaderElection(this, incomingElectionMessages,
                                electionLogger);
                        this.currentLeader = election.lookForLeader();
                        this.electionLogger = election.getLogger();
                        logger.info(
                                "Found leader " + currentLeader.getProposedLeaderID() + " and I'm a "
                                        + state);
                        logger.fine(
                                getServerId() + " : switching from " + ServerState.LOOKING + " to " + getPeerState());
                        this.incomingElectionMessages.clear();
                        logger.info("Cleared incoming message queue");
                        break;
                    case FOLLOWING:
                        if (followerWorker == null) {
                            followerWorker = new JavaRunnerFollower(this, serverSocket);
                            Util.startAsDaemon(followerWorker, getServerId() + " Follower");
                            logger.info("Starting JavaRunner Thread");
                        }
                        if (isPeerDead(currentLeader.getProposedLeaderID())) {
                            logger.info("Leader died, new election starting");
                            setPeerState(ServerState.LOOKING);
                            logger.fine(getServerId() + " : switching from " + ServerState.FOLLOWING + " to "
                                    + getPeerState());
                        }
                        break;
                    case LEADING:
                        if (leaderWorker == null) {
                            Set<InetSocketAddress> workers = new HashSet<>(peerIDtoAddress.values());
                            workers.remove(getAddress());
                            if (followerWorker == null) {
                                leaderWorker = new RoundRobinLeader(this, workers, null, false, serverSocket);
                            } else {
                                followerWorker.shutDown();
                                leaderWorker = new RoundRobinLeader(this, workers, followerWorker.getCompletedWork(),
                                        true, serverSocket);
                            }
                            Util.startAsDaemon(leaderWorker, getServerId() + " Leader");
                            logger.info("Started RoundRobin Thread");
                        }
                        break;
                    default:
                        break;
                }

            }
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Exception was thrown in server run loop", e);
        }
        this.logger.severe("Exiting ZooKeeperPeerServerImpl.run()");
    }

    @Override
    public void setCurrentLeader(Vote v) throws IOException {
        this.currentLeader = v;
    }

    @Override
    public Vote getCurrentLeader() {
        return this.currentLeader;
    }

    @Override
    public void sendMessage(MessageType type, byte[] messageContents, InetSocketAddress target)
            throws IllegalArgumentException {
        Message msg = new Message(type, messageContents, this.myAddress.getHostString(), this.myPort,
                target.getHostString(), target.getPort());
        this.udpOutgoingMessages.offer(msg);

    }

    @Override
    public void sendBroadcast(MessageType type, byte[] messageContents) {
        for (InetSocketAddress peer : peerIDtoAddress.values()) {
            // If the map contains this server
            if (peer.equals(this.myAddress) || isPeerDead(peer)) {
                // skip and don't send message to self or dead servers
                continue;
            }
            Message msg = new Message(Message.MessageType.ELECTION, messageContents, this.myAddress.getHostString(),
                    this.myPort, peer.getHostString(), peer.getPort());
            this.udpOutgoingMessages.offer(msg);
        }

    }

    public InetSocketAddress getRandomPeer() {
        /*
         * Random random = new Random();
         * InetSocketAddress address = null;
         * 
         * Object[] values = peerIDtoAddress.values().toArray();
         * while (address == null || address.equals(myAddress) || isPeerDead(address)) {
         * address = (InetSocketAddress) values[random.nextInt(values.length)];
         * }
         * return address;
         */
        List<InetSocketAddress> peers = new ArrayList<>(peerIDtoAddress.values());
        Collections.shuffle(peers);
        InetSocketAddress result = null;
        while (result == null) {
            for (InetSocketAddress address : peers) {
                if (!address.equals(myAddress) && !isPeerDead(address)) {
                    result = address;
                }
            }
        }
        return result;
    }

    @Override
    public ServerState getPeerState() {
        return this.state;
    }

    @Override
    public void setPeerState(ServerState newState) {
        this.state = newState;
    }

    @Override
    public Long getServerId() {
        return this.id;
    }

    @Override
    public long getPeerEpoch() {
        return this.peerEpoch;
    }

    @Override
    public InetSocketAddress getAddress() {
        return this.myAddress;
    }

    @Override
    public int getUdpPort() {
        return this.myPort;
    }

    public int getTcpPort() {
        return this.myPort + 2;
    }

    @Override
    public InetSocketAddress getPeerByID(long peerId) {
        return this.peerIDtoAddress.get(peerId);
    }

    @Override
    public int getQuorumSize() {
        // if server is in the map
        if (peerIDtoAddress.containsKey(id)) {
            // size accounts for this server
            return ((this.peerIDtoAddress.size() - observerCount) / 2) + 1;
            // not in map
        } else {
            // size does not account for this server - add one
            return (((this.peerIDtoAddress.size() - observerCount) / 2) + 1) + 1;
        }
    }

    @Override
    public boolean isPeerDead(InetSocketAddress address) {
        if (address == null) {
            return true;
        }
        Long id = addressToPeerID.get(address);
        if (id == null) {
            return true;
        }
        return gossipThread.isFailed(id);
    }

    @Override
    public boolean isPeerDead(long peerID) {
        return gossipThread.isFailed(peerID);
    }

    public Long getAddressByPeerID(InetSocketAddress address) {
        return addressToPeerID.get(address);
    }
}
