package edu.yu.cs.com3800.stage5;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.yu.cs.com3800.ZooKeeperLeaderElection;
import edu.yu.cs.com3800.stage5.GossipThread.GossipEntry;

public class GatewayPeerServerImpl extends ZooKeeperPeerServerImpl {
    private Logger electionLogger;

    public GatewayPeerServerImpl(int myPort, long peerEpoch, Long id, Map<Long, InetSocketAddress> peerIDtoAddress, int observerCount) {
        super(myPort, peerEpoch, id, peerIDtoAddress,observerCount);
        this.currentLeader = null;
        super.setPeerState(ServerState.OBSERVER);
        this.state = ServerState.OBSERVER;    
        this.electionLogger = null;
    }
    public Map<Long, GossipEntry> getGossipMap(){
        return super.gossipThread.getGossipMap();
    }
    @Override
    public void run() {
        logger.info("I am a Observer");
        this.udpReceiver.start();
        logger.info("Started UDP Receiver");
        this.udpSender.start();
        logger.info("Started UDP Sender");

        while(!super.shutdown && !this.isInterrupted()){
            try {
                while (!this.shutdown && currentLeader==null) {
                    ZooKeeperLeaderElection election = new ZooKeeperLeaderElection(this,super.incomingElectionMessages,electionLogger);
                    this.electionLogger =  election.getLogger();
                    this.setCurrentLeader(election.lookForLeader());
                }
                if(isPeerDead(currentLeader.getProposedLeaderID())){
                    currentLeader = null;
                }
            } catch (Exception e) {
                logger.log(Level.SEVERE, "Exception was thrown in server run loop", e);
            }
        }
        this.logger.severe("Exiting ZooKeeperPeerServerImpl.run()");
    }
}
