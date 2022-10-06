package edu.yu.cs.com3800;

import static org.junit.Assert.assertTrue;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.yu.cs.com3800.ZooKeeperPeerServer.ServerState;
import edu.yu.cs.com3800.stage5.GatewayServer;
import edu.yu.cs.com3800.stage5.ZooKeeperPeerServerImpl;

public class Cluster {

    static HashMap<Long, InetSocketAddress> map;
    static volatile List<ZooKeeperPeerServerImpl> servers;
    static volatile ZooKeeperPeerServerImpl server;
    static int quorum;
    static int currentPort = 30000;
    static GatewayServer gateway;

    public static List<ZooKeeperPeerServerImpl> initCluster(long machineCount) {

        System.out.println("Initializing " + machineCount + " servers");

        map = new HashMap<>();
        servers = new ArrayList<>();

        quorum = (int) machineCount / 2 + 1;

        for (long i = 1; i <= machineCount; i++) {
            map.put(i, new InetSocketAddress("localhost", currentPort));
            currentPort += 10;
        }

        int gatewaySocket = 8888;

        gateway = new GatewayServer("localhost", gatewaySocket, map, 1);
        gateway.setName("Gateway");
        map.put(-1L, new InetSocketAddress("localhost", gatewaySocket + 2));

        for (Map.Entry<Long, InetSocketAddress> entry : map.entrySet()) {
            if (entry.getValue().getPort() == gatewaySocket + 2) {
                continue;
            }
            HashMap<Long, InetSocketAddress> copy = (HashMap<Long, InetSocketAddress>) map.clone();
            copy.remove(entry.getKey());
            server = new ZooKeeperPeerServerImpl(entry.getValue().getPort(), 0, entry.getKey(), copy, 1);
            servers.add(server);

        }

        gateway.start();
        for (ZooKeeperPeerServerImpl server : servers) {
            server.start();
        }
        waitUntilReady();
        validateServerList();
        return servers;
    }

    public static void killFollower(List<ZooKeeperPeerServerImpl> servers) {
        for (ZooKeeperPeerServerImpl s : servers) {
            if (s.getPeerState() == ServerState.FOLLOWING) {
                s.shutdown();
                return;
            }
        }
    }

    public static void killLeader(List<ZooKeeperPeerServerImpl> server) {
        for (ZooKeeperPeerServerImpl s : servers) {
            if (s.getPeerState() == ServerState.LEADING) {
                s.shutdown();
                return;
            }
        }
    }

    public static void shutdownServers(List<ZooKeeperPeerServerImpl> servers) {
        for (ZooKeeperPeerServerImpl server : servers) {
            server.shutdown();
        }
        gateway.shutdown();
    }

    private static void waitUntilReady() {
        for (ZooKeeperPeerServerImpl server : servers) {
            while (server.getPeerState() == ServerState.LOOKING) {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {

                    e.printStackTrace();
                }
            }
        }
    }

    private static void validateServerList() {
        System.out.println("Validating " + servers.size() + " servers and printing states\n");
        int leaderCount = 0;
        for (ZooKeeperPeerServerImpl server : servers) {
            System.out.println(server.getServerId() + " is " + server.getPeerState() + " "
                    + server.getCurrentLeader().getProposedLeaderID());

            if (server.getPeerState().equals(ServerState.LEADING)) {
                leaderCount++;
            }
            assertTrue("Only one leader!", leaderCount < 2);
            assertTrue("Server is in looking state", !server.getPeerState().equals(ServerState.LOOKING));
            assertTrue("Impossible leader", server.getCurrentLeader().getProposedLeaderID() >= quorum);
            if (server.getPeerState().equals(ServerState.FOLLOWING)) {
                assertTrue("!Following self", server.getCurrentLeader().getProposedLeaderID() != server.getServerId());
            }
            if (server.getPeerState().equals(ServerState.LEADING)) {
                assertTrue("Leading self", server.getCurrentLeader().getProposedLeaderID() == server.getServerId());
            }
        }
        System.out.println();
    }
}
