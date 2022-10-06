package edu.yu.cs.com3800.stage5.demo;

import edu.yu.cs.com3800.stage5.GatewayServer;
import edu.yu.cs.com3800.stage5.ZooKeeperPeerServerImpl;

import java.net.InetSocketAddress;
import java.util.*;

public class DemoServerRunner {

    public static void main(String[] args) {
        long id = Long.parseLong(args[0].trim());
        if(id==0){
            return;
        }
        Map<Long, InetSocketAddress> peerIDToUDPAddress = new HashMap<>();
        peerIDToUDPAddress.put(-1L, new InetSocketAddress("localhost", 8890));
        peerIDToUDPAddress.put(1L, new InetSocketAddress("localhost", 30010));
        peerIDToUDPAddress.put(2L, new InetSocketAddress("localhost", 30020));
        peerIDToUDPAddress.put(3L, new InetSocketAddress("localhost", 30030));
        peerIDToUDPAddress.put(4L, new InetSocketAddress("localhost", 30040));
        peerIDToUDPAddress.put(5L, new InetSocketAddress("localhost", 30050));
        peerIDToUDPAddress.put(6L, new InetSocketAddress("localhost", 30060));
        peerIDToUDPAddress.put(7L, new InetSocketAddress("localhost", 30070));        
        int port = peerIDToUDPAddress.get(id).getPort();
        peerIDToUDPAddress.remove(id);
        List<Long> observerIDs = new ArrayList<>();
        observerIDs.add(0L);
        if(id == -1){
            GatewayServer gateway = new GatewayServer("localhost", 8888, peerIDToUDPAddress, 1);
            gateway.start();
        }else{
            ZooKeeperPeerServerImpl server = new ZooKeeperPeerServerImpl(port, 0, id, peerIDToUDPAddress, 1);
            server.start();
        }
    }
}