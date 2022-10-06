# DistributedJavaRunner

This project was written as part of the Yeshiva University COM3800 (Intro to Distributed Systems) course. 

Written in Java, this application compiles and runs arbitrary Java Source Code submitted via REST APIs. 

The cluster is managed using a modified Zookeeper Algorithm and Hearbeat Protocol. 

These Algorithms allow the cluster to redistribute work if a Follower node crashes and the cluster coordinates the promotion of a single node to the position of Leader should the leader fail.
