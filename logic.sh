#Mayer and Shai Bash Script

rm GossipMessagePaths.txt -f
printf "Running JUnit Tests...\n"
mvn test
wait $!
rm GossipMessagePaths.txt -f
printf "JUnit tests completed - sleeping 5 seconds...\n"
sleep 5
printf "Recompiling via Maven...\n"
mvn -q clean compile
declare -A idToProcessID

# start all servers (ServerRunner skips 0 internally)
for i in {-1..7}; do
  (java -cp target/classes edu.yu.cs.com3800.stage5.demo.DemoServerRunner $i & echo $! > pid)
  sleep 0.1
  idToProcessID[$i]=$(echo $(<pid))
  sleep 0.1
done

rm pid

# wait for election
clusterData="NULL"
while [[ $clusterData != *"LEADING"* ]]; do
  # TODO this formatting messes up the color coding below
  clusterData=$(curl -s -X GET http://localhost:8888/getclusterdata) #-s silent mode, -X IDK
done

# print cluster data
printf "Printing cluster data...\n$clusterData\n"

# send 9 requests in foreground
printf "Sending 9 requests...\n"
src="public class DemoSource{public String run(){return \"insert text\";}}"
responses=()
for i in {1..9}; do
  printf "Request #$i: $src\n"
  responses[${#responses[@]}]=$(curl -s -X POST http://localhost:8888/compileandrun -H "Content-Type: text/x-java-source" -d "$src")
done

printf "Printing responses...\n"
for i in {1..9}; do
  printf "Response #$i: ${responses[$(($i - 1))]}\n"
done

# kill follower with ID 1
killedFollower=1
printf "Killing server with ID $killedFollower...\n"
kill -9 ${idToProcessID[$killedFollower]}

# wait for follower to be confirmed failed
printf "Waiting for follower to be confirmed failed...\n"
while [[ $clusterData == *"<ID: $killedFollower"* ]]; do
  clusterData=$(curl -s -X GET http://localhost:8888/getclusterdata) #-s silent mode, -X IDK
done

# print cluster data
printf "Printing cluster data...\n$clusterData\n"

# kill the leader
printf "Killing the leader...\n"
killedLeader=$(echo $clusterData | grep -o "<ID: [0-9], State: LEADING>" | head -c 6 | tail -c 1)
kill -9 ${idToProcessID[$killedLeader]} 

#sleep
sleep 1




# send 9 requests in background
printf "Sending 9 requests in background...\n"
src="public class DemoSource{public String run(){return \"insert text\";}}"
backgroundIDs=()
for i in {1..9}; do
  printf "Request #$i of 9: $src\n"
  printf "Response #$i of 9: $(curl -s http://localhost:8888/compileandrun -H "Content-Type: text/x-java-source" -d "$src" &) \n" &
  backgroundIDs[${#backgroundIDs[@]}]=$!
done

# wait for new leader
printf "Waiting for new leader\n"
printf "Sleeping 45 seconds as required (3*15)...\n"
sleep 45
while [[ $clusterData == *"$killedLeader"* ]] || [[ $clusterData != *"LEADING"* ]]; do
  clusterData=$(curl -s http://localhost:8888/getclusterdata) #-s silent mode, -X IDK
done

echo new leader is $(echo $clusterData | grep -o "<ID: [0-9], State: LEADING>" | head -c 6 | tail -c 1)
wait $!

# wait on background processes
for backgroundID in "${backgroundIDs[@]}"; do
  wait $backgroundID
done

# send one more request and print response
printf "Sending 1 more request in foreground...\n"
src="public class DemoSource{public String run(){return \"insert text\";}}"
printf "Request #1 of 1: $src\n"
response=$(curl -s http://localhost:8888/compileandrun -H "Content-Type: text/x-java-source" -d "$src")
printf "Response #1 of 1: $response\n"



# kill all servers
printf "Killing all servers...\n"

kill ${idToProcessID["-1"]}


for i in {1..7}; do
  if [ $i -eq $killedFollower ]; then # skip killed follower
    continue
  fi
  if [ $i -eq $killedLeader ]; then # skip killed follower
    continue
  fi
  # TODO skip killed leader
  kill ${idToProcessID[$i]}
done


printf "Printing Gossip Message Paths... \n"
cat GossipMessagePaths.txt

kill $$