#!/bin/bash

# INFO:
# To access the http endpoint for a given servers log, make a request to the servers udp port + 1
# and the context will be /summary or /verbose.
# The end point for elections status/who is doing what is [the gateway address]/electionstatus
# a new output.log is made for each run

# 1. Build your code using mvn test, thus running your Junit tests.
echo "---" | tee output.log
echo "1 - running junit tests" | tee -a output.log
echo "---" | tee -a output.log
mvn clean test -Dtest=Stage5Test | tee -a output.log

# 2. Create a cluster of 7 peer servers and one gateway, starting each in their own JVM.
echo "---" | tee -a output.log
echo "2a - running demo script (via maven)" | tee -a output.log
echo "---" | tee -a output.log
mvn test -Dtest=Demo5Test -q -X | tee -a output.log
# (uses debugging mode for special features)

# 3. Wait until the election has completed before sending any requests to the Gateway. In order to do this, you must add
#       another http based service to the Gateway which can be called to ask if it has a leader or not. If the Gateway has a
#       leader, it should respond with the full list of nodes and their roles (follower vs leader). The script should print out the
#       list of server IDs and their roles.
# REFER TO DEMO JAVA SCRIPT

# 4. Once the gateway has a leader, send 9 client requests. The script should print out both the request and the response
#       from the cluster. In other words, you wait to get all the responses and print them out. You can either write a client in
#       java or use cURL.
# REFER TO DEMO JAVA SCRIPT

# 5. kill -9 a follower JVM, printing out which one you are killing. Wait heartbeat interval * 10 time, and then retrieve and
#       display the list of nodes from the Gateway. The dead node should not be on the list.
# REFER TO DEMO JAVA SCRIPT

# 6. kill -9 the leader JVM and then pause 1000 milliseconds. Send/display 9 more client requests to the gateway, in the
#       background
# REFER TO DEMO JAVA SCRIPT

# 7. Wait for the Gateway to have a new leader, and then print out the node ID of the leader. Print out the responses the
#       client receives from the Gateway for the 9 requests sent in step 6. Do not proceed to step 8 until all 9 requests have
#       received responses.
# REFER TO DEMO JAVA SCRIPT

# 8. Send/display 1 more client request (in the foreground), print the response.
# REFER TO DEMO JAVA SCRIPT

# 9. List the paths to files containing the Gossip messages received by each node.
# REFER TO DEMO JAVA SCRIPT

# 10. Shut down all the nodes
# REFER TO DEMO JAVA SCRIPT

echo "---" | tee -a output.log
echo "Demo complete" | tee -a output.log
echo "---" | tee -a output.log
