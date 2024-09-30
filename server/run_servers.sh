#!/bin/bash

# Start 5 servers in the background
mvn exec:java -Dexec:args="8080 0" > logs/server1.log 2>&1 &
echo "Server 1 started on background with PID $!"

mvn exec:java -Dexec:args="8080 1" > logs/server2.log 2>&1 &
echo "Server 2 started on background with PID $!"

mvn exec:java -Dexec:args="8080 2" > logs/server3.log 2>&1 &
echo "Server 3 started on background with PID $!"

mvn exec:java -Dexec:args="8080 3" > logs/server4.log 2>&1 &
echo "Server 4 started on background with PID $!"

mvn exec:java -Dexec:args="8080 4" > logs/server5.log 2>&1 &
echo "Server 5 started on background with PID $!"

# Wait for all servers to finish (optional)
wait

