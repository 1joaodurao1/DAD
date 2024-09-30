#!/bin/bash

# Check if the user provided an input
if [ -z "$1" ]; then
  echo "Usage: $0 <number_of_clients>"
  exit 1
fi

# Get the number of clients to start from the argument
NUM_clients=$1

# Loop through and start the required number of clients
for ((i=1; i<=NUM_clients; i++)); do
  mvn exec:java > "logs/client$i.log" 2>&1 &
  echo "Client $i started in the background with PID $!"
done

# Optionally, wait for all clients to finish (remove this if you don't want to block the terminal)
wait

