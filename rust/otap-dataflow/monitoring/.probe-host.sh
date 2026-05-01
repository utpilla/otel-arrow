#!/bin/bash
echo 'Default route in WSL:'
ip route | grep default
echo '---'
echo 'WSL eth0 IP:'
ip -4 addr show eth0 | grep inet
echo '---'
echo 'Trying to reach Windows host on common addresses:'
DEFAULT_GW=$(ip route | grep default | awk '{print $3}')
for ip in "$DEFAULT_GW" 172.17.144.1 host.docker.internal; do
    printf '  %s:8080 -> ' "$ip"
    code=$(curl -sS -o /dev/null -w '%{http_code}' --max-time 2 "http://$ip:8080/api/v1/telemetry/metrics?format=prometheus" 2>&1)
    echo "$code"
done
