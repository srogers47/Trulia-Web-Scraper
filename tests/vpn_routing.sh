#!/bin/bash 

# Script to handle randomized ip routing in expressvpn CLI.  Prioritizes US networks/ip with low latency.

expressvpn connect ${vpn_alias}
