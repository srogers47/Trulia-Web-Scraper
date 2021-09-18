#!/bin/bash 

# Test script to handle  ip routing in expressvpn CLI. 
expressvpn disconnect
expressvpn connect ${random_alias}
