#!/bin/bash
# Add 'clickhouseserver.test' to '/etc/hosts' file if it is not already there
if ! grep -q '127.0.0.1[[:space:]]*clickhouseserver.test'  /etc/hosts; then 
  echo '127.0.0.1   clickhouseserver.test' | sudo tee -a /etc/hosts  
fi