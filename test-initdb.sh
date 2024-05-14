#!/usr/bin/env bash

initdb -D ./tmp --auth-host=trust --set unix_socket_directories="" --set listen_addresses="*" --set log_min_messages="debug"  -U postgres
