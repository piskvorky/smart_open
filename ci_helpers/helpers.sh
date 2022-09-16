#!/bin/bash

set -e
set -x

enable_moto_server(){
  moto_server -p5000 2>/dev/null&
}

create_ftp_server(){
  docker run -d -p 21:21 -p 21000-21010:21000-21010 -e USERS="user|123|/home/user/dir" -e ADDRESS=localhost --name my-ftp-server delfer/alpine-ftp-server 
}

disable_moto_server(){
  lsof -i tcp:5000 | tail -n1 | cut -f2 -d" " | xargs kill -9
}

delete_ftp_server(){
  docker kill my-ftp-server
  docker rm my-ftp-server
}

"$@"
