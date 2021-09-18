#!/bin/bash

cd go
make cross-compile

for app in "$@"; do
  # ssh -o ClearAllForwardings=yes -tt $app /home/isucon/logs/parse.sh skip
  ssh -o ClearAllForwardings=yes $app sudo systemctl stop mysql isucholar.go.service nginx
  scp isucholar $app:/home/isucon/webapp/go/isucholar
  ssh -o ClearAllForwardings=yes $app sudo systemctl start mysql isucholar.go.service nginx
done
