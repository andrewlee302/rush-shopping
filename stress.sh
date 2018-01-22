#!/usr/bin/env zsh 
export APP_HOST="localhost"
export APP_PORT="10002"
export KVSTORE_HOST="localhost"
export KVSTORE_PORT="10001"
export ITEM_CSV="data/items.csv"
export USER_CSV="data/users.csv"
go build benchmark/stress.go
if [[ $? == 0 ]] then; ./stress -d -c 300;fi
