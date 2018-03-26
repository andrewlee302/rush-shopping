#!/usr/bin/env zsh 
export APP_HOST="localhost"
export APP_PORT="10000"
export COORD_HOST="localhost"
export COORD_PORT="9999"
export KVSTORE_HOST="localhost"
#export KVSTORE_PORTS="10001,10002,10003"
export KVSTORE_PORTS="10001"
export ITEM_CSV="data/items.csv"
export USER_CSV="data/users.csv"
export TIMEOUT_MS="50"
go build main.go
#if [[ $? == 0 ]] then; ./main -cpuprofile=main.prof;fi
if [[ $? == 0 ]] then; ./main ;fi
