#!/usr/bin/env zsh 
go build benchmark/stress.go
if [[ $? == 0 ]] then; ./stress -d -c 100 -d 10000;fi
