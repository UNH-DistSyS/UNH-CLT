#!/usr/bin/env bash
rm node master processing
go build ../main/node/
go build ../main/master/
go build ../data_processing/processing
