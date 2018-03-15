#!/usr/bin/env bash
hadoop dfs -rm -r out
git fetch
git rebase
gradle jar
hadoop jar build/libs/hw1.jar InpFormat.WordCountJob /data/hw1/*.pkz out.txt
