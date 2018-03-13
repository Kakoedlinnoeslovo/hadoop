#!/usr/bin/env bash
git add -A
NEW_UUID=$(cat /dev/urandom | tr -dc 'a-zA-Z0-9' | fold -w 32 | head -n 1)
git commit -m NEW_UUID
git push origin master
