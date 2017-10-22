#!/bin/bash
curl -s \
  -F "token=aarupdibcecykorfs86x8m73k66bxn" \
    -F "user=ubwdsiqay5p2s8a712druoy6ikpgko" \
    -F "message=aptlab job '$@' done" \
    https://api.pushover.net/1/messages.json
