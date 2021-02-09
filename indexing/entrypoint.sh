#!/bin/bash

# Start the run once job.
echo "Docker container has been started. Adding cron job to scheduler.txt"

echo "Retrieving enviromental variables and saving them"
env > /usr/src/app/env.txt

# Setup a cron schedule
echo "0 1 * * 0,2-6 INDEX_DELETE=false /usr/src/app/cron.sh >> /var/log/cron.log 2>&1
0 1 * * 1 INDEX_DELETE=true /usr/src/app/cron.sh >> /var/log/cron.log 2>&1
# This extra line makes it a valid cron" > scheduler.txt

echo "Running cat scheduler.txt:"
cat scheduler.txt

crontab scheduler.txt

echo "Running crontab -l"
crontab -l

echo "Running cron -f"
cron -f