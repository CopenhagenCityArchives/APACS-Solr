#!/bin/bash
echo "In production, running index scripts now";

echo "$(date): running erindringer.py" >> /var/log/cron.log 2>&1;
/usr/local/bin/python3 /usr/src/app/erindringer.py >> /var/log/cron.log 2>&1;
echo "$(date): running efterretninger.py" >> /var/log/cron.log 2>&1;
/usr/local/bin/python3 /usr/src/app/efterretninger.py >> /var/log/cron.log 2>&1;
#echo "running schools.py";
#/usr/local/bin/python3 /usr/src/app/schools.py > /proc/1/fd/1;
echo "$(date): running burials.py" >> /var/log/cron.log 2>&1;
/usr/local/bin/python3 /usr/src/app/burials.py >> /var/log/cron.log 2>&1;
echo "$(date): running police.py" >> /var/log/cron.log 2>&1;
/usr/local/bin/python3 /usr/src/app/police.py >> /var/log/cron.log 2>&1;
#