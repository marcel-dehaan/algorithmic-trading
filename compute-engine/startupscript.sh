#! /bin/bash
apt update && \
apt upgrade -y && \
apt-get install -y unzip x11vnc xvfb &&
touch /var/log/x11vnc.log && chmod a+rw /var/log/x11vnc.log
