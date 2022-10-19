#!/usr/bin/env bash

set -euxo pipefail
cd "$(dirname "$0")"

USERNAME=$1

# Install Primus deployment
mkdir -p /usr/lib/primus-yarn
chown $USERNAME /usr/lib/primus-yarn
runuser -u $USERNAME -- cp -r ../../output/* /usr/lib/primus-yarn

# Affiliating servers
sed -i "s/<USER>/$USERNAME/" /usr/lib/primus-yarn/resources/systemd/primus-on-yarn-history.service
cp /usr/lib/primus-yarn/resources/systemd/primus-on-yarn-history.service /etc/systemd/system/
systemctl enable primus-on-yarn-history
systemctl start primus-on-yarn-history
