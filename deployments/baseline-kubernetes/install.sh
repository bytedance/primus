#!/usr/bin/env bash

set -euo pipefail
cd "$(dirname "$0")"

USERNAME=$1

# Install Primus deployment
mkdir -p /usr/lib/primus-kubernetes
chown $USERNAME /usr/lib/primus-kubernetes
runuser -u $USERNAME -- cp -r ../../output/* /usr/lib/primus-kubernetes

# Docker images
/usr/lib/primus-kubernetes/resources/containers/build.sh
runuser -u $USERNAME kind load docker-image primus-baseline-init primus-baseline-base

# Permissions
runuser -u $USERNAME -- kubectl apply -f /usr/lib/primus-kubernetes/resources/cluster/permission.yaml

# Networking
# NOTE: Kubernetes ingress controllers are not interchangeable; for instance, Ambassador ingress
# controller trims API paths after forwarding, while Nginx ingress controller leaves API path
# intact.
runuser -u $USERNAME -- kubectl apply -f https://github.com/datawire/ambassador-operator/releases/latest/download/ambassador-operator-crds.yaml
runuser -u $USERNAME -- kubectl apply -n ambassador -f https://github.com/datawire/ambassador-operator/releases/latest/download/ambassador-operator-kind.yaml
runuser -u $USERNAME -- kubectl wait --timeout=180s -n ambassador --for=condition=deployed ambassadorinstallations/ambassador
runuser -u $USERNAME -- kubectl apply -f /usr/lib/primus-kubernetes/resources/cluster/networking.yaml

# Affiliating servers
sed -i "s/<USER>/$USERNAME/" /usr/lib/primus-kubernetes/resources/systemd/primus-on-kubernetes-container-log.service
cp /usr/lib/primus-kubernetes/resources/systemd/primus-on-kubernetes-container-log.service /etc/systemd/system/
systemctl enable primus-on-kubernetes-container-log
systemctl start primus-on-kubernetes-container-log

sed -i "s/<USER>/$USERNAME/" /usr/lib/primus-kubernetes/resources/systemd/primus-on-kubernetes-history.service
cp /usr/lib/primus-kubernetes/resources/systemd/primus-on-kubernetes-history.service /etc/systemd/system/
systemctl enable primus-on-kubernetes-history
systemctl start primus-on-kubernetes-history
