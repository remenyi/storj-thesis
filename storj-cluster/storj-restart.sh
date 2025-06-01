#!/bin/bash

set -e

SN0_IP=""
SN1_IP=""
SN2_IP=""
SN3_IP=""
SN4_IP=""

node_names=("sn1" "sn2" "sn3" "sn4")
node_ips=("$SN1_IP" "$SN2_IP" "$SN3_IP" "$SN4_IP")

echo "Destroying the existing network..."

ssh -i ~/.ssh/gcp-key remenyigergo@sn0 sudo systemctl stop satellite

for node in sn1 sn2 sn3 sn4; do
    ssh -i ~/.ssh/gcp-key remenyigergo@$node sudo systemctl stop storagenode
done

for node in sn0 sn1 sn2 sn3 sn4; do
    echo "Destroying network on $node..."
    ssh -i ~/.ssh/gcp-key remenyigergo@$node 'sudo journalctl --rotate && sudo journalctl --vacuum-time=1s'
    ssh -i ~/.ssh/gcp-key remenyigergo@$node "export PATH=\$PATH:/home/remenyigergo/go/bin/:/usr/bin/; storj-sim network destroy"
done

ssh -i ~/.ssh/gcp-key remenyigergo@sn0 "export PATH=\$PATH:/home/remenyigergo/go/bin/:/usr/bin/; storj-sim network setup --postgres=postgres://storj:storj@localhost:5432/storj?sslmode=disable --storage-nodes=5 --satellites=1"

ssh -i ~/.ssh/gcp-key remenyigergo@sn0 sed -i "s/server.address:\ 127.0.0.1:10000/server.address:\ ${SN0_IP}:10000/" /home/remenyigergo/.local/share/storj/local-network/satellite/0/config.yaml

echo "Updating satellite k/m/o/n - sharesize"
ssh -i ~/.ssh/gcp-key remenyigergo@sn0 sed -i 's+#\ metainfo.rs:\ 4/6/8/10-256\ B+metainfo.rs:\ 4/6/8/10-256\ B+' /home/remenyigergo/.local/share/storj/local-network/satellite/0/config.yaml

SATELLITE_URL=$(ssh -i ~/.ssh/gcp-key remenyigergo@sn0 "export PATH=\$PATH:/home/remenyigergo/go/bin/:/usr/bin/; storj-sim network env --storage-nodes=5" | grep "SATELLITE_0_URL=" | cut -d"=" -f2)

echo "Updating storage node config on sn0..."
ssh -i ~/.ssh/gcp-key remenyigergo@sn0 sed -i "s/storage2.trust.sources:\ .*/storage2.trust.sources:\ $SATELLITE_URL/" /home/remenyigergo/.local/share/storj/local-network/storagenode/{0..4}/config.yaml
ssh -i ~/.ssh/gcp-key remenyigergo@sn0 sed -i "s/server.address:\ 127.0.0.1/server.address:\ ${SN0_IP}/" /home/remenyigergo/.local/share/storj/local-network/storagenode/{0..4}/config.yaml

ssh -i ~/.ssh/gcp-key remenyigergo@sn0 sudo systemctl restart satellite

sleep 20

for i in "${!node_names[@]}"; do
    node="${node_names[$i]}"
    ip="${node_ips[$i]}"

    echo "Updating storage node config on $node..."
    ssh -i ~/.ssh/gcp-key remenyigergo@$node "export PATH=\$PATH:/home/remenyigergo/go/bin/:/usr/bin/; storj-sim network setup --postgres=postgres://storj:storj@localhost:5432/storj?sslmode=disable --storage-nodes=5 --satellites=0 --no-gateways"
    
    ssh -i ~/.ssh/gcp-key remenyigergo@$node sed -i "s/server.address:\ 127.0.0.1/server.address:\ ${ip}/" /home/remenyigergo/.local/share/storj/local-network/storagenode/{0..4}/config.yaml

    ssh -i ~/.ssh/gcp-key remenyigergo@$node sed -i "s/storage2.trust.sources:\ .*/storage2.trust.sources:\ $SATELLITE_URL/" /home/remenyigergo/.local/share/storj/local-network/storagenode/{0..4}/config.yaml
    
    ssh -i ~/.ssh/gcp-key remenyigergo@$node sed -i "s/#\ log.development:\ .*/log.development:\ false/" /home/remenyigergo/.local/share/storj/local-network/storagenode/{0..4}/config.yaml
    
    ssh -i ~/.ssh/gcp-key remenyigergo@$node sudo systemctl restart storagenode
    sleep 5
done

ACCESS_VALUE=$(ssh -i ~/.ssh/gcp-key remenyigergo@sn0 'grep "^access:" /home/remenyigergo/.local/share/storj/local-network/gateway/0/config.yaml | cut -d" " -f2')

GATEWAY_ACCESS_KEY=$(ssh -i ~/.ssh/gcp-key remenyigergo@sn0 'export PATH=\$PATH:/home/remenyigergo/go/bin/:/usr/bin/; storj-sim network env --storage-nodes=5 | grep "GATEWAY_0_ACCESS_KEY=" | cut -d"=" -f2')
GATEWAY_SECRET_KEY=$(ssh -i ~/.ssh/gcp-key remenyigergo@sn0 'export PATH=\$PATH:/home/remenyigergo/go/bin/:/usr/bin/; storj-sim network env --storage-nodes=5 | grep "GATEWAY_0_SECRET_KEY=" | cut -d"=" -f2')


sed -i '' "s/--access=.*/--access=${ACCESS_VALUE} \\\\/" gateway-deployment.yaml
sed -i '' "s/--minio.access-key=.*/--minio.access-key=${GATEWAY_ACCESS_KEY} \\\\/" gateway-deployment.yaml
sed -i '' "s/--minio.secret-key=.*/--minio.secret-key=${GATEWAY_SECRET_KEY} \\\\/" gateway-deployment.yaml

kubectl apply -f gateway-deployment.yaml -n storj

sed -i '' "s/access-grant:.*/access-grant:\ ${ACCESS_VALUE}/" gateway-configmap.yaml
sed -i '' "s/minio-access-key:.*/minio-access-key:\ ${GATEWAY_ACCESS_KEY}/" gateway-configmap.yaml
sed -i '' "s/minio-secret-key:.*/minio-secret-key:\ ${GATEWAY_SECRET_KEY}/" gateway-configmap.yaml

kubectl apply -f gateway-configmap.yaml -n jupyterhub
kubectl apply -f gateway-configmap.yaml -n spark

echo "========================"
echo "SATELLITE_0_URL: $SATELLITE_URL"
echo "Gateway access value: $ACCESS_VALUE"
echo "GATEWAY_0_ACCESS_KEY: $GATEWAY_ACCESS_KEY"
echo "GATEWAY_0_SECRET_KEY: $GATEWAY_SECRET_KEY"
echo "========================"

