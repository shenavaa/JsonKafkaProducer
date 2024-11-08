#!/bin/bash
sudo yum -y install nfs-utils
sudo service nfs start
sudo service nfs-server start
sudo mkdir -p /mnt/efs
sudo chmod 777 -R /mnt/efs
sudo mount -t nfs -o nfsvers=4.1,rsize=1048576,wsize=1048576,hard,timeo=600,retrans=2,noresvport fs-058c7f4a20a3a4573.efs.us-east-1.amazonaws.com:/ /mnt/efs
sudo mkdir -p /mnt/tcpdump/
( cd /mnt/tcpdump && sudo nohup tcpdump -C 1000 -s 2000 -w nfs_pcap_$(date +%FT%T).pcap -i any -z gzip -Z root port 2049 & )

if [ ! -d "/mnt/efs/checkpoint/" ]; then 
 sudo mkdir /mnt/efs/checkpoint
 sudo chown yarn.hadoop /mnt/efs/checkpoint/
fi
