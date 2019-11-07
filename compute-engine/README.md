### Running IB Gateway on a Compute Engine with remote access. 

##### 1. Create Firewall Rule that allows ingress from whitelists 

The following command produces a sorted list of firewall rules for a given network
```console
gcloud compute firewall-rules list --filter network=default \
    --sort-by priority \
    --format="table(
        name,
        network,
        direction,
        priority,
        sourceRanges.list():label=SOURCE_RANGES,
        destinationRanges.list():label=DESTINATION_RANGES,
        allowed[].map().firewall_rule().list():label=ALLOW,
        denied[].map().firewall_rule().list():label=DENY,
        sourceTags.list():label=SRC_TAGS,
        targetTags.list():label=TARGET_TAGS
        )"
```
Check if the Firewall Rules *default-allow-http* and *default-allow-https* allow trafic from all ip addresses (SRC_RANGES=0.0.0.0/0), protocals and ports (ALLOW=all).
If not, update the rules with the following commands. 

```console
gcloud compute firewall-rules update default-allow-http --source-ranges=0.0.0.0/0 --rules=all
gcloud compute firewall-rules update default-allow-https --source-ranges=0.0.0.0/0 --rules=all
```

Create a custom Firewall Rule for the compute engine with a higher priority the. Whitelist your IP address(es), set ports to use with VNC and IB Gateway. 
```console
gcloud compute firewall-rules create gateway \
    --action=ALLOW \
    --description="vnc connect with Gateway Compute Engine via port 5900. Connect to IB Gateway instances via port 4001, 4002 " \
    --direction=INGRESS \
    --network=default \
    --rules=tcp:5900,tcp:4001-4002 \
    --priority=900 \
    --target-tags=gateway \
    --source-ranges=213.127.124.141
```

##### 1. Run following in Cloud Shell to create a Compute Engine with Ubuntu 18.04 image

Rename INSTANCE_NAME to the name of the instance (do this twice: also at --boot-disk-device-name). Name must be lowercase letters, numbers and hyphens
```console
gcloud beta compute \
--project=algo-trading-240010 instances create $INSTANCE_NAME \
--zone=us-east4-a \
--machine-type=n1-highcpu-2 \
--subnet=default \
--network-tier=PREMIUM \
--maintenance-policy=MIGRATE --service-account=1094603561253-compute@developer.gserviceaccount.com \
--scopes=https://www.googleapis.com/auth/devstorage.read_only,https://www.googleapis.com/auth/logging.write,https://www.googleapis.com/auth/monitoring.write,https://www.googleapis.com/auth/servicecontrol,https://www.googleapis.com/auth/service.management.readonly,https://www.googleapis.com/auth/trace.append \
--tags=http-server,https-server,gateway \
--image=ubuntu-1804-bionic-v20191021 \
--image-project=ubuntu-os-cloud 
--boot-disk-size=10GB \
--boot-disk-type=pd-standard \
--boot-disk-device-name= $INSTANCE_NAME \
--reservation-affinity=any
```


```console
/usr/bin/Xvfb :0 -ac -screen 0 1024x768x24 &
/usr/bin/x11vnc -ncache 10 -ncache_cr -viewpasswd remote_view_only_pass -passwd some_pass123  -display :0 -forever -shared -logappend /var/log/x11vnc.log -bg -noipv6
wget https://download2.interactivebrokers.com/installers/ibgateway/latest-standalone/ibgateway-latest-standalone-linux-x64.sh && chmod a+x ibgateway-latest-standalone-linux-x64.sh
DISPLAY=:0 ./ibgateway-latest-standalone-linux-x64.sh
```



