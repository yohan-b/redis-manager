#!/bin/bash

# Paramètres pouvant être modifiés :
# MANAGERS : Liste des managers redis.
# MANAGER_PORT : Port du manager.
# DURATION : Temps approximatif du reboot.
# 
# Ensuite dans le script il y a un sleep 30 et un count qui va jusqu’à 10, à modifier si on a besoin de plus de temps pour que le cluster soit modifié.
# Les logs (sur RHEL6 et RHEL7) sont envoyés dans /var/log/cron si le lancement de ce script est ajouté en crontab.

############
MANAGERS="10.166.119.48 10.166.119.47"
MANAGER_PORT=45000
############
DURATION=300
WAIT=1

SERVER_IP=$(ip route get 10 | head -n 1 | awk '{print $NF}')

echo "Preparing redis for reboot."

for MANAGER in ${MANAGERS}; do
    OUTPUT_ROLE=$(timeout 1 curl -s ${MANAGER}:${MANAGER_PORT}/manager_status 2>&1)
    echo ${OUTPUT_ROLE} | grep -q 'active'
    if [ $? == 0 ]; then
        ACTIVE_MANAGER=${MANAGER}
        break
    else
        continue
    fi
done

if [ -z ${ACTIVE_MANAGER} ]; then
    echo "No active manager found."
    echo "Managers tried : ${MANAGERS}"
    exit 2
else
    echo "Active manager is : ${ACTIVE_MANAGER}"
fi

COUNT=0
while [ $WAIT == 1 ]; do
    echo "Asking ${MANAGER}..."
    OUTPUT=$(timeout 1 curl -s ${MANAGER}:${MANAGER_PORT}/prepare_for_reboot/${SERVER_IP}\&duration=${DURATION} 2>&1)
    echo "$OUTPUT" | grep -q 'DONE' 
    if [ $? == 0 ]; then
        WAIT=0
    else
        COUNT=$(($COUNT+1))
        echo $COUNT
        sleep 30
    fi
    if [ $COUNT == 10 ]; then
        echo "Error, manager taking too long to modify the cluster"
        exit 2
    fi
done

echo "Ready for reboot"
