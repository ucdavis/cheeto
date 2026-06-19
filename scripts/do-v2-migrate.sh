#!/usr/bin/env zsh
set -u

SHARED_OPTS="--config $1"

echo cheeto ng migrate all $SHARED_OPTS --sites farm franklin hive --drop --log 

echo cheeto ng user type $SHARED_OPTS --type shared --user hpccf
echo cheeto ng user add access $SHARED_OPTS --user hpccf --access login-ssh

echo cheeto ng group remove member $SHARED_OPTS -g gquon -s farm -u bababuck
echo cheeto ng group remove member $SHARED_OPTS -g gquon -s farm -u yc55

echo cheeto ng user new system $SHARED_OPTS --fullname "Cheeto Daemon" --email hpccf@ucdavis.edu --user cheeto-user
echo cheeto ng user add access $SHARED_OPTS --user cheeto-user --access root-ssh compute-ssh sudo login-ssh
echo cheeto ng user add site $SHARED_OPTS --user cheeto-user --site farm
echo cheeto ng user add site $SHARED_OPTS --user cheeto-user --site franklin
echo cheeto ng user add site $SHARED_OPTS --user cheeto-user --site hive
echo cheeto ng group add member $SHARED_OPTS -g hpccfgrp -s farm -u cheeto-user
echo cheeto ng group add member $SHARED_OPTS -g hpccfgrp -s franklin -u cheeto-user
echo cheeto ng group add member $SHARED_OPTS -g hpccfgrp -s hive -u cheeto-user
