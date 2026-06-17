#!/bin/zsh

cd

cheeto db site new --sitename test-cluster --fqdn test-cluster.hpc.ucdavis.edu
cheeto db user add site -u camw omen -s test-cluster
cheeto db group add site -g hpccfgrp -s test-cluster
cheeto db group add member -g hpccfgrp -s test-cluster -u camw
cheeto db group add sponsor -g hpccfgrp -s test-cluster -u camw omen
cheeto db group add site -g sponsors -s test-cluster
cheeto db group add sponsor -g sponsors -s test-cluster -u camw omen
cheeto db group add member -g sponsors -s test-cluster -u camw omen
cheeto db storage new collection -s test-cluster --name group --host nas0 --prefix /nas0/export/group --quota 100G --options rw,no_root_squash,sync,no_subtree_check,crossmnt --ranges 10.0.0.0/16 --log --log-level INFO
cheeto db storage new collection -s test-cluster --name home --host nas0 --prefix /nas0/export/home --quota 20G --options rw,no_root_squash,sync,no_subtree_check,crossmnt --ranges 10.0.0.0/16 --log --log-level INFO
cheeto db storage new automountmap -s test-cluster -n group --log --log-level INFO
cheeto db storage new automountmap -s test-cluster -n home --log --log-level INFO
cheeto db storage new automountmap -s test-cluster -n share --log --log-level INFO
cheeto db storage new storage -s test-cluster --name hpccfgrp --owner camw --group hpccfgrp --host nas0 --path /nas0/export/group/hpccfgrp --table group --quota 1T

#mkdir -p /home/camw/puppet.hpc-accounts/domains/test-cluster.hpc.ucdavis.edu/merged/
