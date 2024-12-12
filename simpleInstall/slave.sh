#!/bin/bash



function init_db() {

 info "[init slave datanode.]"
   gs_initdb -D $HOME/slave --nodename=datanode2 -E UTF-8 --locale=en_US.UTF-8 -U $user  -w $password

}

function master_standby_install() {
    init_db
    config_db
    start_db
}


function main() {
	 master_standby_install
}
main $@
