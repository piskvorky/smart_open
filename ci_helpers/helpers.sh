#!/bin/bash

set -e
set -x

enable_moto_server(){
  moto_server -p5000 2>/dev/null&
}

create_ftp_ftps_servers(){
  #
  # Must be run as root
  #
  mkdir $HOME_DIR
  useradd -p $(echo $PASS | openssl passwd -1 -stdin) -d $HOME_DIR $USER
  chown $USER:$USER $HOME_DIR

  server_setup='''
listen=YES
listen_ipv6=NO
write_enable=YES
pasv_enable=YES
pasv_min_port=40000
pasv_max_port=40009
chroot_local_user=YES
allow_writeable_chroot=YES'''

  additional_ssl_setup='''
ssl_enable=YES
allow_anon_ssl=NO
force_local_data_ssl=NO
force_local_logins_ssl=NO
require_ssl_reuse=NO
'''

  cp /etc/vsftpd.conf /etc/vsftpd-ssl.conf
  echo -e "$server_setup\nlisten_port=${FTP_PORT}" >> /etc/vsftpd.conf
  echo -e "$server_setup\nlisten_port=${FTPS_PORT}\n$additional_ssl_setup" >> /etc/vsftpd-ssl.conf

  service vsftpd restart
  vsftpd /etc/vsftpd-ssl.conf &
}

disable_moto_server(){
  lsof -i tcp:5000 | tail -n1 | cut -f2 -d" " | xargs kill -9
}

delete_ftp_ftps_servers(){
  service vsftpd stop
}

"$@"
