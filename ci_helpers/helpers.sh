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
  home_dir=/home/user
  user=user
  pass=123
  ftp_port=21
  ftps_port=90

  mkdir $home_dir
  useradd -p $(echo $pass | openssl passwd -1 -stdin) -d $home_dir $user
  chown $user:$user $home_dir
  openssl req -x509 -nodes -new -sha256 -days 10240 -newkey rsa:2048 -keyout /etc/vsftpd.key -out /etc/vsftpd.pem -subj "/C=ZA/CN=localhost"
  chmod 755 /etc/vsftpd.key
  chmod 755 /etc/vsftpd.pem

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
rsa_cert_file=/etc/vsftpd.pem
rsa_private_key_file=/etc/vsftpd.key
ssl_enable=YES
allow_anon_ssl=NO
force_local_data_ssl=NO
force_local_logins_ssl=NO
require_ssl_reuse=NO
'''

  cp /etc/vsftpd.conf /etc/vsftpd-ssl.conf
  echo -e "$server_setup\nlisten_port=${ftp_port}" >> /etc/vsftpd.conf
  echo -e "$server_setup\nlisten_port=${ftps_port}\n$additional_ssl_setup" >> /etc/vsftpd-ssl.conf

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
