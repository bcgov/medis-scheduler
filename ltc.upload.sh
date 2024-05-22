#!/bin/sh
TS=`date +%Y%m%d%H%M%S`
cd /data/encrypted/ltc
echo -e "Files to be uploaded\n"
ls -l
echo -e "Creating medis_ltc.flag file"
DOMAIN_FILE=medis_domain_values_`date +%Y%m%d%H%M%S`.txt
cp /data/domains/medis_domain_values.txt ${DOMAIN_FILE}
cat ltc*.flag > medis_ltc.flag
echo ${DOMAIN_FILE} >> medis_ltc.flag
echo -e "\nMoH SFTP server output\n"
sftp -oHostKeyAlgorithms=+ssh-rsa -oPubkeyAcceptedAlgorithms=+ssh-rsa -oUserKnownHostsFile=/ssh-config/known_hosts -oIdentityFile=/ssh-hi/medis-etl-ssh-key ${SFTP_CONNECTION} <<EOF
cd ${SFTP_UPLOAD_DIR}
put *.gpg
put ${DOMAIN_FILE}
put medis_ltc.flag
quit
EOF
SFTP_RETURN_CODE=${?}
if [[ 0 != ${SFTP_RETURN_CODE} ]]
  then
  echo -e "SFTP file upload failed"
  exit ${SFTP_RETURN_CODE}
else
  echo -e "Files upload was successful, moving the files into archive"
  mkdir ../../archive/${TS}
  mv * ../../archive/${TS}
  echo -e "\nDeleting old files from archive directory"
  cd ../../archive
  find -type f -mtime +${ARCHIVE_RETENTION_DAYS} -exec rm {} \;
  echo -e "END"
fi
exit 0
