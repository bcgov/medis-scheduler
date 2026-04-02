#!/bin/sh
#
# Waits up to 90 minutes for the SFTP folder to become empty,
# polling every 5 minutes instead of failing immediately.
#
MAX_WAIT_MINUTES=90
POLL_INTERVAL_SECONDS=300  # 5 minutes
MAX_ATTEMPTS=$((MAX_WAIT_MINUTES * 60 / POLL_INTERVAL_SECONDS + 1))

ATTEMPT=1
while [ $ATTEMPT -le $MAX_ATTEMPTS ]; do
    echo "[Attempt $ATTEMPT/$MAX_ATTEMPTS] Checking if SFTP folder is empty..."
    
    /app/bin/emptysftp.sh
    EXIT_CODE=$?
    
    if [ $EXIT_CODE -eq 0 ]; then
        echo "SUCCESS: SFTP folder is empty. Proceeding with ETL."
        exit 0
    fi
    
    if [ $ATTEMPT -lt $MAX_ATTEMPTS ]; then
        echo "SFTP folder not empty. Waiting $((POLL_INTERVAL_SECONDS / 60)) minutes before retry..."
        sleep $POLL_INTERVAL_SECONDS
    else
        echo "TIMEOUT: SFTP folder still not empty after $MAX_WAIT_MINUTES minutes. Failing task."
        exit 1
    fi
    
    ATTEMPT=$((ATTEMPT + 1))
done

exit 1
