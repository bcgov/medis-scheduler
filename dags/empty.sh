#!/bin/sh
echo "Check upload directory is empty"

if timeout 30 ls -1 "${DATA_DIR}" >/tmp/ls_output 2>/dev/null; then
    if [ ! -s /tmp/ls_output ]; then
        echo "Empty"
        exit 0
    else
        echo "Not Empty"
        cat /tmp/ls_output
        exit 1
    fi
else
    echo "ERROR: Unable to access ${DATA_DIR} (timeout)"
    exit 2
fi