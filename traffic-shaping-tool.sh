#!/bin/sh
set -x
SRC_HOSTS=${SRC_HOSTS:-127.0.0.1:9092}
DST_HOSTS=${DST_HOSTS:-$SRC_HOSTS}
SRC_TOPIC=${SRC_TOPIC:-src}
DST_TOPIC=${DST_TOPIC:-dst}
GROUP_ID=${GROUP_ID:-traffic-shaping-tool}
P=${P:-1.0}
./traffic-shaping-tool --src "${SRC_HOSTS}" --src-topic "${SRC_TOPIC}" --dst "${DST_HOSTS}" --dst-topic "${DST_TOPIC}" --group-id "${GROUP_ID}" -p "${P}"
sleep 5000
