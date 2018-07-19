
CLASSES="\
1000;100;default:1\
|applog_recsvc:5000\
|applog_sayl:10000\
|applog_purest:75000\
|applog_admin:3000\
|applog_supply_chain_services:75000\
|applog_logs:18000\
|syslog_haproxy:10000\
|syslog_cisco:500\
|syslog_logs:7000\
|syslog_influxdb:7000\
|syslog_ftpd:30000\
|syslog_hypernova:500\
|edilog:15000\
|sqlserverlog:500\
"

RULES="[\
{\"application=recsvc\":\"applog_recsvc\"}\
,{\"application=sayl\": \"applog_sayl\"}\
,{\"wf_index_type=applog_purest\": \"applog_purest\"}\
,{\"wf_index_type=applog_admin\": \"applog_admin\"}\
,{\"wf_index_type=applog_supply_chain_services\": \"applog_supply_chain_services\"}\
,{\"wf_index_type=applog_logs\": \"applog_logs\"}\
,{\"wf_index_type=syslog_haproxy\": \"syslog_haproxy\"}\
,{\"wf_index_type=syslog_logs\": \"syslog_logs\"}\
,{\"wf_index_type=syslog_influxdb\": \"syslog_influxdb\"}\
,{\"wf_index_type=syslog_ftpd\": \"syslog_ftpd\"}\
,{\"wf_index_type=syslog_hypernova\": \"syslog_hypernova\"}\
,{\"wf_index_type=edilog\": \"edilog\"}\
,{\"wf_index_type=sqlserverlog\": \"sqlserverlog\"}\
,{\"tags:cisco && ! tags=/cisco/\": \"sqlserverlog\"}\
]"


echo "example input:"
echo '{"a": 1}'
echo '{"b": 2}'
echo '{"c": 3}'

K_HOST=kafkac4n1.dev.bo1.csnzoo.com
K_TOPIC=applogIngest
# echo "Forwarding port 9092 from kafkac4n1.dev.bo1.csnzoo.com"

cargo run -- --input kafka --input-config "demo2|$K_TOPIC|127.0.0.1:9092" --output stdout --parser json --classifier mimir --classifier-config "${RULES}" --grouping bucket  --grouping-config "${CLASSES}"


