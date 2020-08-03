
if [ -f "$1" ]
then
	TARGET=$1
else
	TARGET=target/release/tremor-server
fi

for i in 1 2 4 8 16 32 64
do
	echo "$i (3 cores)"
	taskset -c 0,1,2 $TARGET --no-api -c ./bench/real-workflow-throughput-json-$i-$i-1.yaml bench/link.yaml | grep Throughput
	echo "$i (all cores)"
	$TARGET --no-api -c ./bench/real-workflow-throughput-json-$i-$i-1.yaml bench/link.yaml | grep Throughput
done
