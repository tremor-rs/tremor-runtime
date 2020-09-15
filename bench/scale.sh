
if [ -f "$1" ]
then
	TARGET=$1
else
	TARGET=target/release/tremor-server
fi

echo "(3 cores)"
for i in 1 2 4 8 16 32 64 
do
	taskset -c 0,1,2 $TARGET --no-api -c ./bench/real-workflow-throughput-json-$i-$i-1.yaml bench/link.yaml 2>&1 | grep Throughput | sed -e 's/Throughput: //' -e 's; MB/s;;'
done

echo "(all cores)"
for i in 1 2 4 8 16 32 64
do
	$TARGET --no-api -c ./bench/real-workflow-throughput-json-$i-$i-1.yaml bench/link.yaml 2>&1 | grep Throughput | sed -e 's/Throughput: //' -e 's; MB/s;;'
done
