#!/bin/sh

cargo +nightly build --release

rm -rf test_logs/
target/release/hermod --logdir test_logs &
sleep 1
hermod_pid=`pgrep hermod`

target/release/test_client --name test_consume_test1 --group test1 --topic test_top1 --consume --count 100000 &
pid=`pgrep test_client`
#date
target/release/test_client --name test_publish_test1 --group test1 --topic test_top1 --publish --count 100000
#date

# XXX Add test to kill server as soon as publisher ends- make sure all data is in log.
wait $pid

target/release/test_client --name test2_consume_test2 --group test2 --topic test_top2 --consume --count 400000 &
pid=`pgrep test_client`
target/release/test_client --name test2_publish1_test2 --group test2 --topic test_top2 --publish --count 100000
target/release/test_client --name test2_publish2_test2 --group test2 --topic test_top2 --publish --count 100000
target/release/test_client --name test2_publish3_test2 --group test2 --topic test_top2 --publish --count 100000
target/release/test_client --name test2_publish4_test2 --group test2 --topic test_top2 --publish --count 100000

wait $pid

kill $hermod_pid

md5sum test_logs/test_top1.0.log
echo "58e84656b6e53a74301993e874ad7111  test_logs/test_top1.0.log"
