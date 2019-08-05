#!/bin/sh

rm -rf test_logs/
target/debug/hermod --logdir test_logs &
sleep 1
hermod_pid=`pgrep hermod`

target/debug/test_client --name test_consume_test1 --group test1 --topic test_top1 --consume --count 100000 &
pid=`pgrep test_client`
date
target/debug/test_client --name test_publish_test1 --group test1 --topic test_top1 --publish --count 100000
date

# XXX Add test to kill server as soon as publisher ends- make sure all data is in log.
wait $pid

kill $hermod_pid

md5sum test_logs/test_top1.0.log
echo "58e84656b6e53a74301993e874ad7111  test_logs/test_top1.0.log"
