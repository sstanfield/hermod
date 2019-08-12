#!/bin/sh

cargo +nightly build --release

rm -rf test_logs/
target/release/hermod --logdir test_logs &
sleep 1
hermod_pid=`pgrep hermod`

target/release/test_client --name test_consume_test1 --group test1 --topic test_top1 --consume --count 400000 &
pid=`pgrep test_client`

target/release/test_client --name test_publish_test1 --group test1 --topic test_top1 --publish --count 400000

# XXX Add test to kill server as soon as publisher ends- make sure all data is in log.
wait $pid
if [[ $? -ne 0 ]]; then
    echo "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
    echo "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX FAIL XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
    echo "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
fi

target/release/test_client --name test2_consume_test2 --group test2 --topic test_top2 --consume --count 400000 &
pid=`pgrep test_client`
target/release/test_client --base_message CLIENT_1 --name test2_publish1_test2 --group test2 --topic test_top2 --publish --count 100000 &
target/release/test_client --base_message CLIENT_2 --name test2_publish2_test2 --group test2 --topic test_top2 --publish --count 100000 &
target/release/test_client --base_message CLIENT_3 --name test2_publish3_test2 --group test2 --topic test_top2 --publish --count 100000 &
target/release/test_client --base_message CLIENT_4 --name test2_publish4_test2 --group test2 --topic test_top2 --publish --count 100000

wait $pid
if [[ $? -ne 0 ]]; then
    echo "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
    echo "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX FAIL XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
    echo "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
fi

kill $hermod_pid

md5sum test_logs/test_top1.0.log
#echo "58e84656b6e53a74301993e874ad7111  test_logs/test_top1.0.log"
echo "930049ef9fc414d29365fc973d68bbf1  test_logs/test_top1.0.log"
