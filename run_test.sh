#!/bin/sh

cargo build --release

rm -rf test_logs/
target/release/hermod --logdir test_logs &
sleep 1
hermod_pid=`pgrep hermod`

echo "############### Testing 1 publisher / 1 client ##############"
target/release/test_client --name test_consume_test1 --group test1 --topic test_top1 --consume --count 400000 &
pid=`pgrep test_client`

target/release/test_client --name test_publish_test1 --group test1 --topic test_top1 --publish --count 400000

# XXX Add test to kill server as soon as publisher ends- make sure all data is in log.
wait $pid
if [[ $? -ne 0 ]]; then
    echo "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
    echo "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX FAIL XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
    echo "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
    kill $hermod_pid
    wait $hermod_pid
    exit 1
fi

echo "############### Testing 5 publishers/ 1 client ##############"
target/release/test_client --name test2_consume_test2 --group test2 --topic test_top2 --consume --count 500000 &
pid=`pgrep test_client`
target/release/test_client --base_message CLIENT_1 --name test2_publish1_test2 --group test2 --topic test_top2 --publish --count 100000 &
target/release/test_client --base_message CLIENT_2 --name test2_publish2_test2 --group test2 --topic test_top2 --publish --count 100000 &
target/release/test_client --base_message CLIENT_3 --name test2_publish3_test2 --group test2 --topic test_top2 --publish --count 100000 &
target/release/test_client --base_message CLIENT_4 --name test2_publish4_test2 --group test2 --topic test_top2 --publish --count 100000 &
target/release/test_client --base_message CLIENT_5 --name test2_publish5_test2 --group test2 --topic test_top2 --publish --count 100000

wait $pid
if [[ $? -ne 0 ]]; then
    echo "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
    echo "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX FAIL XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
    echo "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
    kill $hermod_pid
    wait $hermod_pid
    exit 1
fi

echo "############### Testing 100 publishers/ 1 client ##############"
target/release/test_client --name test3_consume_test3 --group test3 --topic test_top3 --consume --count 1000000 &
pid=`pgrep test_client`
for n in {0..100}; do
    target/release/test_client --base_message LOTSCLIENT_${n} --name test3_publish${n}_test3 --group test3 --topic test_top3 --publish --count 10000 &
    #echo $n ;
done

wait $pid
if [[ $? -ne 0 ]]; then
    echo "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
    echo "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX FAIL XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
    echo "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
    kill $hermod_pid
    wait $hermod_pid
    exit 1
fi

# Test no-batch and stream.
echo "############### Testing no batch and stream (5 -> 1) ##############"
target/release/test_client --name test_nb_consume --group test_nb --topic test_top_nb --consume --count 500000 --stream &
pid=`pgrep test_client`
target/release/test_client --base_message CLIENT_1 --name test_nb_publish1 --group test_nb --topic test_top_nb --publish --count 100000 --no_batch &
target/release/test_client --base_message CLIENT_2 --name test_nb_publish2 --group test_nb --topic test_top_nb --publish --count 100000 --no_batch &
target/release/test_client --base_message CLIENT_3 --name test_nb_publish3 --group test_nb --topic test_top_nb --publish --count 100000 --no_batch &
target/release/test_client --base_message CLIENT_4 --name test_nb_publish4 --group test_nb --topic test_top_nb --publish --count 100000 --no_batch &
target/release/test_client --base_message CLIENT_5 --name test_nb_publish5 --group test_nb --topic test_top_nb --publish --count 100000 --no_batch

wait $pid
if [[ $? -ne 0 ]]; then
    echo "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
    echo "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX FAIL XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
    echo "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
    kill $hermod_pid
    wait $hermod_pid
    exit 1
fi

kill $hermod_pid
wait $hermod_pid

#md5sum test_logs/1/0.log
# openssl md5 test_logs/1/0.log
#echo "930049ef9fc414d29365fc973d68bbf1  test_logs/test_top1.0.log"



echo "############### Testing read after server restart ##############"
target/release/hermod --logdir test_logs &
sleep 1
hermod_pid=`pgrep hermod`

target/release/test_client --earlest --name test_consume_test1 --group test1 --topic test_top1 --consume --count 400000 &
pid=`pgrep test_client`

wait $pid
if [[ $? -ne 0 ]]; then
    echo "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
    echo "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX FAIL XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
    echo "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
    kill $hermod_pid
    wait $hermod_pid
    exit 1
fi

kill $hermod_pid
wait $hermod_pid
