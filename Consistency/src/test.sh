#! /bin/bash

master1=ec2-52-7-140-80.compute-1.amazonaws.com
master2=ec2-52-8-103-68.us-west-1.compute.amazonaws.com
master3=ec2-52-74-198-57.ap-southeast-1.compute.amazonaws.com
datacenter1=ec2-52-7-191-202.compute-1.amazonaws.com
datacenter2=ec2-52-8-101-242.us-west-1.compute.amazonaws.com
datacenter3=ec2-52-74-201-0.ap-southeast-1.compute.amazonaws.com
consistency=$1

aa=$(curl -s "http://$master1:8080/consistency?consistency=$consistency")
if [ $? -ne 0 ]
then
	echo "Master1 is not running. Please start the master and try again"
	exit 1
fi

bb=$(curl -s "http://$master2:8080/consistency?consistency=$consistency")
if [ $? -ne 0 ]
then
	echo "Master2 is not running. Please start the master and try again"
	exit 1
fi

cc=$(curl -s "http://$master3:8080/consistency?consistency=$consistency")
if [ $? -ne 0 ]
then
	echo "Master3 is not running. Please start the master and try again"
	exit 1
fi

sleep 2s

# STRONG CONSISTENCY TEST
test_sc() {
	start_time=$(($(date +%s%N)/1000000))
	curl -s "http://$master1:8080/put?key=1&value=1&loc=1"
	curl -s "http://$master2:8080/put?key=1&value=2&loc=2"
	curl -s "http://$master3:8080/put?key=1&value=3&loc=3"
	sleep 3

	a=$(curl -s "http://$master1:8080/get?key=1&loc=1")
	b=$(curl -s "http://$master2:8080/get?key=1&loc=2")
	c=$(curl -s "http://$master3:8080/get?key=1&loc=3")
	end_time=$(($(date +%s%N)/1000000))
	process_time=$(expr $end_time - $start_time)
	echo $process_time

	if [ "$a" == "3 1 2 " ] || [ "$b" == "3 1 2 " ] || [ "$c" == "3 1 2 " ];
	then
		echo "Coordinator test = PASS"
	else
		echo "Coordinator test = FAIL"
	fi

	sleep 5
	
	a1=$(curl -s "http://$datacenter1:8080/get?key=1&loc=1&consistency=$consistency")
	a2=$(curl -s "http://$datacenter2:8080/get?key=1&loc=2&consistency=$consistency")
	a3=$(curl -s "http://$datacenter3:8080/get?key=1&loc=3&consistency=$consistency")
	if [ "$a1" == "3 1 2 " ] && [ "$a2" == "3 1 2 " ] && [ "$a3" == "3 1 2 " ];
	then
		echo "Datacenter test = PASS"
	else
		echo "Datacenter test = FAIL"	
	fi

	sleep 0.1s
}

# CAUSAL CONSISTENCY TEST
test_cc() {

	start_time=$(($(date +%s%N)/1000000))
	curl -s "http://$master1:8080/put?key=1&value=1&loc=1"
	curl -s "http://$master2:8080/put?key=1&value=2&loc=2"
	curl -s "http://$master3:8080/put?key=1&value=3&loc=3"

	sleep 3

	a=$(curl -s "http://$master1:8080/get?key=1&loc=1")
	b=$(curl -s "http://$master2:8080/get?key=1&loc=2")
	c=$(curl -s "http://$master3:8080/get?key=1&loc=3")
	
	end_time=$(($(date +%s%N)/1000000))
        process_time=$(expr $end_time - $start_time)
        echo $process_time

        if [ "$a" == "3 1 2 " ] || [ "$b" == "3 1 2 " ] || [ "$c" == "3 1 2 " ];
        then
                echo "Coordinator test = PASS"
        else
                echo "Coordinator test = FAIL"
        fi

        sleep 5

        a1=$(curl -s "http://$datacenter1:8080/get?key=1&loc=1&consistency=$consistency")
        a2=$(curl -s "http://$datacenter2:8080/get?key=1&loc=2&consistency=$consistency")
        a3=$(curl -s "http://$datacenter3:8080/get?key=1&loc=3&consistency=$consistency")
        if [ "$a1" == "3 1 2 " ] && [ "$a2" == "3 1 2 " ] && [ "$a3" == "3 1 2 " ];
        then
                echo "Datacenter test = PASS"
        else
                echo "Datacenter test = FAIL"   
        fi

        sleep 0.1s

}

# EVENTUAL CONSISTENCY TEST
test_ec() {
	start_time=$(($(date +%s%N)/1000000))
	curl -s "http://$master1:8080/put?key=1&value=1&loc=1"
	curl -s "http://$master2:8080/put?key=1&value=2&loc=2"
	curl -s "http://$master3:8080/put?key=1&value=3&loc=3"
	
	sleep 3
	
	a=$(curl -s "http://$master1:8080/get?key=1&loc=1")
	b=$(curl -s "http://$master2:8080/get?key=1&loc=2")
	c=$(curl -s "http://$master3:8080/get?key=1&loc=3")
	end_time=$(($(date +%s%N)/1000000))
	process_time=$(expr $end_time - $start_time)
	echo $process_time

	if [ "$a" == "1 2 3 " ] || [ "$b" == "1 2 3 " ] || [ "$c" == "1 2 3 " ];
        then
                echo "Coordinator test = PASS"
        else
                echo "Coordinator test = FAIL"
        fi

        sleep 5

        a1=$(curl -s "http://$datacenter1:8080/get?key=1&loc=1&consistency=$consistency")
        a2=$(curl -s "http://$datacenter2:8080/get?key=1&loc=2&consistency=$consistency")
        a3=$(curl -s "http://$datacenter3:8080/get?key=1&loc=3&consistency=$consistency")
        if [ "$a1" == "1 2 3 " ] && [ "$a2" == "1 2 3 " ] && [ "$a3" == "1 2 3 " ];
        then
                echo "Datacenter test = PASS"
        else
                echo "Datacenter test = FAIL"   
        fi

        sleep 0.1s


}

if [ "$consistency" == "strong" ]
then
	test_sc
fi

if [ "$consistency" == "causal" ]
then
	test_cc
fi

if [ "$consistency" == "eventual" ]
then
	test_ec
fi

