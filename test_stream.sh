#!/bin/bash

printf "Request : \n"




count=1
while [ $count -le 9 ]
do

price="102$count" 
echo $price
curl -X POST -d $price  "http://localhost:9090/nasdaq/publishQuote"
sleep 1+$count
(( count++ ))
done

