#!/usr/bin/env bash
###################################################################################
# sample demo script for making a deal with boost client
###################################################################################
set -e
# colors
cb="\e[1m"
ci="\e[3m" 
cn="\e[0m"
###################################################################################
printf "\n
###################################################################################\n \
Hello to the demo script that makes a storage deal using the boost client\n \
###################################################################################\n \
1. The boost client needs to know how to connect to the lotus instance. \
We need to set ${cb}FULLNODE_API_INFO${cn} env var. We have the lotus client here that will provide a connection token.\n \
 : ${ci}lotus auth api-info --perm=admin${cn} - returns lotus connection token \n\n"
read -rsp $'Press any key to export variable...\n' -n1 key
export `lotus auth api-info --perm=admin`

printf "\nExported FULLNODE_API_INFO=$FULLNODE_API_INFO\n \
###################################################################################\n"
###################################################################################
printf "2. The boost client needs to be initialized by calling \n${ci}boost init${cn} \n\n"
read -rsp $'Press any key to execute it...\n\n' -n1 key

boost init

printf "\n\nGreat. Boost client has been initialized.\n \
###################################################################################\n"
###################################################################################
printf "3. Now add some funds from lotus to boost wallet. We will use the lotus client:\n\n \
 : ${ci}lotus wallet default${cn} - returns default lotus wallet\n \
 : ${ci}boost wallet default${cn} - returns default wallet for the current boost client actor\n \
 : ${ci}lotus send --from=`lotus wallet default` `boost wallet default` 10${cn} - sends 10 FIL\n"
read -rsp $'Press any key to execute it...\n\n' -n1 key

lotus send --from=`lotus wallet default` `boost wallet default` 10

printf "\n\nDone. Funds transfer was initiated\n \
###################################################################################\n"
###################################################################################
printf "4. Now add some funds to the market actor\n \
 : ${ci}boostx market-add 1${cn}\n\n"
read -rsp $'Press any key to execute it...\n' -n1 key

until boostx market-add 1; do printf "\nOpps, maybe funds not added yet.\nNeed to wait some time. \n"; read -rsp $'Press any key to try again...\n' -n1 key; done

printf "\n\nYes. We can make a deal now.\n \
###################################################################################\n"
###################################################################################
printf "5. Let's generate a sample file in ${ci}/app/public/sample.txt${cn}. We will use it as a demo file.\n\n"
read -rsp $'Press any key to generate it...\n\n' -n1 key
rm -f /app/public/sample.txt 
for i in {1..7}; do echo "Hi Boost, $i times" >> /app/public/sample.txt; done

printf "\n\nFile content:\n\n"
cat /app/public/sample.txt
printf "\n\n \
###################################################################################\n"
###################################################################################

printf "6. After that, you need to generate a car file for data you want to store on Filecoin (${ci}/app/public/sample.txt${cn}), \
and note down its ${ci}payload-cid${cn}. \
We will use the ${ci}boostx${cn} utility\n \
 : ${ci}boostx generate-car /app/public/sample.txt /app/public/sample.car${cn}\n\n"
read -rsp $'Press any key to execute it...\n\n' -n1 key

boostx generate-car /app/public/sample.txt /app/public/sample.car

PAYLOAD_CID=`boostx generate-car /app/public/sample.txt /app/public/sample.car | grep CID | cut -d: -f2 | xargs`
printf "\n\nDone. We noted payload-cid = ${ci}$PAYLOAD_CID${cn}\n \
###################################################################################\n"
###################################################################################
printf "7. Then you need to calculate the commp and piece size for the generated car file:\n \
 : ${ci}boostx commp /app/public/sample.car${cn}\n\n"
read -rsp $'Press any key to execute it...\n\n' -n1 key

boostx commp /app/public/sample.car

COMMP_CID=`boostx commp /app/public/sample.car 2> /dev/null | grep CID | cut -d: -f2 | xargs`
PIECE=`boostx commp /app/public/sample.car 2> /dev/null | grep Piece | cut -d: -f2 | xargs`
CAR=`boostx commp /app/public/sample.car 2> /dev/null | grep Car | cut -d: -f2 | xargs`
printf "\n\nYes. We also have remembered these values:\n \
Commp-cid = $COMMP_CID \n \
Piece size = $PIECE \n \
Car size = $CAR \n \
###################################################################################\n"
###################################################################################
printf "8. That's it. We are ready to make the deal. \n \
 : ${ci}boost deal --verified=false --provider=t01000 \
--http-url=http://demo-http-server/sample.car \
--commp=$COMMP_CID --car-size=$CAR --piece-size=$PIECE \
--payload-cid=$PAYLOAD_CID --storage-price 20000000000\n\n${cn}"
read -rsp $'Press any key to make the deal...\n\n' -n1 key

boost deal --verified=false \
           --provider=t01000 \
           --http-url=http://demo-http-server/sample.car \
           --commp=$COMMP_CID \
           --car-size=$CAR \
           --piece-size=$PIECE \
           --payload-cid=$PAYLOAD_CID --storage-price 20000000000

printf "\n\n   ${cb}Congrats! You have made it.${cn}\n\n \
###################################################################################\n"
