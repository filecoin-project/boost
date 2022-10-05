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
printf "5. Let's generate a random file in ${ci}/app/public/random.bin${cn}. We will use it as a demo file.\n\n"
read -rsp $'Press any key to generate it...\n\n' -n1 key
rm -f /app/public/random.bin 
head -c 7655000 </dev/urandom >/app/public/random.bin

###################################################################################

printf "6. After that, you need to generate a car file for data you want to store on Filecoin (${ci}/app/public/random.bin${cn}), \
and note down its ${ci}payload-cid${cn}. \
We will use the ${ci}boostx${cn} utility\n \
 : ${ci}boostx generate-car /app/public/random.bin /app/public/random.car${cn}\n\n"
read -rsp $'Press any key to execute it...\n\n' -n1 key

boostx generate-car /app/public/random.bin /app/public/random.car

PAYLOAD_CID=`boostx generate-car /app/public/random.bin /app/public/random.car | grep CID | cut -d: -f2 | xargs`
printf "\n\nDone. We noted payload-cid = ${ci}$PAYLOAD_CID${cn}\n \
###################################################################################\n"
###################################################################################
printf "7. Then you need to calculate the commp and piece size for the generated car file:\n \
 : ${ci}boostx commp /app/public/random.car${cn}\n\n"
read -rsp $'Press any key to execute it...\n\n' -n1 key

boostx commp /app/public/random.car

COMMP_CID=`boostx commp /app/public/random.car 2> /dev/null | grep CID | cut -d: -f2 | xargs`
PIECE=`boostx commp /app/public/random.car 2> /dev/null | grep Piece | cut -d: -f2 | xargs`
CAR=`boostx commp /app/public/random.car 2> /dev/null | grep Car | cut -d: -f2 | xargs`
printf "\n\nYes. We also have remembered these values:\n \
Commp-cid = $COMMP_CID \n \
Piece size = $PIECE \n \
Car size = $CAR \n \
###################################################################################\n"
###################################################################################
printf "8. That's it. We are ready to make the deal. \n \
 : ${ci}boost deal --verified=false --provider=t01000 \
--http-url=http://demo-http-server/random.car \
--commp=$COMMP_CID --car-size=$CAR --piece-size=$PIECE \
--payload-cid=$PAYLOAD_CID --storage-price 20000000000\n\n${cn}"
read -rsp $'Press any key to make the deal...\n\n' -n1 key

until boost deal --verified=false \
           --provider=t01000 \
           --http-url=http://demo-http-server/random.car \
           --commp=$COMMP_CID \
           --car-size=$CAR \
           --piece-size=$PIECE \
           --payload-cid=$PAYLOAD_CID --storage-price 20000000000
do  
    printf "\nThe error has occured. Perhaps we should wait some time for funds to arrive into the market account.\n\n" 
    read -rsp $'Press any key to check the boost wallet...\n\n' -n1 key
    boost init
    read -rsp $'\n\nPress any key to try making the deal again...\n' -n1 key 
done           

printf "\n\n   ${cb}Congrats! You have made it.${cn}\n\n \
###################################################################################\n"
###################################################################################"
printf "9. Deal has been made, and it will be published automatically after some time, but you can do it manually using boost's graphql API\n \
: ${ci}curl -X POST -H \"Content-Type: application/json\" -d '{\"query\":\"mutation { dealPublishNow }\"}' http://localhost:8080/graphql/query ${cn}\n\n"
read -rsp $'Press any key to publish the deal...\n\n' -n1 key

curl -X POST -H "Content-Type: application/json" -d '{"query":"mutation { dealPublishNow }"}' http://localhost:8080/graphql/query | jq
printf "\nDone.\n\n \
###################################################################################\n"
###################################################################################
printf "10. To retrieve the file from the ${cb}lotus${cn} system you can use \n\
${ci}lotus client retrieve${cn} or ${ci}lotus client cat${cn} commands.\n\
: ${ci}lotus client cat --miner t01000 $PAYLOAD_CID ${cn}\n\n"

read -rsp $'Press any key to show the file content...\n\n' -n1 key
until lotus client cat --provider=t01000 $PAYLOAD_CID > /tmp/random.bin
do  
    printf "\nFile publishing may take time, please wait some time until the deal is finished and try again.\n\n" 
    read -rsp $'Press any key to try again...\n' -n1 key 
done

printf "\n\nIf you see a file content you have just completed the demo. You have succesfully:\n\n\
  1) initiated the boost client\n\
  2) prepared sample file\n\
  3) sent the sample file to the Filecoin devnet\n\
  4) retrieved the content of the file from it.\n\n\
More info at ${cb}https://boost.filecoin.io${cn} or ${cb}https://github.com/filecoin-project/boost${cn}.\n\n\n"
