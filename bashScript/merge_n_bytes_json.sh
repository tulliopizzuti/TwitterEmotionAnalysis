base=/media/tullio/223809393D6A6F74/e_04
output=/media/tullio/223809393D6A6F74/e_04
maxMb=25
maxByte=$((maxMb*1000000))
mkdir $base
fileList=($(find ${base} -type f -name *.json))
j=1
tot=0
totByte=0
totParz=0
parzList=""

totFile=${#fileList[@]}
totFileIter=$(($totFile-1))

for((i=0; i < ${#fileList[@]}; i+=1))
do

	fileByte=$(stat -c %s ${fileList[i]})
	toCheck=$(($fileByte+$tot))
	((totByte+=$fileByte))
  	if [ $toCheck -le $maxByte ] 
  	then 
  		((tot+=$fileByte))
  		parzList+="  ${fileList[i]}"
  	else
  		echo "${tot}"
  		bzip2 -c ${parzList} > $output/$j.json.bz2
  		echo "${j}.json.bz2 completato"
  		((totParz+=$tot))
  		((tot=$fileByte))
  		parzList=${fileList[i]}
  		((j++))
  	fi
	if [ $i == $totFileIter ]
	then
		echo "${tot}"
  		bzip2 -c ${parzList} > $output/$j.json.bz2
  		echo "${j}.json.bz2 completato"
  		((totParz+=$tot))
  		((tot=$fileByte))
  		parzList=${fileList[i]}
  		((j++))
	
	fi
	
done
echo "${totByte} - ${totParz} - ${j}"