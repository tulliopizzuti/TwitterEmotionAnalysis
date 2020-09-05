base=e_04
maxMb=256
maxByte=$((maxMb*1000000))
mkdir $base
fileList=($(find ./04 -type f -name *.json.bz2))
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
  		bzip2 -dc ${parzList} | bzip2 > $base/$j.json.bz2
  		echo "${j}.json.bz2 completato"
  		((totParz+=$tot))
  		((tot=$fileByte))
  		parzList=${fileList[i]}
  		((j++))
  	fi
	if [ $i == $totFileIter ]
	then
		echo "${tot}"
  		bzip2 -dc ${parzList} | bzip2 > $base/$j.json.bz2
  		echo "${j}.json.bz2 completato"
  		((totParz+=$tot))
  		((tot=$fileByte))
  		parzList=${fileList[i]}
  		((j++))
	
	fi
	
done
echo "${totByte} - ${totParz} - ${j}"
