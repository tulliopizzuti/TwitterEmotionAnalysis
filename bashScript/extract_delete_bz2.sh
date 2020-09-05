base=e_04
fileList=($(find ${base} -type f -name *.json.bz2))
for ((i=0; i < ${#fileList[@]}; i+=1))
do
	bzip2 -d ${fileList[i]}
	echo "File ${fileList[i]} estratto"	
done
echo "Finish"
