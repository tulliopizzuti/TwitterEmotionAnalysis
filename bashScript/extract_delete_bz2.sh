base=e_04
to=/media/tullio/223809393D6A6F74/
fileList=($(find ${base} -type f -name *.json.bz2))
for ((i=0; i < ${#fileList[@]}; i+=1))
do
	bzip2 -dc ${fileList[i]} >  ${to}${fileList[i]%.bz2}
	echo "File ${fileList[i]} estratto"
done
echo "Finish"
