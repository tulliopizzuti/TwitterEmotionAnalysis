base=04
to=/media/tullio/223809393D6A6F74/e_04/
fileList=($(find ${base} -type f -name *.json.bz2))
for ((i=0; i < ${#fileList[@]}; i+=1))
do
	fileName=${fileList[i]////_}
	fileName=${fileName%.bz2}
	bzip2 -dc ${fileList[i]} >  ${to}${fileName}
	echo "File ${fileName}"
done
echo "Finish ${i}"
