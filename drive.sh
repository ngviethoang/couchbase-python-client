#!/bin/bash
fileid="1f5XbJOtYkgEh1ZHtR_inM3Pc6YgjLF9_"
filename="ds.zip"
curl -c ./cookie -s -L "https://drive.google.com/uc?export=download&id=${fileid}" > /dev/null
curl -Lb ./cookie "https://drive.google.com/uc?export=download&confirm=`awk '/download/ {print $NF}' ./cookie`&id=${fileid}" -o ${filename}
