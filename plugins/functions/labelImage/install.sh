#!/bin/sh
dir=/usr/local/tflite
cur=$(dirname "$0")
echo "Base path $cur" 
if [ -d "$dir" ]; then
    echo "SDK path $dir exists." 
else
    echo "Creating SDK path $dir"
    mkdir -p $dir
    echo "Created SDK path $dir"
    echo "Moving libs"
    cp -R $cur/lib $dir
    echo "Moved libs"
fi

if [ -f "/etc/ld.so.conf.d/tflite.conf" ]; then
    echo "/etc/ld.so.conf.d/tflite.conf exists"
else
    echo "Copy conf file"
    cp $cur/tflite.conf /etc/ld.so.conf.d/
    echo "Copied conf file"
fi
ldconfig
echo "Done"