#!/bin/bash
while :
do
	inotifywait *.less
	lessc main.less ../../fanstatic/main.css
done
