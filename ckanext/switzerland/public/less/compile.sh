#!/bin/bash
while :
do
	inotifywait -r .
	lessc main.less ../../fanstatic/main.css
done
