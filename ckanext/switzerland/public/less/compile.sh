#!/bin/bash
while :
do
	inotifywait main.less
	lessc main.less ../../fanstatic/main.css
done
