#!/bin/bash
SRC_DIR=../java/com/github/jsgilmore/protoshell
DST_DIR=../../
protoc -I=$SRC_DIR --java_out=$DST_DIR $SRC_DIR/messages.proto
