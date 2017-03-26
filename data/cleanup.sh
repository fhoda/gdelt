#!/bin/bash

rm -f 2*
hadoop fs -rm -skipTrash /gdelt/input/*
