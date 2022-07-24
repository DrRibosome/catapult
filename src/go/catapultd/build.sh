#!/bin/bash

pkgdir=$(dirname $0)
cd $pkgdir

docker build -t omnirepo/src/go/catapultd .
