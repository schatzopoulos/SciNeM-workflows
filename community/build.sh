#!/bin/bash
cd "$(dirname "$0")"

sbt clean
sbt compile
sbt assembly

