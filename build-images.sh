#!/bin/bash
docker build -f base.dockerfile -t base:0.0.1 .
docker build -f base_nlp.dockerfile -t base_nlp:0.0.1 .