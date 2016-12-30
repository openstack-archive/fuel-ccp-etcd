#!/bin/bash

/server --id=$(hostname -i) --ttl=4s $@
