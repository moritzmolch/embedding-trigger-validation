#!/usr/bin/env bash


action () {
    git clone https://github.com/moritzmolch/TauTriggerNtuples.git TauAnalysis/TauTriggerNtuples
}


action "$@"