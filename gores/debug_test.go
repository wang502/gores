package gores

import (
    "testing"
)

func TestDebug(t *testing.T){
    debug("testing debug")
    debugf("error message: [%s]", "testing debugf")
    debugln("testing debugln")
}

func TestTrace(t *testing.T){
    trace("testing trace")
    tracef("error message: [%s]", "testing tracef")
    traceln("testing traceln")
}
