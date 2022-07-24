package podbackend

import "catapultd/pkg/catapult"

// internalEvent for package internal events
type internalEvent interface {
	isPodBackendEvent()
}

type launchFinished struct {
	ClientID     catapult.ClientID
	NumSubmitted int   // num attempted to launch
	NumLaunched  int   // num actually launched
	Err          error // first error encountered
}

func (*launchFinished) isPodBackendEvent() {}
