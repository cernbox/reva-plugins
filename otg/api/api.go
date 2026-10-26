// Package api holds the names, arguments and results of the otg invocations.
// It has no init() and no reva dependency, so a client such as a CLI plugin
// can import it without registering the otg service.
package api

// Service is the name of the otg HTTP service in the revad config.
const Service = "otg"

// The invocations of the otg service.
const (
	Get   = "get"
	Set   = "set"
	Clear = "clear"
)

// The arguments of the set invocation, as the keys of SetArgs.
const (
	ArgNumber  = "number"
	ArgMessage = "message"
)

// SetArgs are the arguments of the set invocation.
type SetArgs struct {
	Number  string `arg:"number,required" help:"the OTG number from the CERN SSB, e.g. OTG0060602"`
	Message string `arg:"message,required" help:"the message shown to users, HTML allowed"`
}

// Status is the result of all the otg invocations: the message that users
// see, if there is one.
type Status struct {
	Present bool   `json:"present"`
	Number  string `json:"number,omitempty"`
	Message string `json:"message,omitempty"`
}
