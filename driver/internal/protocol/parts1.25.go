//go:build go1.25

package protocol

import "reflect"

// newGenPartReader returns a generic part reader.
func newGenPartReader(kind PartKind) Part {
	if kind == PkAuthentication {
		return nil // cannot instantiate generically
	}
	pt, ok := genPartTypeMap[kind]
	if !ok {
		// whether part cannot be instantiated generically or
		// part is not (yet) known to the driver
		return nil
	}
	// create instance
	part, ok := reflect.TypeAssert[Part](reflect.New(pt))
	if !ok {
		panic("part kind does not implement part reader interface") // should never happen
	}
	if part, ok := part.(initer); ok {
		part.init()
	}
	return part
}
