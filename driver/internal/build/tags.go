// Package build supports go-dhb build tags.
package build

// EmptyDateAsNull returns NULL for empty dates ('0000-00-00') if true, otherwise:
//
//	For data format version 1 the backend does return the NULL indicator for empty date fields.
//	For data format version non equal 1 (field type daydate) the NULL indicator is not set and the return value is 0.
//	As value 1 represents '0001-01-01' (the minimal valid date) without setting EmptyDateAsNull '0000-12-31' is returned,
//	so that NULL, empty and valid dates can be distinguished.
//
// https://help.sap.com/docs/HANA_SERVICE_CF/7c78579ce9b14a669c1f3295b0d8ca16/3f81ccc7e35d44cbbc595c7d552c202a.html?locale=en-US
var EmptyDateAsNull = false // use build tag edan to set to true.
