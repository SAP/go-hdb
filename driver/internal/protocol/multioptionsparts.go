package protocol

import (
	"fmt"
	"sort"

	"github.com/SAP/go-hdb/driver/internal/protocol/encoding"
)

type topologyOption int8

const (
	toHostName         topologyOption = 1
	toHostPortnumber   topologyOption = 2
	toTenantName       topologyOption = 3
	toLoadfactor       topologyOption = 4
	toVolumeID         topologyOption = 5
	toIsPrimary        topologyOption = 6
	toIsCurrentSession topologyOption = 7
	toServiceType      topologyOption = 8
	toNetworkDomain    topologyOption = 9 // deprecated
	toIsStandby        topologyOption = 10
	toAllIPAddresses   topologyOption = 11 // deprecated
	toAllHostNames     topologyOption = 12 // deprecated
	toSiteType         topologyOption = 13
)

type serviceType int32

const (
	stOther            serviceType = 0
	stNameServer       serviceType = 1
	stPreprocessor     serviceType = 2
	stIndexServer      serviceType = 3
	stStatisticsServer serviceType = 4
	stXSEngine         serviceType = 5
	stReserved6        serviceType = 6
	stCompileServer    serviceType = 7
	stDPServer         serviceType = 8
	stDIServer         serviceType = 9
	stComputeServer    serviceType = 10
	stScriptServer     serviceType = 11
)

func convertToServiceType(v any) serviceType {
	i32, ok := v.(int32)
	if !ok {
		return stOther
	}
	st := serviceType(i32)
	if st > stScriptServer {
		return stOther
	}
	return st
}

type topologyInformation []map[topologyOption]any

func (o topologyInformation) String() string {
	s1 := []string{}
	for _, ops := range o {
		s2 := []string{}
		for j, typ := range ops {
			switch j {
			case toServiceType:
				s2 = append(s2, fmt.Sprintf("%s: %s", topologyOption(j), convertToServiceType(typ)))
			default:
				s2 = append(s2, fmt.Sprintf("%s: %v", topologyOption(j), typ))
			}
		}
		sort.Slice(s2, func(i, j int) bool { return s2[i] < s2[j] })
		s1 = append(s1, fmt.Sprintf("%v", s2))
	}
	return fmt.Sprintf("%v", s1)
}

func (o *topologyInformation) decode(dec *encoding.Decoder, ph *PartHeader) error {
	numArg := ph.numArg()
	*o = resizeSlice(*o, numArg)
	for i := 0; i < numArg; i++ {
		ops := map[topologyOption]any{}
		(*o)[i] = ops
		optCnt := int(dec.Int16())
		for j := 0; j < optCnt; j++ {
			k := topologyOption(dec.Int8())
			tc := typeCode(dec.Byte())
			ot := tc.optType()
			ops[k] = ot.decode(dec)
		}
	}
	return dec.Error()
}
