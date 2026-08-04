package protocol

import (
	"errors"
	"reflect"
	"testing"
)

// topologyNode returns a topology node option map populated with the given
// routing attributes.
func topologyNode(host string, port int32, serviceType ServiceType, volumeID int32, currentSession bool, siteType SiteType) *options[topologyOption] {
	node := &options[topologyOption]{}
	node.set(toHostName, host)
	node.set(toHostPortnumber, port)
	node.set(toServiceType, int32(serviceType))
	node.set(toIsCurrentSession, currentSession)
	node.set(toVolumeID, volumeID)
	node.set(toSiteType, int32(siteType))
	return node
}

func TestTopologyInformationSortedNodeList(t *testing.T) {
	host := "proxy:30013"

	tests := []struct {
		name        string
		routingHost *string
		nodes       []*options[topologyOption]
		want        RoutingNodeList
		wantErr     error
	}{
		{
			name: "sorted by identity",
			nodes: []*options[topologyOption]{
				topologyNode("node-c", 30013, StIndexServer, 3, true, SiteTypePrimary),
				topologyNode("node-a", 30013, StIndexServer, 1, false, SiteTypePrimary),
				topologyNode("node-b", 30013, StIndexServer, 2, false, SiteTypeSecondary),
			},
			want: RoutingNodeList{
				{SiteVolumeID: 1, SiteType: SiteTypePrimary, Host: "node-a:30013"},
				{SiteVolumeID: 2, SiteType: SiteTypeSecondary, Host: "node-b:30013"},
				{SiteVolumeID: 3, SiteType: SiteTypePrimary, Host: "node-c:30013"},
			},
		},
		{
			name: "site type tiebreak",
			nodes: []*options[topologyOption]{
				topologyNode("node-b", 30013, StIndexServer, 1, true, SiteTypeSecondary),
				topologyNode("node-a", 30013, StIndexServer, 1, false, SiteTypePrimary),
			},
			want: RoutingNodeList{
				{SiteVolumeID: 1, SiteType: SiteTypePrimary, Host: "node-a:30013"},
				{SiteVolumeID: 1, SiteType: SiteTypeSecondary, Host: "node-b:30013"},
			},
		},
		{
			name: "non index server filtered",
			nodes: []*options[topologyOption]{
				topologyNode("node-a", 30013, StIndexServer, 1, true, SiteTypePrimary),
				topologyNode("node-b", 30013, StNameServer, 2, false, SiteTypePrimary),
			},
			want: RoutingNodeList{
				{SiteVolumeID: 1, SiteType: SiteTypePrimary, Host: "node-a:30013"},
			},
		},
		{
			name: "invalid site volume id",
			nodes: []*options[topologyOption]{
				topologyNode("node-a", 30013, StIndexServer, 0x00FFFFFF, true, SiteTypePrimary),
			},
			wantErr: errInvalidSiteVolumeID,
		},
		{
			name: "duplicate identity",
			nodes: []*options[topologyOption]{
				topologyNode("node-a", 30013, StIndexServer, 1, true, SiteTypePrimary),
				topologyNode("node-b", 30013, StIndexServer, 1, false, SiteTypePrimary),
			},
			wantErr: errBadTopologyDuplicateKey,
		},
		{
			name: "no own record",
			nodes: []*options[topologyOption]{
				topologyNode("node-a", 30013, StIndexServer, 1, false, SiteTypePrimary),
			},
			wantErr: errBadTopologyNoOwnRecord,
		},
		{
			name:        "port mismatch behind proxy",
			routingHost: &host,
			nodes: []*options[topologyOption]{
				topologyNode("node-a", 30099, StIndexServer, 1, true, SiteTypePrimary),
			},
			wantErr: errPortForwarded,
		},
		{
			name:        "port match",
			routingHost: &host,
			nodes: []*options[topologyOption]{
				topologyNode("node-a", 30013, StIndexServer, 1, true, SiteTypePrimary),
			},
			want: RoutingNodeList{
				{SiteVolumeID: 1, SiteType: SiteTypePrimary, Host: "node-a:30013"},
			},
		},
		{
			name: "nil routing host skips port check",
			nodes: []*options[topologyOption]{
				topologyNode("node-a", 30099, StIndexServer, 1, true, SiteTypePrimary),
			},
			want: RoutingNodeList{
				{SiteVolumeID: 1, SiteType: SiteTypePrimary, Host: "node-a:30099"},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ti := TopologyInformation{nodes: test.nodes}
			got, err := ti.SortedNodeList(test.routingHost)
			if !errors.Is(err, test.wantErr) {
				t.Fatalf("SortedNodeList error: got %v, expected %v", err, test.wantErr)
			}
			if test.wantErr != nil {
				return
			}
			if !reflect.DeepEqual(got, test.want) {
				t.Fatalf("SortedNodeList: got %v, expected %v", got, test.want)
			}
		})
	}
}
