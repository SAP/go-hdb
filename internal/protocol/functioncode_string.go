// Code generated by "stringer -type=functionCode"; DO NOT EDIT

package protocol

import "fmt"

const _functionCode_name = "fcNilfcDDLfcInsertfcUpdatefcDeletefcSelectfcSelectForUpdatefcExplainfcDBProcedureCallfcDBProcedureCallWithResultfcFetchfcCommitfcRollbackfcSavepointfcConnectfcWriteLobfcReadLobfcPingfcDisconnectfcCloseCursorfcFindLobfcAbapStreamfcXAStartfcXAJoin"

var _functionCode_index = [...]uint8{0, 5, 10, 18, 26, 34, 42, 59, 68, 85, 112, 119, 127, 137, 148, 157, 167, 176, 182, 194, 207, 216, 228, 237, 245}

func (i functionCode) String() string {
	if i < 0 || i >= functionCode(len(_functionCode_index)-1) {
		return fmt.Sprintf("functionCode(%d)", i)
	}
	return _functionCode_name[_functionCode_index[i]:_functionCode_index[i+1]]
}
