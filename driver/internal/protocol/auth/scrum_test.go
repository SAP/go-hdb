package auth

import (
	"testing"
)

func TestSCRUM(t *testing.T) {
	testData := []struct {
		method          string
		salt            []byte
		serverChallenge []byte
		rounds          int
		clientChallenge []byte
		password        []byte
		clientProof     []byte
		serverProof     []byte
	}{
		{
			method:          MtSCRAMSHA256,
			salt:            []byte{214, 199, 255, 118, 92, 174, 94, 190, 197, 225, 57, 154, 157, 109, 119, 245},
			serverChallenge: []byte{224, 22, 242, 18, 237, 99, 6, 28, 162, 248, 96, 7, 115, 152, 134, 65, 141, 65, 168, 126, 168, 86, 87, 72, 16, 119, 12, 91, 227, 123, 51, 194, 203, 168, 56, 133, 70, 236, 230, 214, 89, 167, 130, 123, 132, 178, 211, 186},
			clientChallenge: []byte{219, 141, 27, 200, 255, 90, 182, 125, 133, 151, 127, 36, 26, 106, 213, 31, 57, 89, 50, 201, 237, 11, 158, 110, 8, 13, 2, 71, 9, 235, 213, 27, 64, 43, 181, 181, 147, 140, 10, 63, 156, 133, 133, 165, 171, 67, 187, 250, 41, 145, 176, 164, 137, 54, 72, 42, 47, 112, 252, 77, 102, 152, 220, 223},
			password:        []byte{65, 100, 109, 105, 110, 49, 50, 51, 52},
			clientProof:     []byte{23, 243, 209, 70, 117, 54, 25, 92, 21, 173, 194, 108, 63, 25, 188, 185, 230, 61, 124, 190, 73, 80, 225, 126, 191, 119, 32, 112, 231, 72, 184, 199},
		},
		{
			method:          MtSCRAMPBKDF2SHA256,
			salt:            []byte{51, 178, 213, 213, 92, 82, 194, 40, 80, 120, 197, 91, 166, 67, 23, 63},
			serverChallenge: []byte{32, 91, 165, 18, 158, 77, 134, 69, 128, 157, 69, 209, 47, 33, 171, 164, 56, 172, 229, 0, 153, 3, 65, 29, 239, 210, 186, 134, 81, 32, 29, 137, 239, 167, 39, 1, 171, 117, 85, 138, 109, 38, 42, 77, 43, 42, 82, 70},
			rounds:          15000,
			clientChallenge: []byte{137, 156, 182, 60, 158, 138, 93, 103, 80, 202, 54, 191, 210, 78, 142, 207, 210, 176, 157, 129, 128, 19, 135, 0, 127, 26, 58, 197, 188, 216, 121, 26, 120, 196, 34, 138, 5, 8, 58, 32, 36, 240, 199, 126, 164, 112, 64, 35, 46, 102, 255, 249, 126, 250, 24, 103, 198, 152, 33, 75, 6, 179, 187, 230},
			password:        []byte{84, 111, 111, 114, 49, 50, 51, 52},
			clientProof:     []byte{253, 181, 101, 0, 214, 222, 25, 99, 98, 253, 141, 106, 38, 255, 16, 153, 34, 74, 211, 70, 21, 91, 71, 223, 170, 36, 249, 124, 1, 135, 176, 37},
			serverProof:     []byte{228, 2, 183, 82, 29, 218, 234, 242, 40, 50, 142, 158, 142, 153, 185, 189, 130, 51, 176, 155, 23, 179, 58, 19, 126, 144, 139, 229, 116, 3, 242, 197},
		},
	}

	for _, r := range testData {
		var key []byte
		switch r.method {
		case MtSCRAMSHA256:
			key = scramsha256Key(r.password, r.salt)
		case MtSCRAMPBKDF2SHA256:
			key = scrampbkdf2sha256Key(r.password, r.salt, r.rounds)
		default:
			t.Fatalf("unknown authentication method %s", r.method)
		}
		clientProof := clientProof(key, r.salt, r.serverChallenge, r.clientChallenge)
		for i, v := range clientProof {
			if v != r.clientProof[i] {
				t.Fatalf("diff index % d - got %v - expected %v", i, clientProof, r.clientProof)
			}
		}
	}
}
