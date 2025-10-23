# Test keys and certificates to test X.509 authentication

## Commands used to create the keys and certs

```bash
openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:4096 -out rootCA.key
openssl req -x509 -new -nodes -key rootCA.key -sha256 -days 3650 -out rootCA.crt -subj "/CN=Go-HDB X.509 Tests RootCA"

openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 -out rsa.pkcs8.key
openssl rsa -in rsa.pkcs8.key -out rsa.pkcs1.key -traditional
openssl req -new -key rsa.pkcs8.key -out rsa.csr -subj "/CN=GoHDBTestUser_rsa"
openssl x509 -req -in rsa.csr -CA rootCA.crt -CAkey rootCA.key -CAcreateserial -out rsa.crt -days 3649 -sha256

openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256 -out ec_p256.pkcs8.key
openssl ec -in ec_p256.pkcs8.key -out ec_p256.ec.key
openssl req -new -key ec_p256.pkcs8.key -out ec_p256.csr -subj "/CN=GoHDBTestUser_ec_p256"
openssl x509 -req -in ec_p256.csr -CA rootCA.crt -CAkey rootCA.key -CAcreateserial -out ec_p256.crt -days 3649 -sha256

openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-384 -out ec_p384.pkcs8.key
openssl ec -in ec_p384.pkcs8.key -out ec_p384.ec.key
openssl req -new -key ec_p384.pkcs8.key -out ec_p384.csr -subj "/CN=GoHDBTestUser_ec_p384"
openssl x509 -req -in ec_p384.csr -CA rootCA.crt -CAkey rootCA.key -CAcreateserial -out ec_p384.crt -days 3649 -sha256

openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-521 -out ec_p521.pkcs8.key
openssl ec -in ec_p521.pkcs8.key -out ec_p521.ec.key
openssl req -new -key ec_p521.pkcs8.key -out ec_p521.csr -subj "/CN=GoHDBTestUser_ec_p521"
openssl x509 -req -in ec_p521.csr -CA rootCA.crt -CAkey rootCA.key -CAcreateserial -out ec_p521.crt -days 3649 -sha256

openssl genpkey -algorithm ED25519 -out ed25519.key
openssl req -new -key ed25519.key -out ed25519.csr -subj "/CN=GoHDBTestUser_ed25519"
openssl x509 -req -in ed25519.csr -CA rootCA.crt -CAkey rootCA.key -CAcreateserial -out ed25519.crt -days 3649 -sha256
```
