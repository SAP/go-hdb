# Test keys and certificates to test X.509 authentication

## Commands used to create the keys and certs

```bash
openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:4096 -out rootCA.key
openssl req -x509 -new -nodes -key rootCA.key -sha256 -days 3650 -out rootCA.crt -subj "/CN=Go-HDB X.509 Tests RootCA"

openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 -out rsa.pkcs8.key
openssl rsa -in rsa.pkcs8.key -out rsa.pkcs1.key -traditional
openssl req -new -key rsa.pkcs8.key -out rsa.csr -subj "/CN=GoHDBTestUser_rsa"
openssl x509 -req -in rsa.csr -CA rootCA.crt -CAkey rootCA.key -CAcreateserial -out rsa.crt -days 3649 -sha256
```
