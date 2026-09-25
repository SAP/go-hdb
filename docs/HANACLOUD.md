# HANA Cloud Connection

The HANA Cloud connection proxy uses SNI, which requires a TLS connection.
By default, one can rely on the root certificate set provided by the host, which already comes with the necessary
DigiCert certificates (CA, G5).
For more information on Go TLS certificate handling, please see [crypto/tls#Config](https://pkg.go.dev/crypto/tls#Config).

Assuming the HANA cloud 'endpoint' is "something.hanacloud.ondemand.com:443", the DSN should look as follows:

```
"hdb://user:password@something.hanacloud.ondemand.com:443?TLSServerName=something.hanacloud.ondemand.com"
```

where:
- TLSServerName: same as 'host'

If a specific root certificate (e.g. self-signed) is needed, the TLSRootCAFile DSN parameter must be the path on the file system to the root certificate file in PEM format.
