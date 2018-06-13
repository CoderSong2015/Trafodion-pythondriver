

cer = """-----BEGIN CERTIFICATE-----
MIIEKDCCAxCgAwIBAgIJAM763qvY0FvaMA0GCSqGSIb3DQEBBQUAMG8xCzAJBgNV
BAYTAlVTMQswCQYDVQQIEwJDQTERMA8GA1UEBxMITWlscGl0YXMxGjAYBgNVBAoT
EUVzZ3luIENvcnBvcmF0aW9uMSQwIgYDVQQDExtlc2dneS1kZXYtaGFvbGluMS5u
b3ZhbG9jYWwwHhcNMTgwNDEwMTUzNjUyWhcNMTkwNDEwMTUzNjUyWjBvMQswCQYD
VQQGEwJVUzELMAkGA1UECBMCQ0ExETAPBgNVBAcTCE1pbHBpdGFzMRowGAYDVQQK
ExFFc2d5biBDb3Jwb3JhdGlvbjEkMCIGA1UEAxMbZXNnZ3ktZGV2LWhhb2xpbjEu
bm92YWxvY2FsMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA5H0gqZKS
1pGa7+Vqf3oYUCD1BZiDLt28kV7TTBDHiQ0Bxpbs+1k1J59P/Mmil2cDqeQm5dWY
uinhOSjK+YTTwIWZCrONsUv940Cx6Ur9COQE0n/fBCWYIhTyQh05Vp3vVzwa1IoX
DRQupLcem/mQV9BQyjSDwg+Z0f0k18v+RlgThbfBXm/TyD/wFTpgE+7GzANnjvZP
BjVonWmTQ07dVjofIKQ45Dc5meSDtXA21NKrXCWn3isGNe/CeWSMIzrtBasHOGBB
YwF0tJkUDoLksyko0Es86iPMJNxm0TDDJD3daDes3EaTHID56bifKjLIROVLSyo+
e77cwO9MVchg0QIDAQABo4HGMIHDMB0GA1UdDgQWBBTvdg8RkYo2Y5vRLOzvrjGf
rW7x0jCBoQYDVR0jBIGZMIGWgBTvdg8RkYo2Y5vRLOzvrjGfrW7x0qFzpHEwbzEL
MAkGA1UEBhMCVVMxCzAJBgNVBAgTAkNBMREwDwYDVQQHEwhNaWxwaXRhczEaMBgG
A1UEChMRRXNneW4gQ29ycG9yYXRpb24xJDAiBgNVBAMTG2VzZ2d5LWRldi1oYW9s
aW4xLm5vdmFsb2NhbIIJAM763qvY0FvaMA0GCSqGSIb3DQEBBQUAA4IBAQAvLxL/
ifhr+XCJEghM1lOdDhXXj6d9BT27M19J+kabgeBmnUsZ8gg83fqyFe4c42ju7vfQ
n1d1x/z7HeV4tlTDoHBGk0u1AVvXqyg3/nCbs8l+KQ4Cwn0hQdJhjaWFXuFZlFDx
USgN685K30jO6zOsoC7+BVg1pE6mTuu1dO25PhRc5xAF6bFpvBa4i805Wmk6gYj3
7eOoaQJ+HO2o8+sk5BoQ8hByqTQpb681a6oi1yB3RAejEp3iW0XiCVGB+ULE08lJ
XrhHtuFqHkk1Od1WVKQTgPXBU3Otvi3b9u0UY2tufnf82ZkLodbzLkvwBjsQ6nRu
xqHLMLszP/DVbnAF
-----END CERTIFICATE-----"""







if __name__ == '__main__':
    import OpenSSL.crypto
    c = OpenSSL.crypto
    cert = c.load_certificate(c.FILETYPE_PEM, cer.encode("utf-8"))
    c = cert.get_pubkey()
    dd = 256 if c.bits() / 8 > 128 else 128
    print(cert.get_notBefore())

    import random
    from sys import maxsize
    b = bytearray(10)
    for i in range(len(b)):
        b[i] = (random.randint(0, maxsize) & 0xFF)
    print(b)
    import time
    print(time.time())