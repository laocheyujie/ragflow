import base64
from Cryptodome.PublicKey import RSA
from Cryptodome.Cipher import PKCS1_v1_5 as Cipher_pkcs1_v1_5

# 这是从你的项目中读取的公钥 (conf/public.pem)
public_key_pem = """-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEArq9XTUSeYr2+N1h3Afl/
z8Dse/2yD0ZGrKwx+EEEcdsBLca9Ynmx3nIB5obmLlSfmskLpBo0UACBmB5rEjBp
2Q2f3AG3Hjd4B+gNCG6BDaawuDlgANIhGnaTLrIqWrrcm4EMzJOnAOI1fgzJRsOO
UEfaS318Eq9OVO3apEyCCt0lOQK6PuksduOjVxtltDav+guVAA068NrPYmRNabVK
RNLJpL8w4D44sfth5RvZ3q9t+6RTArpEtc5sh5ChzvqPOzKGMXW83C95TxmXqpbK
6olN4RevSfVjEAgCydH6HN6OhtOQEcnrU97r9H0iZOWwbw3pVrZiUkuRD1R56Wzs
2wIDAQAB
-----END PUBLIC KEY-----"""

def encrypt_password(password):
    rsa_key = RSA.importKey(public_key_pem)
    cipher = Cipher_pkcs1_v1_5.new(rsa_key)
    # RAGFlow 的加密逻辑：先 Base64 编码明文，再 RSA 加密，最后 Base64 编码密文
    password_base64 = base64.b64encode(password.encode('utf-8')).decode("utf-8")
    encrypted_bytes = cipher.encrypt(password_base64.encode('utf-8'))
    return base64.b64encode(encrypted_bytes).decode('utf-8')

password = "aminer2026"
encrypted_password = encrypt_password(password)

print(f"Password: {password}")
print(f"Encrypted Password: {encrypted_password}")