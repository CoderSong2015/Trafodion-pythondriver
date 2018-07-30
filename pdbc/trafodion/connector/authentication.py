import hashlib
import hmac
import os
import random
import time
from sys import maxsize

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.padding import PKCS1v15
from cryptography import x509
from cryptography.hazmat.backends import default_backend


from . import errors


class SecPwd:

    def __init__(self, directory, filename, spj_mode, server_name, proc_info):
        """
        # Ctor for the SecPwd. There are two possible certificates: active
        # certificate and certificate that is going to be active.
        #
        # If autodownload is true, certificate will always come from the
        # server. In this case, only active certificate is used.
        #
        # If autodownload is false, active certificate is used to encrypt the
        # password. When there is a new certificate, it will be stored in
        # "certificate". As soon as this new certificate is activated on the
        # server, the current active certificate will become stale, and the new
        # certificate will be copied over and becomes the active certificate.
        #
        # If spjMode is true, the OS name is NONSTOP_KERNEL and the host name
        # is the same as the server name then just setSpj mode to true
        # and does nothing.
        #
        # @param directory
        #            specifies the directory to locate the certificate. The default
        #            value is %HOME% if set else %HOMEDRIVE%%HOMEPATH%.
        # @param file_name
        #            specifies the certificate that is in waiting. The default
        #            value is the first 5 characters of server name.
        # 
        # @param spj_mode
        #            true - and if os.name == NSK and the host name
        #            matches the local host - token case.  Certificate is not
        #            handled in this case.
        #            false - handles certificate
        # @param server_name
        #            server name for this certificate.
        # @param proc_info
        #            stores only 4 bytes pid + 4 bytes nid
        # @throws SecurityException
        #   

        """
        self.m_sec =''
        self.filename = filename if filename else server_name + '.cer'

        # check USERID env variable for MXCI testing of SPJs.If set use normal password encryption
        self.spj_mode = spj_mode
        if self.spj_mode:
            pass
        else:  # password
            if not proc_info:
                raise errors.NotSupportedError#SecurityException

            # Stores proc_info with the time stamp for data message encryption used
            self.proc_info = proc_info

            env = os.environ
            if directory:
                self.directory = directory
            elif 'HOME' in env:
                self.directory = env['HOME']
            else:
                if 'HOMEDRIVE' in env and 'HOMEPATH' in env:
                    self.directory = env['HOMEDRIVE'] + os.sep+ env['HOMEPATH']
                else:
                    raise errors.NotSupportedError  # SecurityException (SecClientMsgKeys.HOME_ENVIRONMENT_VAR_IS_NULL)
        self.cer_filepath = self.directory + os.sep + self.filename
        self.security = None

    def open_certificate(self, certificate=None):
        if not self.spj_mode:  # do nothing for the token case
            if certificate is not None:
                self.security = Security(cer=certificate)
            else:
                self.security = Security(cer_file=self.cer_filepath)

    def encrypt_pwd(self, pwd, rolename):
        """
         This method builds the password key which consists 4 bytes of password id,
         #  128 bytes of role name which would be 128 spaces when role name is null,
         #  32 bytes of the digest message calculated using the session key on the data made up of
         #  the procInfo and the encrypted data and 256 bytes (if the 2048 public key is used) or
         #  128 bytes (if the1024 public key is used) encrypted data calculated using the public key
         #  on the plain text made up of the session key, the nonce and the password.
         #  The password key is generated only when the spjMode is false.  When
         #  the spjMode is true, 26 bytes of the token is returned instead.
         # Builds password key
         # @param pwd
         # 		 	password to be encrypted
         # @param rolename
         # 			role name to build password key
         # @param procInfo
         # 			process information (PIN, CPU, segment name and time stamp)
         # @return pwdkey
         # 			returns the password key if spjMode is false
         #          returns the token when spjMode is true
         # @throws SecurityException
         #
        """
        if not pwd:
            raise errors.NotSupportedError
        if self.spj_mode:   # token
            pass
        else:
            return self.security.encrypt_pwd(pwd, rolename, self.proc_info)

    def get_cer_exp_date(self):
        date = self.security.get_cer_exp_date()
        return date


class Security:
    def __init__(self, cer_file=None, cer=None):
        # m_encrypted = 0
        self.pwdkey = SecdefsCommon.PwdKey()
        self.pwdkey.data = SecdefsCommon.LoginData()
        self.pwdkey.id[0] = 1
        self.pwdkey.id[1] = 2
        self.pwdkey.id[2] = 3
        self.pwdkey.id[3] = 4
        self.key_time = None

        try:
            self.keyobj = Key()
            self.cert = Certificate()
            self.cert.import_cert(cer) if cer is not None else self.cert.import_cert_file(cer_file)
            self.keyobj.import_pub_key(self.cert.get_pubkey())
            self.generate_session_key()
        except:
            raise errors.InternalError

    def get_cer_exp_date(self):

        #  YYYY-mm-dd HH:MM:SS
        timestamp = str(self.cert.get_not_after())
        timestamp = timestamp.replace('-', '')
        timestamp = timestamp.replace(' ', '')
        timestamp = timestamp.replace(':', '')
        timestamp = tuple(timestamp[2:].encode())
        # TODO timestamp
        return timestamp

    def encrypt_pwd(self, pwd:str, rolename:str, proc_info:bytes):

        # Get public key length int
        pubkey_len = self.keyobj.key_len
        max_plaintext_len = pubkey_len - SecdefsCommon.UNUSEDBYTES

        # Password + nonce + session key can't be longer than the public key's length
        if SecdefsCommon.NONCE_SIZE + SecdefsCommon.SESSION_KEYLEN \
                + len(pwd) > max_plaintext_len:
            pass

        pwd_key_result = bytearray(self.keyobj.key_len + SecdefsCommon.PWDKEY_SIZE_LESS_LOGINDATA)
        try:
            # Build password key
            # Copy 4 bytes of id
            pwd_key_result[0:4] = self.pwdkey.id
            # Copy rolename
            if (rolename != ''):
                rolename_len = len(rolename)
                pwd_key_result[4:rolename_len] = pwd_key_result + rolename.encode("utf-8")
            else:
                rolename_len = 0

            # Copy 16 bytes of procInfo and timestamp(4 + 4 + 8) to
            # password key store procInfo in the digest starting from
            # digest[20]
            s =SecdefsCommon.PWDID_SIZE + SecdefsCommon.ROLENAME_SIZE + SecdefsCommon.DIGEST_LENGTH - \
                            SecdefsCommon.PROCINFO_SIZE
            pwd_key_result[s: s + 16] = proc_info

            # Build plain text to encrypt
            to_encrypt = self.pwdkey.data.session_key + self.pwdkey.data.nonce + pwd.encode()

            # # Encrypt the data
            cipher_text = self.encrypt(to_encrypt, self.cert.get_pubkey())

            # # Copy cipherText to pwdkey
            s = SecdefsCommon.PWDKEY_SIZE_LESS_LOGINDATA
            pwd_key_result[s:s + len(cipher_text)] = cipher_text

            # # Create digest
            # # Get bytes from digest[20] on
            s = SecdefsCommon.PWDKEY_SIZE_LESS_LOGINDATA - SecdefsCommon.TIMESTAMP_SIZE - SecdefsCommon.PROCINFO_SIZE
            e = SecdefsCommon.PROCINFO_SIZE + SecdefsCommon.TIMESTAMP_SIZE + len(cipher_text)

            to_digest = pwd_key_result[s:s + e]
            digested_msg = self.digest(self.pwdkey.data.session_key, to_digest)
            if len(digested_msg) != SecdefsCommon.DIGEST_LENGTH:
                raise errors.NotSupportedError
            s = SecdefsCommon.PWDKEY_SIZE_LESS_LOGINDATA - SecdefsCommon.TIMESTAMP_SIZE - SecdefsCommon.DIGEST_LENGTH
            pwd_key_result[s:s + len(digested_msg)] = digested_msg

            return pwd_key_result
        except:
            raise errors.NotSupportedError

    def digest(self, session_key, to_digest):
        try:
            hm = hmac.new(session_key, to_digest, hashlib.sha256)
            return hm.digest()
        except:
            raise errors.NotSupportedError

    @staticmethod
    def encrypt(to_encrypt: bytearray, p_key):
        #rsa_pk = rsa.PublicKey.load_pkcs1_openssl_pem(p_key)
        #encrypt_text = rsa.encrypt(to_encrypt, rsa_pk)
        encrypt_text = p_key.encrypt(bytes(to_encrypt), PKCS1v15())
        return encrypt_text

    def generate_session_key(self):
        for i in range(len(self.pwdkey.data.session_key)):
            self.pwdkey.data.session_key[i] = (random.randint(0, maxsize) & 0xFF)
            #self.pwdkey.data.session_key[i] = 7

        for i in range(len(self.pwdkey.data.nonce)):
            self.pwdkey.data.nonce[i] = (random.randint(0, maxsize) & 0xFF)
            #self.pwdkey.data.nonce[i] = 7

        #  TODO
        #  m_nonceSeq

        self.key_time = time.time()

        pass


class Key:
    def __init__(self):
        self.key = None
        self._key_len = 0
        self._pub_key = None

    def get_pubkey_from_file(self, file):
        pass

    def import_pub_key(self, key):
        self.key = key
        #self._key_len = 256 if self.key.bits() / 8 > 128 else 128
        self._key_len = key.key_size
        self._pub_key = key.public_bytes(encoding=serialization.Encoding.PEM,
                                         format=serialization.PublicFormat.SubjectPublicKeyInfo)

    @property
    def key_len(self):
        return self._key_len

    @property
    def public_key(self):
        return self._pub_key


class Certificate:
    def __init__(self):
        self.cer_obj = None

    def import_cert(self, cer: str):
        self.cer_obj = x509.load_pem_x509_certificate(cer.encode(), default_backend())

    def import_cert_file(self, cer_file):
        # TODO
        return None

    def get_not_after(self):
        return self.cer_obj.not_valid_after

    def get_pubkey(self):
        return self.cer_obj.public_key()


class SecdefsCommon:

    NONCE_RANDOM = 24
    NONCE_SEQNUM = 8
    NONCE_SIZE = (NONCE_RANDOM+NONCE_SEQNUM)
    SESSION_KEYLEN = 32
    DIGEST_LENGTH = 32
    ###AES block size used in data encryption
    AES_BLOCKSIZE = 16
    KEY_REFRESH = 30
    TIMESTAMP_SIZE = 8
    ROLENAME_SIZE  = 128
    PROCINFO_SIZE =  8
    PWDID_SIZE = 4
    EXPDATESIZE = 12
    PWDKEY_SIZE_LESS_LOGINDATA = (PWDID_SIZE + ROLENAME_SIZE + DIGEST_LENGTH +  TIMESTAMP_SIZE)       # For public key encryption, the number of bytes
    #    to be encrypted is 11 bytes less than the public key length
    UNUSEDBYTES = 11
    TOKENSIZE = 68
    # User tokens begin with byte values 3,4.
    USERTOKEN_ID_1 = b'\3' 	# User token identifier, must be a sequence
    USERTOKEN_ID_2 = b'\4'	# not allowed in password
    DATA_BLOCK_BIT_SIZE = 128    # data encryption block size in bits.  Java
                                 # supports block size of 128 bits for AES
                                 # algorithm using cryptographic key of 256 bits only.


     #Structure used to describe layout of Encrypted data in login message
    class LoginData:
        #000 Session key

        def __init__(self):
            self.session_key = bytearray(SecdefsCommon.SESSION_KEYLEN)
            #032 Nonce
            self.nonce = bytearray(SecdefsCommon.NONCE_SIZE)
            self.password = b''            # 064 User's password # 128 for 1024 or 256 for 2048

     # Structure used to describe layout of password key

    class PwdKey:
        def __init__(self):
            # 000 Key identifier, binary values 1,2,3,4
            # or 1,2,2,4 keys, optional mode only
            self.id= bytearray(SecdefsCommon.PWDID_SIZE)
            #  004 RolenameA
            self.rolename = bytearray(SecdefsCommon.ROLENAME_SIZE)
            #  132 Digest of server id and encrypted data
            self.digest = bytearray(SecdefsCommon.DIGEST_LENGTH)
            # 164 time stamp
            self.ts = bytearray(SecdefsCommon.TIMESTAMP_SIZE)
            self.data = SecdefsCommon.LoginData()             #172 Encrypted data



