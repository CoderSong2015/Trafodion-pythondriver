import os
from . import errors

class SecPwd:

    NONCE_RANDOM = 24
    NONCE_SEQNUM = 8
    NONCE_SIZE = (NONCE_RANDOM + NONCE_SEQNUM)
    SESSION_KEYLEN = 32
    DIGEST_LENGTH = 32
    # AES block size used in data encryption
    AES_BLOCKSIZE = 16
    KEY_REFRESH = 30
    TIMESTAMP_SIZE = 8
    ROLENAME_SIZE = 128
    PROCINFO_SIZE = 8
    PWDID_SIZE = 4
    EXPDATESIZE = 12
    PWDKEY_SIZE_LESS_LOGINDATA = (PWDID_SIZE + ROLENAME_SIZE + DIGEST_LENGTH + TIMESTAMP_SIZE)

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

    def open_certificate(self):
        if not self.spj_mode:  # do nothing for the token case
            self.security = Security(self.cer_filepath)

    def encrypt_pwd(self, pwd, rolename, pwdkey):
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
        if not pwdkey:
            raise errors.NotSupportedError
        if self.spj_mode: # token
            pass
        else:
            self.security.encrypt_pwd(pwd, rolename, self.proc_info, pwdkey)


class Security:
    def __init__(self, cer):
        pass

    def encrypt_pwd(self, pwd, rolename, proc_info, pwdkey):
        pass





