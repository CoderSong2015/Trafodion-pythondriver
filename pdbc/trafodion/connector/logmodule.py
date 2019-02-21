import logging
import logging.config
from . import errors


class PyLog:
    global_logger = None

    @staticmethod
    def set_global_logger(logger):
        PyLog.global_logger = logger
        return PyLog.global_logger

    def __init__(self, file_path=None, logger_name=None):
        self.logger_name = logger_name
        self.logger = None

        if not file_path:
            pass
            #self.init_default()
        else:
            self.init_conf(file_path)

    def init_default(self):
        #TODO
        self.logger = logging.getLogger(self.logger_name)

    def init_conf(self, conf_file):
        logging.config.fileConfig(conf_file)
        self.logger = logging.getLogger(self.logger_name)

    def add_handle(self, handle):
        try:
            self.logger.addHandler(handle)
        except:
            errors.InternalError("add logging handle error")

    def get_logger(self):
        return logging.getLogger(self.logger_name)

    def set_debug(self, message, exc_info=None):
        if not self.logger:
            return
        if exc_info is not None:
            self.logger.debug(message, exc_info)
        else:
            self.logger.debug(message)

    def set_info(self, message, exc_info=None):
        if not self.logger:
            return
        if exc_info is not None:
            self.logger.info(message, exc_info)
        else:
            self.logger.info(message)

    def set_warn(self, message, exc_info=None):
        if not self.logger:
            return
        if exc_info is not None:
            self.logger.warning(message, exc_info)
        else:
            self.logger.warning(message)

    def set_error(self, message, exc_info=None):
        if not self.logger:
            return
        if exc_info is not None:
            self.logger.error(message, exc_info)
        else:
            self.logger.error(message)

    def set_critical(self, message, exc_info=None):
        if not self.logger:
            return
        if exc_info is not None:
            self.logger.critical(message, exc_info)
        else:
            self.logger.critical(message)



