from logging.handlers import SocketHandler
import ssl
import logging
import json


class LogstashHandler(SocketHandler):
    def __init__(self, host, port, keyfile=None, certfile=None, ssl=True):
        SocketHandler.__init__(self, host, port)
        self.keyfile = keyfile
        self.certfile = certfile
        self.ssl = ssl

    def makeSocket(self, timeout=1):
        s = SocketHandler.makeSocket(self, timeout)
        if self.ssl:
            return ssl.wrap_socket(s, keyfile=self.keyfile, certfile=self.certfile)
        return s

    def makePickle(self, record):
        """
         Just format the record according to the formatter. A new line is appended to
         support streaming listeners.
        """
        return (self.format(record) + "\n").encode("utf-8")


class LogstashFormatter(logging.Formatter):

    RESERVED_ATTRS = (
        'args', 'asctime', 'created', 'exc_info', 'exc_text', 'filename',
        'funcName', 'levelname', 'levelno', 'lineno', 'module',
        'msecs', 'message', 'msg', 'name', 'pathname', 'process',
        'processName', 'relativeCreated', 'stack_info', 'thread', 'threadName')

    DEFAULT_FIELDS = ('asctime', 'levelname', 'filename', 'funcName', 'msg', 'exc_info',)

    DEFAULT_MAPPING = {
        'asctime': 'logGenerationTime',
    }

    def __init__(self, fmt=None, datefmt=None, rename=None, version="1", *args, **kwargs):

        super(LogstashFormatter, self).__init__(fmt, datefmt, *args, **kwargs)

        if isinstance(fmt, (list, tuple)):
            self.fields = [f for f in fmt if f in self.RESERVED_ATTRS]
        else:
            self.fields = self.DEFAULT_FIELDS

        self.datefmt = datefmt
        self.rename_map = rename or self.DEFAULT_MAPPING
        self.version = version

    def format(self, record):
        _msg = record.msg

        record.asctime = self.formatTime(record, self.datefmt)

        if isinstance(_msg, dict):
            msg_dict = _msg
        else:
            msg_dict = {}
            record.message = record.getMessage()

        extra_dict = {k: v for k, v in record.__dict__.items()
                      if k not in self.RESERVED_ATTRS and not k.startswith('_')}

        # Fields specified at "fmt"
        fields_dict = {k: v for k, v in record.__dict__.items() if k in self.fields}

        # Adding fields coming from base message
        fields_dict.update(msg_dict)

        # Adding extra fields
        fields_dict.update(extra_dict)

        # Replacing fields names if rename mapping exists
        for k, v in self.rename_map.items():
            if k in fields_dict.keys():
                fields_dict[v] = fields_dict.pop(k)

        # Adding logging schema version if exists
        if self.version:
            fields_dict['@version'] = self.version

        return json.dumps(fields_dict, default=str)
