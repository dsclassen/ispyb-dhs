# -*- coding: utf-8 -*-
"""
ispybDHS
"""
__author__ = "Scott Classen"
__copyright__ = "Scott Classen"
__license__ = "mit"

import os
import sys
import logging
from logging.handlers import RotatingFileHandler
import coloredlogs
import verboselogs
import signal
import yaml

from dotty_dict.dotty_dict import Dotty
from dotty_dict import dotty as dot
from datetime import datetime
from pathlib import Path

from pydhsfw.processors import  Context, register_message_handler
from pydhsfw.dhs import Dhs, DhsInit, DhsStart, DhsContext
from pydhsfw.dcss import DcssContext, DcssStoCSendClientType, DcssHtoSClientIsHardware, DcssStoHRegisterOperation, DcssStoHStartOperation, DcssHtoSOperationUpdate, DcssHtoSOperationCompleted, register_dcss_start_operation_handler

from ispyb_dhs import __version__

_logger = verboselogs.VerboseLogger('ispybDHS')


class ISPyBDHSConfig(Dotty):
    """Class to wrap DHS configuration settings."""
    def __init__(self, conf_dict:dict):
        super().__init__(conf_dict)

    @property
    def dcss_url(self):
        return 'dcss://' + str(self['dcss.host']) + ':' + str(self['dcss.port'])

    @property
    def log_dir(self):
        return self['ispybdhs.log_dir']

@register_message_handler('dhs_init')
def dhs_init(message:DhsInit, context:DhsContext):
    """DHS initialization handler function."""
    parser = message.parser

    parser.add_argument(
        '--version',
        action='version',
        version='ispybDHS version {ver}'.format(ver=__version__))
    parser.add_argument(
        dest='beamline',
        help='Beamline Name (e.g. BL-831 or SIM831). This determines which beamline-specific config file to load from config directory.',
        metavar='Beamline')
    parser.add_argument(
        dest='dhs_name',
        help='Optional alternate DHS Name (e.g. what DCSS is expecting this DHS to be named). If omitted then this value is set to be the name of this script.',
        metavar='DHS Name',
        nargs='?',
        default=Path(__file__).stem)
    parser.add_argument(
        '-v',
        dest='verbosity',
        help='Sets the chattiness of logging (none to -vvvv)',
        action='count',
        default=0)

    args = parser.parse_args(message.args)

    logfile = configure_logging(args.verbosity)

    conf_file = 'config/' + args.beamline + '.config'
    with open(conf_file, 'r') as f:
        yconf = yaml.safe_load(f)
        context.config = ISPyBDHSConfig(yconf)
    context.config['DHS'] = args.dhs_name

    loglevel_name = logging.getLevelName(_logger.getEffectiveLevel())

    #context.state = LoopDHSState()

    _logger.success('=============================================')
    _logger.success(f'Initializing DHS')
    _logger.success(f'Start Time: {datetime.now()}')
    _logger.success(f'Logging level: {loglevel_name}')
    _logger.success(f'Log File: {logfile}')
    _logger.success(f'Config file: {conf_file}')
    _logger.success(f'Initializing: {context.config["DHS"]}')
    _logger.success(f'DCSS HOST: {context.config["dcss.host"]} PORT: {context.config["dcss.port"]}')
    #_logger.success(f'AUTOML HOST: {context.config["loopdhs.automl.host"]} PORT: {context.config["loopdhs.automl.port"]}')
    #_logger.success(f'JPEG RECEIVER PORT: {context.config["loopdhs.jpeg_receiver.port"]}')
    #_logger.success(f'AXIS HOST: {context.config["loopdhs.axis.host"]} PORT: {context.config["loopdhs.axis.port"]}')
    _logger.success('=============================================')

@register_message_handler('dhs_start')
def dhs_start(message:DhsStart, context:DhsContext):
    """DHS start handler"""
    # Connect to DCSS
    context.create_connection('dcss_conn', 'dcss', context.config.dcss_url)
    context.get_connection('dcss_conn').connect()

    # Connect to ISPyB
    #context.create_connection('ispyb_conn', 'ispyb', context.config.ispyb_url)
    #context.get_connection('ispyb_conn').connect()

@register_message_handler('stoc_send_client_type')
def dcss_send_client_type(message:DcssStoCSendClientType, context:Context):
    """Send client type to DCSS during initial handshake."""
    context.get_connection('dcss_conn').send(DcssHtoSClientIsHardware(context.config['DHS']))

@register_message_handler('stoh_register_operation')
def dcss_reg_operation(message:DcssStoHRegisterOperation, context:Context):
    """Register the operations that DCSS has assigned to this DHS."""
    # Need to deal with unimplemented operations
    _logger.success(f'REGISTER: {message}')

@register_message_handler('stoh_start_operation')
def dcss_start_operation(message:DcssStoHStartOperation, context:Context):
    """Handle incoming requests to start an operation."""
    _logger.info(f"FROM DCSS: {message}")
    op = message.operation_name
    opid = message.operation_handle
    _logger.debug(f"OPERATION: {op}, HANDLE: {opid}")

def configure_logging(verbosity):

    loglevel = 20

    if verbosity >= 4:
        _logger.setLevel(logging.SPAM)
        loglevel = 5
    elif verbosity >= 3:
        _logger.setLevel(logging.DEBUG)
        loglevel = 10
    elif verbosity >= 2:
        _logger.setLevel(logging.VERBOSE)
        loglevel = 15
    elif verbosity >= 1:
        _logger.setLevel(logging.NOTICE)
        loglevel = 25
    elif verbosity <= 0:
        _logger.setLevel(logging.WARNING)
        loglevel = 30

    #verboselogs.install()

    logdir = 'logs'

    if not os.path.exists(logdir):
        os.makedirs(logdir)

    logfile = os.path.join(logdir,Path(__file__).stem + '.log')
    handler = RotatingFileHandler(logfile, maxBytes=100000, backupCount=5)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    handler.setLevel(loglevel)
    _logger.addHandler(handler)



    # By default the install() function installs a handler on the root logger,
    # this means that log messages from your code and log messages from the
    # libraries that you use will all show up on the terminal.
    #coloredlogs.install(level='DEBUG')
    
    # If you don't want to see log messages from libraries, you can pass a
    # specific logger object to the install() function. In this case only log
    # messages originating from that logger will show up on the terminal.
    #coloredlogs.install(level='DEBUG', logger=logger)

    coloredlogs.install(level=loglevel,fmt='%(asctime)s,%(msecs)03d %(hostname)s %(name)s[%(funcName)s():%(lineno)d] %(levelname)s %(message)s')

    # LOG LEVELS AVAILABLE IN verboselogs module
    #  5 SPAM
    # 10 DEBUG
    # 15 VERBOSE
    # 20 INFO
    # 25 NOTICE
    # 30 WARNING
    # 35 SUCCESS
    # 40 ERROR
    # 50 CRITICAL

    # EXAMPLES
    # _logger.spam("this is a spam message")
    # _logger.debug("this is a debugging message")
    # _logger.verbose("this is a verbose message")
    # _logger.info("this is an informational message")
    # _logger.notice("this is a notice message")
    # _logger.warning("this is a warning message")
    # _logger.success("this is a success message")
    # _logger.error("this is an error message")
    # _logger.critical("this is a critical message")
    return logfile

def run():
    """Entry point for console_scripts."""

    main(sys.argv[1:])

def main(args):
    """Main entry point for allowing external calls."""

    dhs = Dhs()
    dhs.start()
    sigs = {}
    sigs = {signal.SIGINT, signal.SIGTERM}
    dhs.wait(sigs)

if __name__ == '__main__':
    run()
