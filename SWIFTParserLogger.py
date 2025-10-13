import logging

def getSWIFTParserLogger(file_name,logger_level):

    SWIFTParserLogger = logging.getLogger('SWIFT_LOGGER')
    SWIFTParserLogger.setLevel(logger_level)
    
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    # Handler para el fichero de log
    file_handler = logging.FileHandler(file_name)
    file_handler.setLevel(logger_level)
    file_handler.setFormatter(formatter)
    SWIFTParserLogger.addHandler(file_handler)
    
    # Handler para el terminal
    # console_handler = logging.StreamHandler()
    # console_handler.setLevel(logging.DEBUG)
    # console_handler.setFormatter(formatter)
    # SWIFTParserLogger.addHandler(console_handler)

    return SWIFTParserLogger
