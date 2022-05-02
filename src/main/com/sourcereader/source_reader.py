import logging
import os.path
import sys
import traceback
from datetime import datetime

v_start_date_time = datetime.now()
import pandas as pd

from src.main.com.utils import constants


def source_data_read(config):
    try:
        source_file_path = config[constants.FILE_PROTOCOL] + ":/" \
                           + config[constants.BUCKET_NAME] \
                           + constants.PROTOCOL_SEPERATOR \
                           + config[constants.PATH_PREFIX] \
                           + constants.PROTOCOL_SEPERATOR \
                           + config[constants.SOURCE_FILE_NAME]
        logging.info(f'[{v_start_date_time}]: INFO :Source Data file found on this path {source_file_path} .')

        if not os.path.exists(source_file_path):
            raise FileNotFoundError
        return pd.read_json(source_file_path)
    except FileNotFoundError as err:
        logging.error("Error {}".format(str(err)))
        logging.error(traceback.print_exc())
        sys.exit()
