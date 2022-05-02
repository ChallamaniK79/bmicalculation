import logging
import os.path
import sys
from datetime import datetime
from src.main.com.utils import constants
from numpy.core.defchararray import lower

v_start_date_time = datetime.now()


def csv_(final_dataframe, output_file_path):
    final_dataframe.to_csv(output_file_path)


def json_(final_dataframe, output_file_path):
    final_dataframe.to_json(output_file_path)


switcher = {constants.CSV: csv_, constants.JSON: json_}


def switch(format_towrite):
    return switcher.get(format_towrite)


def write_data(config, final_dataframe):
    output_file_path = config[constants.FILE_PROTOCOL] + ":/" \
                       + config[constants.OUTPUT_FILE_PATH] \
                       + constants.PROTOCOL_SEPERATOR \

    if not os.path.exists(output_file_path):
        logging.info(f'[{v_start_date_time}]: INFO :Output File is not found path will be created in the below path')
        os.mkdir(output_file_path)
    logging.info(f'[{v_start_date_time}]: INFO :{output_file_path} ')
    #   final_dataframe.to_json(output_file_path)
    output_file_format = config[constants.OUTPUT_FILE_FORMAT]
    print("out format is " + output_file_format)
    switch(output_file_format)

    match output_file_format :
        case constants.CSV : csv_(final_dataframe, output_file_path + config[constants.OUTPUT_FILE_NAME])
        case constants.JSON: json_(final_dataframe, output_file_path + config[constants.OUTPUT_FILE_NAME])
        case _ : {
            logging.error(f'[{v_start_date_time}]: INFO : output file format is not recognised ')

            }
