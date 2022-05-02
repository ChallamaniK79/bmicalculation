import json
import os.path
import sys
import traceback
from datetime import datetime
import logging

from src.main.com.sink.sink import write_data
from src.main.com.sourcereader import source_reader
from src.main.com.utils import utils, logs

v_start_date_time = datetime.now()
v_end_date_time = datetime.now()


# config = None

def main():
    logging.info(f'[{v_start_date_time}]: *** START ****')
    logging.info(f'[{v_start_date_time}]: INFO : Validating Input Arguments...')

    if not os.path.exists(sys.argv[1]):
        logging.error(
            f'[{v_start_date_time}]: ERROR : *** Application configuration file is not found for the below path ****')
        logging.error(f'[{v_start_date_time}]: {sys.argv[1]}')
        sys.exit()
    logging.info(f'[{v_start_date_time}]: INFO : *** Application configuration file is found on the below path ****')
    logging.info(f'[{v_start_date_time}]: {sys.argv[1]}')
    controlfile = sys.argv[1]
    try:
        logging.info(f'[{v_start_date_time}]: INFO : Processing the Config file is started ****')
        config = json.load(open(controlfile, "r"))
        bmi_dataframe = source_reader.source_data_read(config)
        logging.info(f'[{v_start_date_time}]: INFO :Source Data file has been successfully read for to process ****')

        logging.info(f'[{v_start_date_time}]: INFO :BMI Range Calculation started ****')
        bmi_dataframe = bmi_dataframe.assign(
            BmiRange=lambda _: utils.bmi_range_calculation(config, _.WeightKg, _.HeightCm))
        logging.info(f'[{v_start_date_time}]: INFO :BMI Range Calculation Completed ****')

        logging.info(f'[{v_start_date_time}]: INFO :BMI Category and Health Risk Calculation started ****')
        bmi_dataframe[['BmiCategory', 'HealthRisk']] = [utils.bmi_category_calculation(_) for _ in
                                                        bmi_dataframe['BmiRange']]

        logging.info(f'[{v_start_date_time}]: INFO :BMI Category and Health Risk Calculation Completed ****')
        logging.info(f'[{v_start_date_time}]: INFO :Final data frame flush into output location started ****')
        write_data(config, bmi_dataframe)
        logging.info(f'[{v_start_date_time}]: INFO :Final data frame flush into output location Completed ****')
        print(bmi_dataframe.head(10))
        logging.info(f'[{v_start_date_time}]: *** Application Ended successfully ****')

    except (ValueError, FileNotFoundError) as err:
        logging.info(
            "Exception(ValueError" + "fileNotFoundError) Job Failed due to {}".format(
                str(err)))
        logging.error(traceback.print_exc())
    except Exception as ex:
        traceback.print_exc()
        #     logging.error('[{}]: Config file is not found on the given path ****'.format(v_start_date_time))
        logging.error("Exception Due to {} ".format(str(ex)))


if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s %(message)s')
    logging.root.setLevel(logging.NOTSET)
    main()
