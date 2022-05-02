import logging
import sys
from datetime import datetime
from src.main.com.utils import constants

v_start_date_time = datetime.now()


def bmi_range_calculation(config, weight_kg, height_cm):
    try:
        logging.info(f'[{v_start_date_time}]: INFO :BMI Range Calculation in Progress ****')
        bmi_range = round(weight_kg / (height_cm * 0.01), 2)
        return bmi_range
    except Exception as Err:
        logging.error(f'[{v_start_date_time}]:ERROR : Process failed due to {str(Err)}')
        sys.exit()


def bmi_category_calculation(bmi_range):

    if bmi_range.__ge__(constants.VALUE_40):
        return constants.VERY_SEVERELY_OBESE, constants.VERY_HIGH_RISK
    elif bmi_range.__ge__(constants.VALUE_35) and bmi_range.__lt__(constants.VALUE_40):
        return constants.SEVERELY_OBESE, constants.HIGH_RISK
    elif bmi_range.__ge__(constants.VALUE_30) and bmi_range.__lt__(constants.VALUE_35):
        return constants.MODERATELY_OBESE, constants.MEDIUM_RISK
    elif bmi_range.__ge__(constants.VALUE_25) and bmi_range.__lt__(constants.VALUE_30):
        return constants.OVER_WEIGHT, constants.ENHANCED_RISK
    elif bmi_range.__ge__(constants.VALUE_18_5) and bmi_range.__lt__(constants.VALUE_25):
        return constants.NORMAL_WEIGHT, constants.LOW_RISK
    elif bmi_range.__lt__(constants.VALUE_18_5) and bmi_range.__gt__(constants.VALUE_0):
        return constants.UNDER_WEIGHT, constants.MAL_NUT_RISK
    else:
        return None, None
