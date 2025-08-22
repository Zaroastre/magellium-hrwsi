#!/usr/bin/env python3
"""This script is to be used at integration stage to generate configuration YAML files at will."""

import logging
from datetime import datetime
from typing import List

import yaml

from magellium.hrwsi.system.launchers.configuration_file_generators.configuration_file_generator import AbstractConfigurationFileGenerator
from magellium.hrwsi.utils.logger import LogUtil


class WICS1S2ConfigFileGenerator(AbstractConfigurationFileGenerator):
    """Main class of the YAML config file generator for the WICS1S2 processing routine.
    """

    S3_WICS1S2_ROOT="s3://HRWSI/WIC_S1S2"
    S3_WICS1_ROOT="s3://HRWSI/WIC_S1"
    S3_WICS2_ROOT="s3://HRWSI/WIC_S2"
    S3_QAS_ROOT="s3://HRWSI-KPI-FILES/WIC_S1S2"
    WORK_DIR = "/".join(["","opt","wsi"])
    PRODUCT_VERSION="V100"

    def __init__(self, tile_id:str,
                 measurement_date:str,
                 wic_s1_list:List[str],
                 wic_s2_list:List[str],
                 hour:str) -> None:
        """Initialization function."""
        self.logger = LogUtil.get_logger("WIC S1 S2 ConfigFileGenerator", logging.INFO)
        super().__init__(tile_id)
        self.wic_s1_list = wic_s1_list
        self.wic_s2_list = wic_s2_list
        self.measurement_date = measurement_date
        self.hour=hour

    def _build_yaml_conf(self)->None:
        """Fills the YAML template located at 
        HRWSI_System/launcher/config_file_generation/configuration_file_template.yml 
        with values depending on the calss attributes for the WIC S1S2 processing routine execution.
        """

        with open("/".join(["HRWSI_System","launcher","config_file_generation","configuration_file_template.yml"]),
                "r",
                encoding="UTF-8") as config_template_stream:
            config_template = yaml.safe_load(config_template_stream)

        #### Setting up measurement date section
        ## Checking input data
        try:
            date = datetime.strptime(self.measurement_date,"%Y-%m-%d")
            assert datetime.now() > date > datetime(2016,8,1)

        ## Storing data into template
        except ValueError as error:
            self.logger.critical( f"Wrong format for measurement date, should be YYYY-mm-dd, got {self.measurement_date}.")
            raise error
        except AssertionError as error:
            self.logger.critical(f"Measurement date must be contained between {datetime(2016,8,1)} and {datetime.now()}, got {date}")
            raise error

        ##### Setting up INPUT path section
        ## Storing data into template
        config_template["input"]["measurement_date"] = datetime.strftime(date,"%Y%m%d")
        config_template["input"]["S2_tile"] = self.tile_id

        config_template["input"]["WIC_S1"] = [wic_s1_name for wic_s1_name in self.wic_s1_list]
        config_template["input"]["WIC_S2"] = [wic_s2_name for wic_s2_name in self.wic_s2_list]
        
        #### Setting up auxiliaries section
        water_mask_name = "".join(["WL_2018_20m_",self.tile_id,".tif"])
        config_template["auxiliaries"]["WATER_MASK"] = "/".join([self.S3_AUX_ROOT,"WL","20m",water_mask_name])

        #### Setting up output path section
        if int(self.hour) < 12:
            date_time = "000000"
        else:
            date_time = "120000"

        product_name = "_".join(["CLMS","WSI","WIC","020m",f"T{self.tile_id}",
                                    f"{datetime.strftime(date,'%Y%m%d')}T{date_time}P12H",
                                    "COMB",
                                    self.PRODUCT_VERSION])

        year, month, day = self.measurement_date.split("-")
        output_dst_path = "/".join([self.S3_WICS1S2_ROOT,self.tile_id, year, month, day, product_name, ''])

        config_template["output"]["src"] = "/".join([self.WORK_DIR,"output", product_name, ""])
        config_template["output"]["dst"] = output_dst_path

        #### Setting up log path section
        logs_out_path="/".join([self.S3_LOGS_ROOT,"WIC_S1S2",self.tile_id,year,month,day,f"{datetime.now().strftime('%Y%m%dT%H%M%S')}_{product_name}.stdout.log"])
        logs_err_path="/".join([self.S3_LOGS_ROOT,"WIC_S1S2",self.tile_id,year,month,day,f"{datetime.now().strftime('%Y%m%dT%H%M%S')}_{product_name}.stderr.log"])

        config_template["log"]["STDOUT"]["src"] = "/".join([self.WORK_DIR,"logs","processing_routine.stdout.log"])
        config_template["log"]["STDOUT"]["dst"] = logs_out_path

        config_template["log"]["STDERR"]["src"] = "/".join([self.WORK_DIR,"logs","processing_routine.stderr.log"])
        config_template["log"]["STDERR"]["dst"] = logs_err_path

        #### Setting up KPI path section
        config_template["qas"]["src"] = "/".join([self.WORK_DIR,"temp",product_name+'temp',product_name+"_QAS.yaml"])
        config_template["qas"]["dst"] = "/".join([self.S3_QAS_ROOT,self.tile_id, year, month, day,product_name+"_QAS.yaml"])

        #### Writting the configuration file to /tmp/configuration_file.yml
        with open("/".join(["","tmp","configuration_file.yml"]),
                  "w+", encoding="UTF-8") as config_stream:
            yaml.safe_dump(config_template, config_stream, encoding="UTF-8")
        return

if __name__ == "__main__": # pragma no cover

    from argparse import ArgumentParser
    parser = ArgumentParser(description="Create the configuration file to launch the GFSC processing routine"\
                          "on SWSs and FSCs for a processing day."\
                          "from the HRWSI S3 bucket.")
    parser.add_argument("--tile-id", type=str, action="store", dest="tile_id", required=True, help="Tile ID to be processed. 32TMS for example.")
    parser.add_argument("--wic-s1-name-list", type=str, action="store", dest="wic_s1_name_list", required=True, help="WIC S1 files name..")
    parser.add_argument("--wic-s2-name-list", type=str, action="store", dest="wic_s2_name_list", required=True, help="WIC S2 files name.")
    parser.add_argument("--measurement-date", type=str, action="store", dest="measurement_date", required=True, help="Measurement date."\
                      "2020-12-15 for example.")
    parser.add_argument("--hour", type=str, action="store", dest="hour", required=True, help="WIC_S1 measurement hour")

    args = parser.parse_args()
    cfg = WICS1S2ConfigFileGenerator(tile_id=args.tile_id,
                              wic_s1_list=args.wic_s1_name_list,
                              wic_s2_list=args.wic_s2_name_list,
                              measurement_date=args.measurement_date,
                              hour=args.hour)
    cfg.generate()