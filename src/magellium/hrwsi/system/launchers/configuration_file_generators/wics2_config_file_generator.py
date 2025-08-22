#!/usr/bin/env python3
"""This script is to be used at integration stage to generate configuration YAML files at will."""

import logging
from datetime import datetime

import yaml

from magellium.hrwsi.system.launchers.configuration_file_generators.configuration_file_generator import AbstractConfigurationFileGenerator
from magellium.hrwsi.utils.logger import LogUtil


class WICS2ConfigFileGenerator(AbstractConfigurationFileGenerator):
    """Main class of the YAML config file generator for the WIC S2 processing routine.
    """

    S3_AUX_ROOT="s3://HRWSI-AUX"
    S3_WIC_S2_ROOT="s3://HRWSI/WIC_S2"
    S3_L2A_ROOT="s3://HRWSI-INTERMEDIATE-RESULTS/L2A"
    S3_QAS_ROOT="s3://HRWSI-KPI-FILES/WIC_S2"
    S3_LOGS_ROOT="s3://HRWSI-LOGS"
    WORK_DIR = "/".join(["","opt","wsi"])
    PRODUCT_VERSION="V100"

    def __init__(self, tile_id:str,
                 measurement_date:str,
                 l2a_name:str) -> None:
        """Initialization function."""
        self.logger = LogUtil.get_logger("WICS2ConfigFileGenerator", logging.INFO)
        super().__init__(tile_id)
        self.tile_id = tile_id
        self.l2a_name = l2a_name
        self.measurement_date = measurement_date

    def _build_yaml_conf(self)->None:
        """Fills the YAML template located at config/configuration_file.yml with values
        depending on the calss attributes for the WIC S2 processing routine execution.
        """

        with open("/".join(["HRWSI_System","launcher","config_file_generation","configuration_file_template.yml"]),
                "r",
                encoding="UTF-8") as config_template_stream:
            config_template = yaml.safe_load(config_template_stream)

        #### Setting up auxiliaries section
        dem_name = "".join(["Copernicus_DSM_04_N02_00_00_DEM_20m_",self.tile_id,".tif"])
        config_template["auxiliaries"]["DEM"] = "/".join([self.S3_AUX_ROOT,"DEM","20m",dem_name])

        water_layer_name = "".join(["WL_2018_20m_",self.tile_id,".tif"])
        config_template["auxiliaries"]["WATER_LAYER"] = "/".join([self.S3_AUX_ROOT,"WL","20m",water_layer_name])

        slope_name = "_".join(["S2__TEST_AUX_REFDE2",self.tile_id,"1001_SLP_R2.TIF"])
        slope_directory = "_".join(["S2__TEST_AUX_REFDE2",self.tile_id,"1001.DBL.DIR"])
        config_template["auxiliaries"]["SLOPE"] = "/".join([self.S3_AUX_ROOT,"DTM",self.tile_id, slope_directory, slope_name])

        #### Setting up measurement date section
        ## Checking input data
        try:
            date = datetime.strptime(self.measurement_date,"%Y-%m-%d")
            assert datetime.now() >= date >= datetime(2016,8,1)

        ## Storing data into template
        except ValueError as error:
            self.logger.critical( f"Wrong format for measurement date, should be YYYY-mm-dd, got {self.measurement_date}.")
            raise error
        except AssertionError as error:
            self.logger.critical(f"Measurement date must be contained between {datetime(2016,8,1)} and {datetime.now()}, got {date}")
            raise error
        config_template["date"] = datetime.strftime(date,"%Y%m%d")

        ##### Setting up L2A path section
        ## Checking input data
        year, month, day = self.measurement_date.split("-")
        l2a_mission_id = f"S{self.l2a_name.split('_', maxsplit=1)[0][-2:]}"
        l2a_measurement_day = self.l2a_name.split("-", maxsplit=1)[0].split("_")[1]
        l2a_measurement_time = self.l2a_name.split("-", maxsplit=2)[1]
        l2a_tile_id = self.l2a_name.split("_")[3]
        try:
            assert "".join([year, month, day]) == l2a_measurement_day
            assert l2a_mission_id in ["S2A", "S2B", "S2C"]
            assert l2a_tile_id[1:] == self.tile_id
        except AssertionError as error:
            raise error
        ## Storing data into template
        config_template["input"]["L2A"] = "/".join([self.S3_L2A_ROOT, l2a_tile_id[1:], year, month, day, self.l2a_name])

        #### Setting up output path section
        product_name = "_".join(["CLMS","WSI","WIC","020m",
                                 f"T{self.tile_id}","T".join([l2a_measurement_day, l2a_measurement_time]),
                                 l2a_mission_id,self.PRODUCT_VERSION])

        output_dst_path = "/".join([self.S3_WIC_S2_ROOT,self.tile_id, year, month, day, ''])

        config_template["output"]["src"] = "/".join([self.WORK_DIR,"output",product_name])
        config_template["output"]["dst"] = output_dst_path

        #### Setting up log path section
        logs_out_path="/".join([self.S3_LOGS_ROOT,"WIC_S2",self.tile_id,year,month,day,f"{datetime.now().strftime('%Y%m%dT%H%M%S')}_{product_name}.stdout.log"])
        logs_err_path="/".join([self.S3_LOGS_ROOT,"WIC_S2",self.tile_id,year,month,day,f"{datetime.now().strftime('%Y%m%dT%H%M%S')}_{product_name}.stderr.log"])

        config_template["log"]["STDOUT"]["src"] = "/".join([self.WORK_DIR,"logs","processing_routine.stdout.log"])
        config_template["log"]["STDOUT"]["dst"] = logs_out_path

        config_template["log"]["STDERR"]["src"] = "/".join([self.WORK_DIR,"logs","processing_routine.stderr.log"])
        config_template["log"]["STDERR"]["dst"] = logs_err_path

        #### Setting up KPI path section
        config_template["qas"]["src"] = "/".join([self.WORK_DIR, "temp", product_name + "_temp", product_name+"_QAS.yaml"])
        config_template["qas"]["dst"] = "/".join([self.S3_QAS_ROOT,self.tile_id, year, month, day,product_name+"_QAS.yaml"])

        #### Writing the configuration file to /tmp/configuration_file.yml
        with open("/".join(["","tmp","configuration_file.yml"]),
                  "w+", encoding="UTF-8") as config_stream:
            yaml.safe_dump(config_template, config_stream, encoding="UTF-8")
        return

if __name__ == "__main__": # pragma no cover

    from argparse import ArgumentParser
    parser = ArgumentParser(description="Create the configuration file to launch the WIC S2 processing routine"\
                          "on a L2A from the HRWSI S3 bucket.")
    parser.add_argument("--tile-id", type=str, action="store", dest="tile_id", required=True, help="Tile ID to be processed. 32TMS for example.")
    parser.add_argument("--l2a-name", type=str, action="store", dest="l2a_name", required=True, help="L2A name. SENTINEL2B_20201215-103755-817_L2A_T32TMS_C_V1-0 for example.")
    parser.add_argument("--measurement-date", type=str, action="store", dest="measurement_date", required=True, help="L1C measurement date."\
                      "2020-12-15 for example.")

    args = parser.parse_args()

    cfg = WICS2ConfigFileGenerator(tile_id=args.tile_id,
                                   l2a_name=args.l2a_name,
                                   measurement_date=args.measurement_date)
    cfg.generate()
