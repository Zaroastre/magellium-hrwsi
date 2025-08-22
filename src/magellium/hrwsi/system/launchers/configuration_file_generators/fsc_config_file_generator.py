#!/usr/bin/env python3
"""This script is to be used at integration stage to generate configuration YAML files at will."""

import logging
from datetime import datetime

import yaml

from magellium.hrwsi.system.launchers.configuration_file_generators.configuration_file_generator import AbstractConfigurationFileGenerator
from magellium.hrwsi.utils.logger import LogUtil


class FSCConfigFileGenerator(AbstractConfigurationFileGenerator):
    """Main class of the YAML config file generator for the FSC processing routine.
    """

    S3_FSC_ROOT="s3://HRWSI/FSC"
    S3_L2A_ROOT="s3://HRWSI-INTERMEDIATE-RESULTS/L2A"
    S3_GVM_ROOT="s3://HRWSI-INTERMEDIATE-RESULTS/FSC"
    S3_QAS_ROOT="s3://HRWSI-KPI-FILES/FSC"

    def __init__(self, tile_id: str,
                 measurement_date: str,
                 l2a_name: str) -> None:
        """Initialization function."""

        self.logger = LogUtil.get_logger("ConfigFileGenerator", logging.INFO)
        super().__init__(tile_id)
        self.l2a_name = l2a_name
        self.measurement_date = measurement_date

    def _build_yaml_conf(self) -> None:
        """Fills the YAML template located at 
        HRWSI_System/launcher/config_file_generation/configuration_file_template.yml 
        with values depending on the calss attributes for the FSC processing routine execution.
        """

        with open("/".join(["HRWSI_System", "launcher", "config_file_generation", "configuration_file_template.yml"]),
                  "r",
                  encoding="UTF-8") as config_template_stream:
            config_template = yaml.safe_load(config_template_stream)

        #### Setting up auxiliaries section
        dem_name = "".join(["Copernicus_DSM_04_N02_00_00_DEM_20m_", self.tile_id, ".tif"])
        config_template["auxiliaries"]["DEM"] = "/".join([self.S3_AUX_ROOT, "DEM", "20m", dem_name])

        water_mask_name = "".join(["WL_2018_20m_", self.tile_id, ".tif"])
        config_template["auxiliaries"]["WATER_MASK"] = "/".join([self.S3_AUX_ROOT, "WL", "20m", water_mask_name])

        tcd_name = "".join(["TCD_2018_010m_eu_03035_V2_0_20m_", self.tile_id, ".tif"])
        config_template["auxiliaries"]["TCD"] = "/".join([self.S3_AUX_ROOT, "TCD", "20m", tcd_name])

        #### Setting up measurement date section
        ## Checking input data
        try:
            date = datetime.strptime(self.measurement_date, "%Y-%m-%d")
            assert datetime.now() > date > datetime(2016, 9, 1)

        ## Storing data into template
        except ValueError as error:
            self.logger.critical(f"Wrong format for measurement date, should be YYYY-mm-dd, got {self.measurement_date}.")
            raise error
        except AssertionError as error:
            self.logger.critical(
                f"Measurement date must be contained between {datetime(2016, 9, 1)} and {datetime.now()}, got {date}")
            raise error
        config_template["date"] = datetime.strftime(date, "%Y%m%d")

        ##### Setting up L2A path section
        ## Checking input data
        year, month, day = self.measurement_date.split("-")
        l2a_mission_id = self.l2a_name.split("_", maxsplit=1)[0]
        l2a_measurement_date = self.l2a_name.split("-", maxsplit=1)[0].split("_")[1]
        l2a_measurement_time = self.l2a_name.split("-", maxsplit=2)[1]
        l2a_tile_id = self.l2a_name.split("_")[3]
        try:
            assert "".join([year, month, day]) == l2a_measurement_date
            assert l2a_mission_id in ["SENTINEL2A", "SENTINEL2B", "SENTINEL2C"]
            assert l2a_tile_id[1:] == self.tile_id
        except AssertionError as error:
            self.logger.critical(f"Wrong L2A name provided: {self.l2a_name}")
            raise error
        ## Storing data into template
        config_template["input"]["L2A"] = "/".join([self.S3_L2A_ROOT, l2a_tile_id[1:], year, month, day, self.l2a_name])

        #### Setting up output path section
        l2a_mission_id = f"S2{l2a_mission_id[-1]}"
        product_title = "_".join(
            ["CLMS", "WSI", "FSC", "020m", l2a_tile_id, "T".join([l2a_measurement_date, l2a_measurement_time]),
             l2a_mission_id, "V200"])
        output_dst_path = "/".join([self.S3_FSC_ROOT, self.tile_id, year, month, day, product_title, ''])

        config_template["output"]["src"] = "/".join(["", "opt", "wsi", "output", product_title, ""])
        config_template["output"]["dst"] = output_dst_path
        
        output_dst_path = "/".join([self.S3_GVM_ROOT, self.tile_id, year, month, day, product_title, f"{product_title}_GV_mask.tif"])
        config_template["intermediates"]["GVmask"]["src"] = "/".join(["", "opt", "wsi", "intermediate", f"{product_title}_GV_mask.tif"])
        config_template["intermediates"]["GVmask"]["dst"] = output_dst_path

        #### Setting up log path section
        logs_out_path = "/".join(["s3://HRWSI-LOGS", "FSC", self.tile_id, year, month, day,
                                  f"{datetime.now().strftime('%Y%m%dT%H%M%S')}_{product_title}_processing_routine.stdout.log"])
        logs_err_path = "/".join(["s3://HRWSI-LOGS", "FSC", self.tile_id, year, month, day,
                                  f"{datetime.now().strftime('%Y%m%dT%H%M%S')}_{product_title}_processing_routine.stderr.log"])

        config_template["log"]["STDOUT"]["src"] = "/".join(["", "opt", "wsi", "logs", "processing_routine.stdout.log"])
        config_template["log"]["STDOUT"]["dst"] = logs_out_path

        config_template["log"]["STDERR"]["src"] = "/".join(["", "opt", "wsi", "logs", "processing_routine.stderr.log"])
        config_template["log"]["STDERR"]["dst"] = logs_err_path

        #### Setting up KPI path section
        config_template["qas"]["src"] = "/".join(["","opt","wsi","output","tmp",product_title+"_QAS.yaml"])
        config_template["qas"]["dst"] = "/".join([self.S3_QAS_ROOT,self.tile_id, year, month, day,product_title+"_QAS.yaml"])

        #### Writting the configuration file to /tmp/configuration_file.yml
        with open("/".join(["", "tmp", "configuration_file.yml"]),
                  "w+", encoding="UTF-8") as config_stream:
            yaml.safe_dump(config_template, config_stream, encoding="UTF-8")
        return


if __name__ == "__main__": # pragma no cover
    from argparse import ArgumentParser

    parser = ArgumentParser(description="Create the configuration file to launch the FSC processing routine" \
                                        "on a L2A from the HRWSI S3 bucket or the HRWSI Google Drive")
    parser.add_argument("--tile-id", type=str, action="store", dest="tile_id", required=True,
                        help="Tile ID to be processed. 32TMS for example.")
    parser.add_argument("--l2a-name", type=str, action="store", dest="l2a_name", required=True,
                        help="L2A name. SENTINEL2B_20201215-103755-817_L2A_T32TMS_C_V1-0 for example.")
    parser.add_argument("--measurement-date", type=str, action="store", dest="measurement_date", required=True,
                        help="L1C measurement date." \
                             "2020-12-15 for example.")

    args = parser.parse_args()

    cfg = FSCConfigFileGenerator(tile_id=args.tile_id,
                                  l2a_name=args.l2a_name,
                                  measurement_date=args.measurement_date)
    cfg.generate()
