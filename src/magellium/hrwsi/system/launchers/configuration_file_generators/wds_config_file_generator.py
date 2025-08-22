#!/usr/bin/env python3
"""This script is to be used at integration stage to generate configuration YAML files at will."""

import logging
from datetime import datetime
from typing import List

import yaml

from magellium.hrwsi.system.launchers.configuration_file_generators.configuration_file_generator import AbstractConfigurationFileGenerator
from magellium.hrwsi.utils.logger import LogUtil


class WDSConfigFileGenerator(AbstractConfigurationFileGenerator):
    """Main class of the YAML config file generator for the WDS processing routine.
    """

    S3_WDS_ROOT = "s3://HRWSI/WDS"
    S3_SIGMA0_ROOT = "s3://HRWSI-INTERMEDIATE-RESULTS/Backscatter_10m"
    S3_FSC_ROOT = "s3://HRWSI/FSC"
    S3_QAS_ROOT="s3://HRWSI-KPI-FILES/WDS"

    def __init__(self, tile_id: str,
                 measurement_date: str,
                 sigma0_name: str,
                 fsc_list: List[str]) -> None:
        """Initialization function."""

        self.logger = LogUtil.get_logger("SWS ConfigFileGenerator", logging.INFO)
        super().__init__(tile_id)
        self.sigma0_name = sigma0_name
        self.fsc_list = fsc_list
        self.measurement_date = measurement_date

    def _build_yaml_conf(self) -> None:
        """Fills the YAML template located at 
        HRWSI_System/launcher/config_file_generation/configuration_file_template.yml 
        with values depending on the class attributes for the WDS processing routine execution.
        """

        with open("/".join(["HRWSI_System", "launcher", "config_file_generation", "configuration_file_template.yml"]),
                  "r",
                  encoding="UTF-8") as config_template_stream:
            config_template = yaml.safe_load(config_template_stream)

        #### Setting up measurement date section
        ## Checking input data
        try:
            date = datetime.strptime(self.measurement_date, "%Y-%m-%d")
            assert datetime.now() > date > datetime(2016, 8, 1)

        ## Storing data into template
        except ValueError as error:
            self.logger.critical(f"Wrong format for measurement date, should be YYYY-mm-dd, got {self.measurement_date}.")
            raise error
        except AssertionError as error:
            self.logger.critical(
                f"Measurement date must be contained between {datetime(2016, 8, 1)} and {datetime.now()}, got {date}")
            raise error
        config_template["date"] = datetime.strftime(date, "%Y%m%d")

        ##### Setting up INPUT path section
        ## Checking input data
        year, month, day = self.measurement_date.split("-")
        sigma0_mission_id = self.sigma0_name.split("_")[-2][:3]
        sigma0_measurement_date = self.sigma0_name.split("_")[1].split("T")[0]
        sigma0_measurement_time = self.sigma0_name.split("_")[1].split("T")[1]
        sigma0_tile_id = self.sigma0_name.split("_")[5][1:]
        sigma0_relative_orbit = self.sigma0_name.split("_")[4]

        try:
            assert "".join([year, month, day]) == sigma0_measurement_date
            assert sigma0_mission_id in ["S1A", "S1B", "S1C"]
            assert sigma0_tile_id == self.tile_id
        except AssertionError as error:
            self.logger.critical(f"Wrong Backscatter at 10m name provided: {self.sigma0_name}")
            raise error

        ## Storing data into template
        config_template["input"]["SIGMA0"] = "/".join(
            [self.S3_SIGMA0_ROOT, self.tile_id, year, month, day, self.sigma0_name])
        config_template["input"]["FSCs"] = ["/".join([self.S3_FSC_ROOT, self.tile_id, year, month, day, fsc_name]) for
                                            fsc_name in self.fsc_list]

        #### Setting up auxiliaries section
        mask_forest_urban_water_name = "".join(["MASK_FOREST_URBAN_WATER_T", self.tile_id, "_60m_V20240827.tif"])
        config_template["auxiliaries"]["MASK_FOREST_URBAN_WATER"] = "/".join(
            [self.S3_AUX_ROOT, "MASK_FOREST_URBAN_WATER", mask_forest_urban_water_name])

        s1_reference_name = "".join(["S1_REFERENCE_T", self.tile_id, f"_60m_t{sigma0_relative_orbit}_V20240827.tif"])
        config_template["auxiliaries"]["S1_REFERENCE"] = "/".join([self.S3_AUX_ROOT, "S1_REFERENCE", s1_reference_name])

        s1_radar_shadow_layover_name = "".join(["S1_RADAR_SHADOW_LAYOVER_T",self.tile_id,f"_60m_t{sigma0_relative_orbit}_V20240827.tif"])
        config_template["auxiliaries"]["S1_RADAR_SHADOW_LAYOVER"] = "/".join(
            [self.S3_AUX_ROOT, "S1_RADAR_SHADOW_LAYOVER", s1_radar_shadow_layover_name])

        s1_incidence_angle_name = "".join(["S1_INCIDENCE_ANGLE_T", self.tile_id, f"_60m_t{sigma0_relative_orbit}_V20240827.tif"])
        config_template["auxiliaries"]["S1_INCIDENCE_ANGLE"] = "/".join(
            [self.S3_AUX_ROOT, "S1_INCIDENCE_ANGLE", s1_incidence_angle_name])

        #### Setting up output path section

        product_title = "_".join(["CLMS", "WSI", "WDS", "060m",
                                 f"T{self.tile_id}",
                                 "T".join([sigma0_measurement_date, sigma0_measurement_time]),
                                 f"{sigma0_mission_id}",
                                 "V200"])
        output_dst_path = "/".join([self.S3_WDS_ROOT, self.tile_id, year, month, day, product_title, ''])

        config_template["output"]["src"] = "/".join(["", "opt", "wsi", "output", product_title, ""])
        config_template["output"]["dst"] = output_dst_path

        #### Setting up log path section
        logs_out_path = "/".join([self.S3_LOGS_ROOT, "WDS", self.tile_id, year, month, day,
                                  f"{datetime.now().strftime('%Y%m%dT%H%M%S')}_{product_title}.stdout.log"])
        logs_err_path = "/".join([self.S3_LOGS_ROOT, "WDS", self.tile_id, year, month, day,
                                  f"{datetime.now().strftime('%Y%m%dT%H%M%S')}_{product_title}.stderr.log"])

        config_template["log"]["STDOUT"]["src"] = "/".join(["", "opt", "wsi", "logs", "processing_routine.stdout.log"])
        config_template["log"]["STDOUT"]["dst"] = logs_out_path

        config_template["log"]["STDERR"]["src"] = "/".join(["", "opt", "wsi", "logs", "processing_routine.stderr.log"])
        config_template["log"]["STDERR"]["dst"] = logs_err_path

        #### Setting up KPI path section
        config_template["qas"]["src"] = "/".join(["","opt","wsi","output",product_title+"_QAS.yaml"])
        config_template["qas"]["dst"] = "/".join([self.S3_QAS_ROOT,self.tile_id, year, month, day,product_title+"_QAS.yaml"])


        #### Writting the configuration file to /tmp/configuration_file.yml
        with open("/".join(["", "tmp", "configuration_file.yml"]),
                  "w+", encoding="UTF-8") as config_stream:
            yaml.safe_dump(config_template, config_stream, encoding="UTF-8")
        return


if __name__ == "__main__": # pragma no cover

    from argparse import ArgumentParser

    parser = ArgumentParser(description="Create the configuration file to launch the WDS processing routine" \
                                        "on a Backscatter at 10m or sigma0 map (two names for one thing) and 1 or 2 FSC " \
                                        "from the HRWSI S3 bucket.")
    parser.add_argument("--tile-id", type=str, action="store", dest="tile_id", required=True,
                        help="Tile ID to be processed. 32TMS for example.")
    parser.add_argument("--sigma0-name", type=str, action="store", dest="sigma0_name", required=True,
                        help="Sigam0 name. SENTINEL2B_20201215-103755-817_L2A_T32TMS_C_V1-0 for example.")
    parser.add_argument("--fsc-name-list", type=str, action="store", dest="fsc_name_list", required=True,
                        help="FSC files name. for example.")
    parser.add_argument("--measurement-date", type=str, action="store", dest="measurement_date", required=True,
                        help="L1C measurement date." \
                             "2020-12-15 for example.")

    args = parser.parse_args()
    if len(args.fsc_name_list.split(" ")) == 1:
        args.fsc_name_list = [args.fsc_name_list]
    cfg = WDSConfigFileGenerator(tile_id=args.tile_id,
                                  sigma0_name=args.sigma0_name,
                                  fsc_list=args.fsc_name_list,
                                  measurement_date=args.measurement_date)
    cfg.generate()
