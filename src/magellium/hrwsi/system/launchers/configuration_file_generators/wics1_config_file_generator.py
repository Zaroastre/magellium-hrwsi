#!/usr/bin/env python3
"""This script is to be used at integration stage to generate configuration YAML files at will."""

import logging
from datetime import datetime

import yaml

from magellium.hrwsi.system.launchers.configuration_file_generators.configuration_file_generator import AbstractConfigurationFileGenerator
from magellium.hrwsi.utils.logger import LogUtil
from magellium.hrwsi.utils.s3_client import S3Client
from magellium.hrwsi.utils.vault_client import VaultClient


class WICS1ConfigFileGenerator(AbstractConfigurationFileGenerator):
    """Main class of the YAML config file generator for the WIC S1 processing routine.
    """

    S3_WIC_S1_ROOT="s3://HRWSI/WIC_S1"
    S3_SIGMA0_ROOT="s3://HRWSI-INTERMEDIATE-RESULTS/Backscatter_10m"
    S3_QAS_ROOT="s3://HRWSI-KPI-FILES/WIC_S1"

    def __init__(self, tile_id:str,
                 measurement_date:str,
                 sigma0_name:str) -> None: # pragma no cover
        """Initialization function."""

        self.logger = LogUtil.get_logger("WIC S1 ConfigFileGenerator", logging.INFO)
        super().__init__(tile_id)
        self.sigma0_name = sigma0_name
        self.measurement_date = measurement_date

    def _build_yaml_conf(self) -> bool: # pragma no cover
        """Fills the YAML template located at
        HRWSI_System/launcher/config_file_generation/configuration_file_template.yml
        with values depending on the calss attributes for the WIC S1 processing routine execution.
        """

        with open("/".join(["HRWSI_System","launcher","config_file_generation","configuration_file_template.yml"]),
                "r",
                encoding="UTF-8") as config_template_stream:
            config_template = yaml.safe_load(config_template_stream)


        #### Setting up auxiliaries section
        grassland_name = "".join(["GRA_2018_010m_eu_03035_V1_0_60m_",self.tile_id,".tif"])
        config_template["auxiliaries"]["GRASSLAND"] = "/".join([self.S3_AUX_ROOT,"GRASSLAND","60m",grassland_name])

        imperviousness_name = "".join(["IMD_2018_010m_eu_03035_V2_0_60m_",self.tile_id,".tif"])
        config_template["auxiliaries"]["IMPERVIOUSNESS"] = "/".join([self.S3_AUX_ROOT,"IMPERVIOUSNESS","60m", imperviousness_name])

        tree_cover_density_name = "".join(["TCD_2018_010m_eu_03035_V2_0_60m_",self.tile_id,".tif"])
        config_template["auxiliaries"]["TREE_COVER"] = "/".join([self.S3_AUX_ROOT,"TCD", "60m", tree_cover_density_name])

        water_layer_name = "".join(["WL_2018_60m_",self.tile_id,".tif"])
        config_template["auxiliaries"]["WATER_LAYER"] = "/".join([self.S3_AUX_ROOT,"WL","60m",water_layer_name])

        #### Setting up measurement date section
        ## Checking input data
        try:
            date = datetime.strptime(self.measurement_date,"%Y-%m-%d")
            assert datetime.now() > date > datetime(2016,8,1)

        ## Storing data into template
        except ValueError as error:
            self.logger.critical(f"Wrong format for measurement date, should be YYYY-mm-dd, got {self.measurement_date}.")
            raise error
        except AssertionError as error:
            self.logger.critical(f"Measurement date must be contained between {datetime(2016,8,1)} and {datetime.now()}, got {date}")
            raise error
        config_template["date"] = datetime.strftime(date,"%Y%m%d")

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
        windspeed_filename_path = "/".join(["FMI_WINDSPEED", f"{year}{month}{day}_wind_speed.nc"])
        temperature_filename_path = "/".join(["FMI_TEMPERATURE", f"{year}{month}{day}_t2m_sum.nc"])
        config_template["tile_id"] = self.tile_id
        config_template["input"]["SIGMA0"] = "/".join([self.S3_SIGMA0_ROOT, self.tile_id, year, month, day, self.sigma0_name])
        config_template["input"]["CLASSIFICATION_COEFFICIENTS"] = "/".join([self.S3_AUX_ROOT, "WIC_S1_CLASSIFICATION_COEFFICIENTS", f"cc_60m_{self.tile_id}.tif"])
        config_template["input"]["RADARSHADOW"] = "/".join([self.S3_AUX_ROOT, "S1_RADAR_SHADOW_LAYOVER", "".join(["S1_RADAR_SHADOW_LAYOVER_T",self.tile_id,f"_60m_t{sigma0_relative_orbit}_V20240827.tif"])])
        config_template["input"]["TEMPERATURE"] = "/".join([self.S3_AUX_ROOT, temperature_filename_path])
        config_template["input"]["WATER_CATEGORY"] = "/".join([self.S3_AUX_ROOT, "WIC_S1_WATER_CLASSIFICATION",  f"wc_60m_{self.tile_id}.tif"])
        config_template["input"]["WIND_SPEED"] = "/".join([self.S3_AUX_ROOT, windspeed_filename_path])

        #### Setting up output path section
        product_title = "_".join(["CLMS","WSI","WIC", "060m", f"T{self.tile_id}", "T".join([sigma0_measurement_date, sigma0_measurement_time]),
                                 f"{sigma0_mission_id}",
                                 "V100"])
        output_dst_path = "/".join([self.S3_WIC_S1_ROOT,self.tile_id, year, month, day, product_title, ''])

        config_template["output"]["src"] = "/".join(["","opt","wsi","output", product_title, ""])
        config_template["output"]["dst"] = output_dst_path

        # Check existence of FMI_WINDSPEED and FMI_TEMPERATURE dynamic auxiliaries files
        # If at least one of them not exists, return
        # temperature example: 's3://HRWSI-AUX/FMI_TEMPERATURE/20250126_t2m_sum.nc'
        # wind example: 's3://HRWSI-AUX/FMI_WINDSPEED/20250126_wind_speed.nc'

        unavailable_dynamic_aux = []
        filenames_path = [windspeed_filename_path, temperature_filename_path]
        bucket_name = self.S3_AUX_ROOT[5:]

        try:
            # Get the security credentials needed to connect to the S3
            vault_client = VaultClient()
            s3_credentials = vault_client.read_secret('s3cfg_HRWSI')
            s3_access_key_id = s3_credentials['access_key']
            s3_secret_access_key = s3_credentials['secret_key']
            endpoint_url = s3_credentials['endpoint_url']
            region_name = s3_credentials['region_name']

            # Connect to the S3
            s3_client = S3Client(s3_access_key_id, s3_secret_access_key, endpoint_url, region_name)

            for dynamic_auxiliary_path in filenames_path:
                if not s3_client.check_file_exists(bucket_name, dynamic_auxiliary_path):
                    unavailable_dynamic_aux.append(dynamic_auxiliary_path)

            if unavailable_dynamic_aux:
                self.logger.info(f"The following dynamic auxiliaries are not available for the product {product_title}: {unavailable_dynamic_aux}")
                return True
        except RuntimeError as error:
            raise RuntimeError from error

        #### Setting up log path section
        logs_out_path="/".join([self.S3_LOGS_ROOT,"WIC_S1",self.tile_id,year,month,day,f"{datetime.now().strftime('%Y%m%dT%H%M%S')}_{product_title}.stdout.log"])
        logs_err_path="/".join([self.S3_LOGS_ROOT,"WIC_S1",self.tile_id,year,month,day,f"{datetime.now().strftime('%Y%m%dT%H%M%S')}_{product_title}.stderr.log"])

        config_template["log"]["STDOUT"]["src"] = "/".join(["","opt","wsi","logs","processing_routine.stdout.log"])
        config_template["log"]["STDOUT"]["dst"] = logs_out_path

        config_template["log"]["STDERR"]["src"] = "/".join(["","opt","wsi","logs","processing_routine.stderr.log"])
        config_template["log"]["STDERR"]["dst"] = logs_err_path

        config_template["qas"]["src"] = "/".join(["","opt","wsi","output",product_title+"_QAS.yaml"])
        config_template["qas"]["dst"] = "/".join([self.S3_QAS_ROOT,self.tile_id, year, month, day,product_title+"_QAS.yaml"])

        #### Writting the configuration file to /opt/wsi/config/configuration_file.yml
        with open("/".join(["","tmp","configuration_file.yml"]),
                  "w+", encoding="UTF-8") as config_stream:
            yaml.safe_dump(config_template, config_stream, encoding="UTF-8")

        return False

if __name__ == "__main__": # pragma no cover

    from argparse import ArgumentParser
    parser = ArgumentParser(description="Create the configuration file to launch the FSC processing routine"\
                          "on a Backscatter at 10m or sigma0 map (two names for one thing) from the HRWSI S3"\
                          "bucket.")
    parser.add_argument("--tile-id", type=str, action="store", dest="tile_id", required=True, help="Tile ID to be processed. 32TMS for example.")
    parser.add_argument("--sigma0-name", type=str, action="store", dest="sigma0_name", required=True, help="L2A name. SENTINEL2B_20201215-103755-817_L2A_T32TMS_C_V1-0 for example.")
    parser.add_argument("--measurement-date", type=str, action="store", dest="measurement_date", required=True, help="L1C measurement date."\
                      "2020-12-15 for example.")

    args = parser.parse_args()

    cfg = WICS1ConfigFileGenerator(tile_id=args.tile_id,
                              sigma0_name=args.sigma0_name,
                              measurement_date=args.measurement_date)
    cfg.generate()
