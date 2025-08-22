#!/usr/bin/env python3
"""This script is to  be used at integration stage to generate configuration YAML files at will."""
import logging
from datetime import datetime

import yaml

from magellium.hrwsi.system.launchers.configuration_file_generators.configuration_file_generator import AbstractConfigurationFileGenerator

from magellium.serviceproviders.s3 import S3ServiceProvider, S3Client
from magellium.serviceproviders.vault import VaultServiceProvider, HashcorpVaultClient

from magellium.hrwsi.utils.logger import LogUtil
from magellium.hrwsi.utils.s3_client import S3Client


class CCConfigFileGenerator(AbstractConfigurationFileGenerator):
    """Main class of the YAML config file generator for the CC processing routine.
    """

    S3_CC_ROOT = "s3://HRWSI/CC"
    S3_L2A_ROOT = "s3://HRWSI-INTERMEDIATE-RESULTS/L2A"
    S3_L1C_ROOT = "s3://EODATA/Sentinel-2/MSI/L1C"
    S3_QAS_ROOT="s3://HRWSI-KPI-FILES/CC"
    S3_L1C_ROOT_COLLECTION_1 = "s3://EODATA/Sentinel-2/MSI/L1C_N0500"
    S2_MISSION_ID = ["S2A", "S2B"]
    WORK_DIR = "/".join(["","opt","wsi"])
    PRODUCT_VERSION="V100"

    def __init__(self,
                 maja_run_mode:str,
                 sentinel2_tile_id:str,
                 l1c_measurement_date:str,
                 product_measurement_date:str,
                 input_l1c_name:str,
                 input_l2a_name:str=None
                 ) -> None:
        """Initialization method

        Args:
            maja_run_mode (str): MAJA run mode from ["L2INIT", "L2NOMINAL"]
            sentinel2_tile_id (str): Sentinel-2 tile of the L1C e-g "31TCH"
            l1c_measurement_date (str): Measurement date of the L1C as %Y-%m-%d
            product_measurement_date (str) : Measurement date of the L1C for the product path as %Y%m%dT%H%M%S
            input_l1c_name (str): Name of the input L1C to use
            input_l2a_name (str, optional): name of the input L2A to use to be sued if run_mode is "L2NOMINAL", default to None

        Raises:
            AssertionError: Raised if run_mode is not in ["L2INIT", "L2NOMINAL"]
        """
        self.logger = LogUtil.get_logger("ConfigFileGenerator", logging.INFO)
        super().__init__(sentinel2_tile_id)
        try:
            assert maja_run_mode in ["L2INIT", "L2NOMINAL"]
        except AssertionError as error:
            self.logger.critical(error)
            raise error

        self.run_mode = maja_run_mode
        self.tile_id = sentinel2_tile_id
        self.l1c_name = input_l1c_name
        self.l2a_name = input_l2a_name
        self.l1c_measurement_date = l1c_measurement_date
        self.S3_AUX_BUCKET = self.S3_AUX_ROOT[5:]
        self.product_measurement_date = product_measurement_date

    def _build_yaml_conf(self)-> bool:
        """
        Fills the YAML template located at /tmp/configuration_file.yml with values
        depending on the class attributes for the CC processing routine execution.

        Raises:
            error: FileExistsError if the configuration file template is not found.
            error: ValueError if the measurment date is not in the proper format
            error: AssertionError if the measurement date is out of scope
            error: _description_
        """
        #### Open the configuration file template
        #TODO When merging in the nrt system, remove the dependency on the run mode for the template
        # as only 1 template is to be used and that the processing routine only reads the fileds it is interested in.
        config_file_template_path = "/".join(["HRWSI_System","launcher","config_file_generation","configuration_file_template.yml"])

        try:
            with open(config_file_template_path,
                  "r", encoding="UTF-8") as config_template_stream:
                config_template = yaml.safe_load(config_template_stream)
        except FileExistsError as error: # pragma no cover
            self.logger.critical("Configuration file creation failed as template was not found at location %s", config_file_template_path)
            raise FileExistsError from error

        try:
            l1c_measurement_date = datetime.strptime(self.l1c_measurement_date, "%Y-%m-%d")
            year, month, day = self.l1c_measurement_date.split("-")

            #### Setting up auxiliaries section
            cams_folder_path = "/".join(["CAMS", year, month, day, ""])
            config_template["auxiliaries"]["DTM"] = "/".join([self.S3_AUX_ROOT, "DTM", self.tile_id, ""])
            config_template["auxiliaries"]["GIPP"] = "/".join([self.S3_AUX_ROOT, "GIPP", "GIPP_DATA.zip"])
            config_template["auxiliaries"]["CAMS"] = "/".join([self.S3_AUX_ROOT, cams_folder_path])

            # Check that the required CAMS content folder is not empty
            try:
                # Get the security credentials needed to connect to the S3
                vault_client: VaultServiceProvider = HashcorpVaultClient()
                s3_credentials_prod = vault_client.read_secret('s3cfg_HRWSI')
                s3_access_key_id = s3_credentials_prod['access_key']
                s3_secret_access_key = s3_credentials_prod['secret_key']
                endpoint_url = s3_credentials_prod['endpoint_url']
                region_name = s3_credentials_prod['region_name']

                # Connect to the S3
                s3_client = S3Client(s3_access_key_id, s3_secret_access_key, endpoint_url, region_name)

                if not s3_client.check_folder_exists_and_not_empty(self.S3_AUX_BUCKET, cams_folder_path):
                    self.logger.info("CAMS auxiliaries not found at location %s", cams_folder_path)
                    return True

            except RuntimeError as error:
                self.logger.critical("S3 operating issue : %s.", error)
                raise RuntimeError from error

            #### Setting up conf section -- specific to CC
            config_template["conf"]["maja_userconf"] = "/".join([self.S3_AUX_ROOT, "USERCONF", ""])

            #### Setting up measurement date section
            ## Checking input data measurement date
            assert datetime.now() > l1c_measurement_date > datetime(2016,8,1)

        except ValueError as error:
            self.logger.critical("Wrong format for measurement date, should be YYYY-mm-dd, got %s.",self.l1c_measurement_date)
            raise error
        except AssertionError as error:
            self.logger.critical(f"Measurement date must be contained between {datetime(2016,8,1)} and {datetime.now()}, got {l1c_measurement_date}")
            raise error

        ## Storing data into template

        ##### Setting up Input
        ## L1C
        l1c_mission_id = self.l1c_name.split("_", maxsplit=1)[0]
        l1c_measurement_day = self.l1c_name.split("_", maxsplit=3)[2].split("T")[0]
        l1c_tile_id = self.l1c_name.split("_")[5][1:]
        try:
            assert "".join([year, month, day]) == l1c_measurement_day
            assert l1c_mission_id in self.S2_MISSION_ID
            assert l1c_tile_id == self.tile_id
        except AssertionError as error:
            self.logger.critical("Wrong L1C name provided: %s",self.l1c_name)
            self.logger.critical("Expected tile %s, got %s",self.tile_id, l1c_tile_id)
            self.logger.critical("Expected mission ID %s, got %s", str(self.S2_MISSION_ID), l1c_mission_id)
            self.logger.critical("Expected measurement date %s, got %s",l1c_measurement_day, "".join([year, month, day]))
            raise error
        ## Storing data into template
        # Reprocessing of collection one case
        if l1c_measurement_date < datetime(year=2022,month=1,day=1):
            config_template["input"]["L1C"] = "/".join([self.S3_L1C_ROOT_COLLECTION_1, year, month, day, self.l1c_name])
        else:
            config_template["input"]["L1C"] = "/".join([self.S3_L1C_ROOT, year, month, day, self.l1c_name])
            
        config_template["input"]["measurement_date"] = l1c_measurement_day

        ## L2A if run_mode is L2NOMINAL
        if self.run_mode == "L2NOMINAL":
            l2a_mission_id = self.l2a_name.split("_", maxsplit=1)[0]
            l2a_measurement_date = self.l2a_name.split("-", maxsplit=1)[0].split("_")[1]
            l2a_year = l2a_measurement_date[:4]
            l2a_month = l2a_measurement_date[4:6]
            l2a_day = l2a_measurement_date[6:8]
            l2a_tile_id = self.l2a_name.split("_")[3]
            try:
                assert l2a_mission_id in ["SENTINEL2A", "SENTINEL2B", "SENTINEL2C"]
                assert l2a_tile_id[1:] == self.tile_id
            except AssertionError as error:
                self.logger.critical("Wrong L2A name provided: %s", self.l2a_name)
                self.logger.critical("Expected tile %s, got %s",self.tile_id, l2a_tile_id)
                self.logger.critical("Expected mission ID %s, got %s",str(self.S2_MISSION_ID),
                                     l2a_mission_id)
                self.logger.critical("Expected measurement date %s, got %s",l1c_measurement_day,
                                     "".join([l2a_year, l2a_month, l2a_day]))
                raise error
            ## Storing data into template
            config_template["input"]["L2A"] = "/".join([self.S3_L2A_ROOT, l1c_tile_id, l2a_year,
                                                        l2a_month, l2a_day, self.l2a_name])

        #### Setting up output path section
        product_title="_".join(["CLMS","WSI","CC","020m",f"T{self.tile_id}",
                               f"{self.product_measurement_date}",
                               f"{l1c_mission_id}", f"{self.PRODUCT_VERSION}"])
        output_dst_path = "/".join([self.S3_CC_ROOT,self.tile_id, year, month, day, product_title, ''])
        config_template["output"]["src"] = "/".join([self.WORK_DIR,"output", "CC", product_title, ""])
        config_template["output"]["dst"] = output_dst_path

        #### Setting up KPI path section
        config_template["qas"]["src"] = "/".join([self.WORK_DIR, "temp", product_title + "_temp", product_title+"_QAS.yaml"])
        config_template["qas"]["dst"] = "/".join([self.S3_QAS_ROOT,self.tile_id, year, month, day,product_title+"_QAS.yaml"])

        #### Setting up intermediate path section
        intermediate_dst_path = "/".join([self.S3_L2A_ROOT,self.tile_id, year, month, day, ''])

        config_template["intermediates"]["L2A"]["src"] = "/".join([self.WORK_DIR,"output", "L2A", ""])
        config_template["intermediates"]["L2A"]["dst"] = intermediate_dst_path

        #### Setting up log path section
        logs_out_path="/".join([self.S3_LOGS_ROOT,"CC",self.tile_id,year,month,day,f"{datetime.now().strftime('%Y%m%dT%H%M%S')}_{product_title}.stdout.log"])
        logs_err_path="/".join([self.S3_LOGS_ROOT,"CC",self.tile_id,year,month,day,f"{datetime.now().strftime('%Y%m%dT%H%M%S')}_{product_title}.stderr.log"])

        config_template["log"]["STDOUT"]["src"] = "/".join([self.WORK_DIR,"logs","processing_routine.stdout.log"])
        config_template["log"]["STDOUT"]["dst"] = logs_out_path

        config_template["log"]["STDERR"]["src"] = "/".join([self.WORK_DIR,"logs","processing_routine.stderr.log"])
        config_template["log"]["STDERR"]["dst"] = logs_err_path

        #### Setting up run mode
        config_template["run_mode"] = self.run_mode

        #### Writting the configuration file to /opt/wsi/config/configuration_file.yml
        with open("/".join(["","tmp","configuration_file.yml"]),
                  "w+", encoding="UTF-8") as config_stream:
            yaml.safe_dump(config_template, config_stream, encoding="UTF-8")
        return False

if __name__ == "__main__": # pragma no cover

    from argparse import ArgumentParser
    parser = ArgumentParser(description="Create the configuration file to launch the FSC processing routine"\
                          "on a L2A from the HRWSI S3 bucket or the HRWSI Google Drive")
    parser.add_argument("--run-mode", type=str, action="store", dest="run_mode", required=True, help="MAJA run mode. Must be either 'L2INIT' or 'L2NOMINAL'.")
    parser.add_argument("--tile-id", type=str, action="store", dest="tile_id", required=True, help="Tile ID to be processed. 32TMS for example.")
    parser.add_argument("--l1c-name", type=str, action="store", dest="l1c_name", required=True, help="L1C name. S2B_MSI1C_20200905T124309_N0500_R095_T28WET_20230328T093834.SAFE for example.")
    parser.add_argument("--measurement-date", type=str, action="store", dest="measurement_date", required=True, help="L1C measurement date."\
                        "2020-12-15 for example.")
    parser.add_argument("--product-measurement-date", type=str, action="store", dest="product_measurement_date", required=True, help="L1C measurement date.")
    parser.add_argument("--l2a-name", type=str, action="store", dest="l2a_name", required=False, default=None, help="L2A name. SENTINEL2B_20201215-103755-817_L2A_T32TMS_C_V1-0 for example.")

    args = parser.parse_args()

    cfg = CCConfigFileGenerator(maja_run_mode=args.run_mode,
                                 sentinel2_tile_id=args.tile_id,
                                 input_l1c_name=args.l1c_name,
                                 product_measurement_date=args.product_measurement_date,
                                 input_l2a_name=args.l2a_name,
                                 l1c_measurement_date=args.measurement_date)
    cfg.generate()
