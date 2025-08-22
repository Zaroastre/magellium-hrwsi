#!/usr/bin/env python3
"""This script is to be used at integration stage to generate configuration YAML files at will."""
import logging
from datetime import datetime
from typing import List

import yaml

from magellium.hrwsi.system.launchers.configuration_file_generators.configuration_file_generator import AbstractConfigurationFileGenerator
from magellium.hrwsi.utils.logger import LogUtil


class Sig0ConfigFileGenerator(AbstractConfigurationFileGenerator):
    """Main class of the YAML config file generator for the Backscatter at 10m processing routine.
    """

    S3_SIGMA0_ROOT="s3://HRWSI-INTERMEDIATE-RESULTS/Backscatter_10m"
    S3_EODATA_ROOT="s3://EODATA/Sentinel-1/SAR/IW_GRDH_1S"
    S3_EODATA_ROOT_OLDER="s3://EODATA/Sentinel-1/SAR/GRD"

    def __init__(self, tile_id:str,
                 measurement_date:str,
                 grd_list:List[str],
                 relative_orbit:str) -> None:
        """Initialization function."""
        self.logger = LogUtil.get_logger("ConfigFileGenerator", logging.INFO)
        super().__init__(tile_id)
        self.tile_id = tile_id
        self.measurement_date = measurement_date
        self.grd_list = grd_list
        self.relative_orbit = relative_orbit

    def _build_yaml_conf(self)->None:
        """Fills the YAML template located at config/configuration_file.yml with values
        depending on the calss attributes for the SWS processing routine execution.
        """

        with open("/".join(["HRWSI_System","launcher","config_file_generation","configuration_file_template.yml"]),
                  "r",encoding="UTF-8") as config_template_stream:
            config_template = yaml.safe_load(config_template_stream)
        #### Setting up measurement date section
        ## Checking input data
        try:
            date = datetime.strptime(self.measurement_date,"%Y-%m-%d")
            assert datetime.now() > date > datetime(2016,8,1)
            if date <= datetime(year=2023, month=2, day=21):
                s3_eodata_root = self.S3_EODATA_ROOT_OLDER
            else:
                s3_eodata_root = self.S3_EODATA_ROOT

        ## Storing data into template
        except ValueError as error:
            self.logger.critical( f"Wrong format for measurement date, should be YYYY-mm-dd, got {self.measurement_date}.")
            raise error
        except AssertionError as error:
            self.logger.critical(f"Measurement date must be contained between {datetime(2016,8,1)} and {datetime.now()}, got {date}")
            raise error
        config_template["date"] = datetime.strftime(date,"%Y%m%d")

        ##### Setting up INPUT path section
        ## Checking input data
        # S1A_IW_GRDH_1SDV_20211227T052658_20211227T052723_041190_04E503_C194.SAFE
        year, month, day = self.measurement_date.split("-")
        for index, grd_path in enumerate(self.grd_list):
            grd_name = grd_path.split("/")[-1]
            grd_mission_id = grd_name.split("_")[0]
            grd_measurement_date = grd_name.split("_")[4].split("T")[0]
            if index == 0:
                grd_start_measurement_time = grd_name.split("_")[4].split("T")[1]
                grd_end_measurement_time = grd_name.split("_")[5].split("T")[1]
            else:
                grd_end_measurement_time = grd_name.split("_")[5].split("T")[1]
            grd_acquisition_id = grd_name.split("_")[7]
            try:
                assert "".join([year, month, day]) == grd_measurement_date
                assert grd_mission_id in ["S1A", "S1B", "S1C"]
            except AssertionError as error:
                self.logger.critical(f"Wrong GRD name provided: {grd_path}")
                raise error

        ## Storing data into template
        config_template["input"]["GRD"] = [ "/".join([s3_eodata_root,year,month,day,grd_name]) for grd_name in self.grd_list]
        config_template["tile"] = self.tile_id
        
        #### Setting up auxiliaries section
        config_template["auxiliaries"]["DEM"] = '/'.join([self.S3_AUX_ROOT,"DEM","10m",f"Copernicus_DSM_04_N02_00_00_DEM_10m_{self.tile_id}_b60m_wgs84.tif"])
        config_template["auxiliaries"]["TILES_UTM"] = '/'.join([self.S3_AUX_ROOT,"TILES_UTM",'tiles_utm.yml'])

        #### Setting up output path section
        while len(str(self.relative_orbit)) <=2:
            self.relative_orbit=f"0{self.relative_orbit}"
        product_title = "_".join(["SIG0",
                                 "T".join([grd_measurement_date, grd_start_measurement_time]),
                                 "T".join([grd_measurement_date, grd_end_measurement_time]),
                                 f"{grd_acquisition_id}",
                                 f"{self.relative_orbit}",
                                 f"T{self.tile_id}",
                                 "10m",
                                 f"{grd_mission_id}IWGRDH_ENVEO.tif"])
        output_dst_path = "/".join([self.S3_SIGMA0_ROOT,self.tile_id, year, month, day, product_title])

        config_template["output"]["src"] = "/".join(["","opt","wsi","output", product_title])
        config_template["output"]["dst"] = output_dst_path

        #### Setting up log path section
        logs_out_path="/".join([self.S3_LOGS_ROOT,"Backscatter_10m",self.tile_id,year,month,day,f"{datetime.now().strftime('%Y%m%dT%H%M%S')}_{product_title}.stdout.log"])
        logs_err_path="/".join([self.S3_LOGS_ROOT,"Backscatter_10m",self.tile_id,year,month,day,f"{datetime.now().strftime('%Y%m%dT%H%M%S')}_{product_title}.stderr.log"])

        config_template["log"]["STDOUT"]["src"] = "/".join(["","opt","wsi","logs","processing_routine.stdout.log"])
        config_template["log"]["STDOUT"]["dst"] = logs_out_path

        config_template["log"]["STDERR"]["src"] = "/".join(["","opt","wsi","logs","processing_routine.stderr.log"])
        config_template["log"]["STDERR"]["dst"] = logs_err_path


        #### Writting the configuration file to /tmp/configuration_file.yml
        with open("/".join(["","tmp","configuration_file.yml"]),
                  "w+", encoding="UTF-8") as config_stream:
            yaml.safe_dump(config_template, config_stream, encoding="UTF-8")
        return

if __name__ == "__main__": # pragma no cover

    from argparse import ArgumentParser
    parser = ArgumentParser(description="Create the configuration file to launch the SWS processing routine"\
                          "on a Backscatter at 10m or sigma0 map (two names for one thing) from the HRWSI S3"\
                          "bucket.")
    parser.add_argument("--tile-id", type=str, action="store", dest="tile_id", required=True, help="Tile ID to be processed. 32TMS for example.")
    parser.add_argument("--grd-list", type=str, action="store", dest="grd_list", required=True, help="GRD names. [S1A_IW_GRDH_1SDV_20211227T052658_20211227T052723_041190_04E503_C194.SAFE] for example.")
    parser.add_argument("--relative-orbit", type=str, action="store", dest="relative_orbit", required=True, help="GRD relative orbit number.")
    parser.add_argument("--measurement-date", type=str, action="store", dest="measurement_date", required=True, help="L1C measurement date."\
                        "2020-12-15 for example.")

    args = parser.parse_args()

    cfg = Sig0ConfigFileGenerator(tile_id=args.tile_id,
                                   grd_list=args.grd_list.split(","),
                                   measurement_date=args.measurement_date,
                                   relative_orbit=args.relative_orbit)
    cfg.generate()
