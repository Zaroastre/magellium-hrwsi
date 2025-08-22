#!/usr/bin/env python3
"""This script is to  be used at integration stage to generate configuration YAML files at will."""
import logging
from datetime import datetime
from typing import List

import yaml

from magellium.hrwsi.system.launchers.configuration_file_generators.configuration_file_generator import AbstractConfigurationFileGenerator
from magellium.hrwsi.utils.logger import LogUtil


class GFSCConfigFileGenerator(AbstractConfigurationFileGenerator):
    """Main class of the YAML config file generator for the GFSC processing routine.
    """

    S3_GFSC_ROOT="s3://HRWSI/GFSC"
    S3_SWS_ROOT="s3://HRWSI/SWS"
    S3_FSC_ROOT="s3://HRWSI/FSC"
    S3_LOGS_ROOT="s3://HRWSI-LOGS"
    PRODUCT_VERSION="V102"
    S3_QAS_ROOT="s3://HRWSI-KPI-FILES/GFSC"

    def __init__(self,
                 tile_id:str,
                 processing_date:str,
                 sws_list:List[str],
                 fsc_list:List[str],
                 aggregation_timespan:str=None) -> None:
        """Initialization function."""
        self.logger = LogUtil.get_logger("ConfigFileGenerator", logging.INFO)
        super().__init__(tile_id)
        self.tile_id = tile_id
        self.sws_list = sws_list
        self.fsc_list = fsc_list
        self.processing_date = processing_date
        self.aggregation_timespan=aggregation_timespan
        if not self.aggregation_timespan: # pragma no cover
            self.aggregation_timespan = "7"

    def _build_yaml_conf(self)->None:
        """Fills the YAML template located at config/configuration_file.yml with values
        depending on the calss attributes for the SWS processing routine execution.
        """
        config_file_template_path = "/".join(["HRWSI_System","launcher","config_file_generation","configuration_file_template.yml"])
        with open(config_file_template_path,
                  "r", encoding="UTF-8") as config_template_stream:
                config_template = yaml.safe_load(config_template_stream)

        #### Setting up measurement date section
        ## Checking input data
        try:
            date = datetime.strptime(self.processing_date,"%Y-%m-%d")
            assert datetime.now() > date > datetime(2016,9,1)

        ## Storing data into template
        except ValueError as error:
            self.logger.critical( f"Wrong format for processing date, should be YYYY-mm-dd, got {self.processing_date}.")
            raise error
        except AssertionError as error:
            self.logger.critical(f"Measurement date must be contained between {datetime(2016,9,1)} and {datetime.now()}, got {date}")
            raise error
        config_template["date"] = datetime.strftime(date,"%Y-%m-%d")

        ##### Setting up INPUT path section
        ## Checking input data

        ## Storing data into template
        config_template["tile"] = self.tile_id
        #TODO this is to be adapted for the launcher to use as this strange looking contraption comes from bash behaviour.
        config_template["input"]["SWS"] = [sws_name for sws_name in self.sws_list]
        config_template["input"]["FSC"] = [fsc_name for fsc_name in self.fsc_list]
        config_template["aggregation_timespan"] = self.aggregation_timespan
        
        #### Setting up auxiliaries section
        water_mask_name = "".join(["WL_2018_60m_",self.tile_id,".tif"])
        config_template["auxiliaries"]["WATER_MASK"] = "/".join([self.S3_AUX_ROOT,"WL","60m",water_mask_name])

        #### Setting up output path section

        product_title = "_".join(["CLMS","WSI","GFSC","060m",f"T{self.tile_id}",
                                 f"{datetime.strftime(date,'%Y%m%d')}P{self.aggregation_timespan}D",
                                 "COMB",
                                 self.PRODUCT_VERSION])
        year, month, day = self.processing_date.split("-")
        output_dst_path = "/".join([self.S3_GFSC_ROOT,self.tile_id, year, month, day, product_title, ''])

        config_template["output"]["src"] = "/".join(["","opt","wsi","output", product_title, ""])
        config_template["output"]["dst"] = output_dst_path

        #### Setting up log path section
        logs_out_path="/".join([self.S3_LOGS_ROOT, "GFSC",self.tile_id,year,month,day,f"{datetime.now().strftime('%Y%m%dT%H%M%S')}_{product_title}.stdout.log"])
        logs_err_path="/".join([self.S3_LOGS_ROOT, "GFSC",self.tile_id,year,month,day,f"{datetime.now().strftime('%Y%m%dT%H%M%S')}_{product_title}.stderr.log"])

        config_template["log"]["STDOUT"]["src"] = "/".join(["","opt","wsi","logs","processing_routine.stdout.log"])
        config_template["log"]["STDOUT"]["dst"] = logs_out_path

        config_template["log"]["STDERR"]["src"] = "/".join(["","opt","wsi","logs","processing_routine.stderr.log"])
        config_template["log"]["STDERR"]["dst"] = logs_err_path

        #### Setting up KPI path section
        config_template["qas"]["src"] = "/".join(["","opt","wsi","output",product_title+"_QAS.yaml"])
        config_template["qas"]["dst"] = "/".join([self.S3_QAS_ROOT,self.tile_id, year, month, day,product_title+"_QAS.yaml"])

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
    parser.add_argument("--sws-name-list", type=str, action="store", dest="sws_name_list", required=True, help="SWS files name..")
    parser.add_argument("--fsc-name-list", type=str, action="store", dest="fsc_name_list", required=True, help="FSC files name.")
    parser.add_argument("--processing-date", type=str, action="store", dest="processing_date", required=True, help="Processing date."\
                      "2020-12-15 for example.")
    parser.add_argument("--aggregation-timespan", type=str, action="store", dest="aggregation_timespan", required=True, help="Number of days to be taken into consideration for the aggregation computation.")

    args = parser.parse_args()
    if len(args.fsc_name_list.split(" ")) == 1:
        args.fsc_name_list = [args.fsc_name_list]
    cfg = GFSCConfigFileGenerator(tile_id=args.tile_id,
                                   sws_list=args.sws_name_list,
                                   fsc_list=args.fsc_name_list,
                                   processing_date=args.processing_date,
                                   aggregation_timespan=args.aggregation_timespan)
    cfg.generate()
