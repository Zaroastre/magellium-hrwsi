from abc import ABC, abstractmethod

from magellium.hrwsi.system.core.products_types import ProductType
from magellium.hrwsi.system.launchers.configuration_file_generators.cc_config_file_generator import CCConfigFileGenerator
from magellium.hrwsi.system.launchers.configuration_file_generators.fsc_config_file_generator import FSCConfigFileGenerator
from magellium.hrwsi.system.launchers.configuration_file_generators.gfsc_config_file_generator import GFSCConfigFileGenerator
from magellium.hrwsi.system.launchers.configuration_file_generators.sig0_config_file_generator import Sig0ConfigFileGenerator
from magellium.hrwsi.system.launchers.configuration_file_generators.sws_config_file_generator import SWSConfigFileGenerator
from magellium.hrwsi.system.launchers.configuration_file_generators.wds_config_file_generator import WDSConfigFileGenerator
from magellium.hrwsi.system.launchers.configuration_file_generators.wics1_config_file_generator import WICS1ConfigFileGenerator
from magellium.hrwsi.system.launchers.configuration_file_generators.wics2_config_file_generator import WICS2ConfigFileGenerator
from magellium.hrwsi.system.launchers.configuration_file_generators.wics1s2_config_file_generator import WICS1S2ConfigFileGenerator

class Factory(ABC):
    @staticmethod
    @abstractmethod
    def create(*args, **kwargs) -> "Factory":
        raise NotImplementedError()

class CCConfigurationFileGeneratorFactory(Factory):
    @staticmethod
    def create(
            maja_run_mode,
            sentinel2_tile_id,
            input_l1c_name,
            product_measurement_date,
            input_l2a_name,
            l1c_measurement_date) -> CCConfigFileGenerator:
        return CCConfigFileGenerator(
            maja_run_mode=maja_run_mode,
            sentinel2_tile_id=sentinel2_tile_id,
            input_l1c_name=input_l1c_name,
            product_measurement_date=product_measurement_date,
            input_l2a_name=input_l2a_name,
            l1c_measurement_date=l1c_measurement_date
        )

class FSCConfigFileGeneratorFactory(Factory):
    @staticmethod
    def create(
            tile_id: str,
            measurement_date: str,
            l2a_name: str) -> FSCConfigFileGenerator:
        return FSCConfigFileGenerator(
            tile_id=tile_id,
            measurement_date=measurement_date,
            l2a_name=l2a_name
        )

class GFSCConfigFileGeneratorFactory(Factory):
    @staticmethod
    def create(
            tile_id:str,
            processing_date:str,
            sws_list:list[str],
            fsc_list:list[str],
            aggregation_timespan:str=None
    ) -> GFSCConfigFileGenerator:
        return GFSCConfigFileGenerator(
            tile_id=tile_id,
            processing_date=processing_date,
            sws_list=sws_list,
            fsc_list=fsc_list,
            aggregation_timespan=aggregation_timespan
        )

class Sig0ConfigFileGeneratorFactory(Factory):
    @staticmethod
    def create(
            tile_id:str,
            measurement_date:str,
            grd_list:list[str],
            relative_orbit:str) -> Sig0ConfigFileGenerator:
        return Sig0ConfigFileGenerator(
            tile_id=tile_id,
            measurement_date=measurement_date,
            grd_list=grd_list,
            relative_orbit=relative_orbit
        )

class SWSConfigFileGeneratorFactory(Factory):
    @staticmethod
    def create(
            tile_id:str,
            measurement_date:str,
            sigma0_name:str
    ) -> SWSConfigFileGenerator:
        return SWSConfigFileGenerator(
            tile_id=tile_id,
            measurement_date=measurement_date,
            sigma0_name=sigma0_name
        )

class WDSConfigFileGeneratorFactory(Factory):
    @staticmethod
    def create(
            tile_id: str,
            measurement_date: str,
            sigma0_name: str,
            fsc_list: list[str]
    ) -> WDSConfigFileGenerator:
        return WDSConfigFileGenerator(
            tile_id=tile_id,
            measurement_date=measurement_date,
            sigma0_name=sigma0_name,
            fsc_list=fsc_list
        )

class WICS1ConfigFileGeneratorFactory(Factory):
    @staticmethod
    def create(
            tile_id:str,
            measurement_date:str,
            sigma0_name:str
    ) -> WICS1ConfigFileGenerator:
        return WICS1ConfigFileGenerator(
            tile_id=tile_id,
            measurement_date=measurement_date,
            sigma0_name=sigma0_name
        )
    

class WICS2ConfigFileGeneratorFactory(Factory):
    @staticmethod
    def create(
            tile_id:str,
            measurement_date:str,
            l2a_name:str
    ) -> WICS2ConfigFileGenerator:
        return WICS2ConfigFileGenerator(
            tile_id=tile_id,
            measurement_date=measurement_date,
            l2a_name=l2a_name
        )
    

class WICS1S2ConfigFileGeneratorFactory(Factory):
    @staticmethod
    def create(
        tile_id:str,
                 measurement_date:str,
                 wic_s1_list:list[str],
                 wic_s2_list:list[str],
                 hour:str
    ) -> WICS1S2ConfigFileGenerator:
        return WICS1S2ConfigFileGenerator(
            tile_id=tile_id,
            measurement_date=measurement_date,
            wic_s1_list=wic_s1_list,
            wic_s2_list=wic_s2_list,
            hour=hour
        )


class ConfigurationFileGeneratorFactory:
    """Factory class to create the correct configuration file generator depending on the product to be generated.
    """

    @staticmethod
    def create_factory_for_product_type(product_type: ProductType|str):
        """Returns the correct configuration file generator depending on the product to be generated.

        Args:
            product_type (str): Type of product to be generated.
            **kwargs: Keyword arguments to be passed to the configuration file generator.

        Returns:
            AbstractConfigurationFileGenerator: Configuration file generator.

        Raises:
            ValueError: If the product type is not supported.
        """
        if product_type == ProductType.S2_CC_L2B:
            return CCConfigurationFileGeneratorFactory()
        elif product_type == ProductType.S2_FSC_L2B:
            return FSCConfigFileGeneratorFactory()
        elif product_type == ProductType.GFSC_L2C:
            return GFSCConfigFileGeneratorFactory()
        elif product_type == "SIG0":
            return Sig0ConfigFileGeneratorFactory()
        elif product_type == ProductType.S1_SWS_L2B:
            return SWSConfigFileGeneratorFactory()
        elif product_type == ProductType.S1_WDS_L2B:
            return WDSConfigFileGeneratorFactory()
        elif product_type == ProductType.S1_WICS1_L2B:
            return WICS1ConfigFileGeneratorFactory()
        elif product_type == ProductType.S2_WICS2_L2B:
            return WICS2ConfigFileGeneratorFactory()
        elif product_type == ProductType.COMB_WICS1S2:
            return WICS1S2ConfigFileGeneratorFactory()
        else:
            raise ValueError(f"Product type {product_type} is not supported.")
