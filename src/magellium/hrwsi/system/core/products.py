from dataclasses import dataclass
from datetime import datetime as DateTime
from pathlib import Path

from magellium.hrwsi.system.core.trigger_validations import TriggerValidation
from magellium.hrwsi.system.core.products_types import ProductType


@dataclass(frozen=True)
class Product:
    id: str
    trigger_validation: TriggerValidation
    product_type_code: ProductType
    product_path: Path
    creation_date: DateTime
    catalogue_date: DateTime
    kpi_file: Path

    @staticmethod
    def from_dict_record(record: dict) -> "Product":
        return Product(
            id=record[0],
            trigger_validation=TriggerValidation(id=record[1], triggering_condition=None, validation_date=None, run_mode=None, artificial_measurement_day=None), # Placeholder, needs full TriggerValidation object
            product_type_code=ProductType(record[2]),
            product_path=Path(record[3]),
            creation_date=record[4],
            catalogue_date=record[5],
            kpi_file=Path(record[6]) if record[6] else None
        )
    