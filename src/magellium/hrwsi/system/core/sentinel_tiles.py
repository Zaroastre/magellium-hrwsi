from dataclasses import dataclass
from datetime import datetime as DateTime

from magellium.hrwsi.system.core.products_types import ProductType


@dataclass
class SentinelTile:
    id: str
    product_type: ProductType
    start_date: DateTime
    publishing_date: DateTime
    tile: str
    measurement_day: int
    input_path: str
    is_partial: bool
    relative_orbit_number: int
    harvesting_date: DateTime

    @staticmethod
    def map_from_dict_record(record: dict) -> "SentinelTile":
        return SentinelTile(
            id=record.get('id'),
            product_type=record.get('product_type_code'),
            start_date=record.get('start_date'),
            publishing_date=record.get('publishing_date'),
            tile=record.get('tile'),
            measurement_day=record.get('measurement_day'),
            input_path=record.get('input_path'),
            is_partial=record.get('is_partial'),
            relative_orbit_number=record.get('relative_orbit_number'),
            harvesting_date=record.get('harvesting_date')
        )