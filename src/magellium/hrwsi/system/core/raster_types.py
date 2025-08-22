from dataclasses import dataclass


@dataclass(frozen=True)
class RasterType:
    product_type: str
    description: str
    processing_level: int