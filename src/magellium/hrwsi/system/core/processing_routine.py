from dataclasses import dataclass

from magellium.hrwsi.system.core.docker_images import DockerImage
from magellium.hrwsi.system.core.flavours import Flavour
from magellium.hrwsi.system.core.processing_levels import ProcessingLevel


@dataclass(frozen=True)
class ProcessingRoutine:
    name: str
    product_type_code: str
    cpu: int
    ram: int
    storage_space: int
    duration: int
    docker_image: DockerImage
    flavour: Flavour

    