from dataclasses import dataclass

@dataclass(frozen=True)
class DockerImage:
    registry_host: str
    registry_port: str
    image_name: str
    tag: str

    def __str__(self) -> str:
        return f'{self.registry_host}:{self.registry_port}/{self.image_name}:{self.tag}'
    
    @staticmethod
    def from_dict_record(record: dict) -> "DockerImage":
        return DockerImage()