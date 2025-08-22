import geojson
from pathlib import Path
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from psycopg2.extras import execute_values
from HRWSI_System.apimanager.hrwsi_database_api_manager import HRWSIDatabaseApiManager
from typing import List, Tuple, Dict

# Path to GeoJSON file using pathlib
GEOJSON_PATH = Path("S2_tiles_proposal_GV.geojson")

def geojson_feature_to_wkt(feature: Dict) -> str:
    """Convert a GeoJSON feature to WKT format."""
    geom_type = feature['geometry']['type']
    coords = feature['geometry']['coordinates']

    if geom_type == 'Polygon':
        coordinates_str = ', '.join(f"{x} {y} {z}" for x, y, z in coords[0])
        return f"POLYGON Z(({coordinates_str}))"
    elif geom_type == 'MultiPolygon':
        polygons_wkt = []
        for polygon in coords:
            rings_wkt = []
            for ring in polygon:
                ring_str = ', '.join(f"{x} {y} {z}" for x, y, z in ring)
                rings_wkt.append(f"({ring_str})")
            polygons_wkt.append(f"({', '.join(rings_wkt)})")
        return f"MULTIPOLYGON Z({', '.join(polygons_wkt)})"
    else:
        raise ValueError(f"Unsupported geometry type: {geom_type}")


def load_geojson(file_path: Path) -> List[Tuple[str, str]]:
    """Load GeoJSON and convert features to a list of tuples (tile_name, wkt)."""
    if not file_path.exists():
        raise FileNotFoundError(f"GeoJSON file not found: {file_path}")

    with file_path.open("r") as f:
        data = geojson.load(f)

    insert_data = []
    for feature in data.get('features', []):
        name = feature['properties'].get('Name')
        wkt = geojson_feature_to_wkt(feature)
        insert_data.append((name, wkt))
    return insert_data


def insert_tiles_into_db(insert_data: List[Tuple[str, str]]):
    """Insert tiles into PostgreSQL using execute_values and ST_GeomFromText."""
    with HRWSIDatabaseApiManager.database_connection(True) as (conn, cur):
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

        insert_query = """
        INSERT INTO hrwsi.tile_geometry (tile, geom)
        VALUES %s
        ON CONFLICT (tile) DO NOTHING;
        """
        execute_values(
            cur, insert_query, insert_data,
            template="(%s, ST_GeomFromText(%s, 4326))"
        )
        print(f"{len(insert_data)} GeoJSON features inserted successfully.")


def main():
    try:
        tiles = load_geojson(GEOJSON_PATH)
        insert_tiles_into_db(tiles)
    except Exception as error:
        print(f"Error inserting GeoJSON data: {error}")
        raise


if __name__ == "__main__":
    main()
