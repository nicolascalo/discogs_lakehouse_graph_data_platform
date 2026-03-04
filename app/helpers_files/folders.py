import os 
from pathlib import Path

def get_output_dir(dump_type: str, logger, data_folder) -> str:
    output_dir = os.path.join(data_folder, dump_type)
    logger.info(f"{output_dir = }")

    output_dir_path = Path(output_dir)
    output_dir_path.mkdir(parents=True, exist_ok=True)
    return output_dir

