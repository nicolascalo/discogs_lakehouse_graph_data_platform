import pandas as pd
import shutil
import os
import glob


def get_or_create_last_log(path:str) -> pd.DataFrame:
        try:

            last_merge_log = pd.read_csv(path)
            print(f"log loaded from {path}")
        except:
            last_merge_log = pd.DataFrame(
                columns=["timestamp", "input_path", "output_path", "dump","duration_min"]
            )
            print(f"new log created")
            
        return last_merge_log

def get_last_dump_date(last_merge_log:pd.DataFrame, output_path:str) -> int :

        try: 
            df = last_merge_log[last_merge_log['output_path'] == output_path]
            last_processed_dump = int(df['dump'].max())
            print(last_processed_dump)

        except:
             last_processed_dump = 0
             

            

        print(f"{last_processed_dump = }")

        return last_processed_dump


def output_single_json(df, output_path):

    (
        df.coalesce(1)
        .write.mode("overwrite")
        .json(f"{output_path}.json_folder")
    )

    json_folder = f"{output_path}.json_folder"
    output_file = f"{output_path}.json"

    # Spark wrote part-00000-xxxx.json inside the folder
    part_file = glob.glob(os.path.join(json_folder, "part-*.json"))[0]

    # Move/rename
    shutil.move(part_file, output_file)

    # Remove the temporary folder
    shutil.rmtree(json_folder)

    return None

def output_schema_json(spark_df, output_path:str) -> None:
    schema = spark_df.schema
    with open(f"{output_path}_schema.json", "w") as f:
        f.write(schema.json())

def output_schema_txt(spark_df, output_path:str) -> None:
    schema = spark_df.schema
    with open(f"{output_path}_schema.txt", "w") as f:
        f.write(schema.simpleString())


def output_schema_csv(spark_df, output_path:str) -> None:
    fields = [(f.name, f.dataType.simpleString(), f.nullable) for f in spark_df.schema.fields]
    schema_df = pd.DataFrame(fields, columns=["column_name", "type", "nullable"])
    schema_df.to_csv(f"{output_path}_schema.csv", index=False)
