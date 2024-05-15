import sys
from dotenv import load_dotenv
import os
import argparse

if __name__ == "__main__":
    main_path = os.path.join(os.path.dirname(os.getcwd()))
    src_folder = os.path.join(main_path, "text2sql_epi")
    sys.path.append(main_path)
    sys.path.append(src_folder)

    from text2sql_epi.query_library import QueryLibrary

    # load environment variables
    load_dotenv("../.env.local")

    in_folder = os.path.join(main_path, "dataset")
    out_folder = os.path.join(main_path, "data_out")

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input_path",
        default=in_folder,
        help="path where the data is stored",
        type=str,
    )
    parser.add_argument(
        "--output_path",
        default=out_folder,
        help="path where the query lib will be generated",
        type=str,
    )
    args = parser.parse_args()

    querylib_source_file = os.path.join(in_folder, "text2sql_epi_dataset_omop.xlsx")
    querylib_file = os.path.join(out_folder, "querylib.pkl")

    querylib = QueryLibrary(
        querylib_name="patient_counts",
        source="text2sql_epi_dataset_omop",
        querylib_source_file=querylib_source_file,
        col_question="QUESTION",
        col_question_masked="QUESTION_MASKED",
        col_query_w_placeholders="QUERY_SNOWFLAKE_WITH_PLACEHOLDERS",
        col_query_executable="QUERY_SNOWFLAKE_RUNNABLE",
    )

    print(f"Calculating query library from {querylib_source_file}...")
    querylib.calc_embedding(embedding_model_name="BAAI/bge-large-en-v1.5")
    querylib.save(querylib_file=querylib_file)
    print(f"Embedding calculated and saved to {querylib_file}")

    print(f"Loading embedding from {querylib_file}")
    querylib = querylib.load(querylib_file=querylib_file)
    print(f"Query library length: {len(querylib)}")
    print("Done")
