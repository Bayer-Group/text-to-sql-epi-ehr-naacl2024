import sys
from dotenv import load_dotenv
import os
import argparse
import asyncio

if __name__ == "__main__":
    main_path = os.path.join(os.path.dirname(os.getcwd()))
    src_folder = os.path.join(main_path, "text2sql_epi")
    sys.path.append(main_path)
    sys.path.append(src_folder)

    from text2sql_epi.query_library import MedCodingOnto

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

    medcodeonto_source_file = os.path.join(in_folder, "medcodes_mockup.xlsx")
    medcodeonto_file = os.path.join(out_folder, "medcodes_onto.pkl")

    medcodeonto = MedCodingOnto(
        ontolib_name="medcodes_mockup",
        source="medcodes_mockup",
        ontolib_source_file=medcodeonto_source_file,
        col_text="CONCEPT_NAME"
    )

    print(f"Calculating ontology embeddings from {medcodeonto_source_file}...")
    medcodeonto.calc_embedding(embedding_model_name="BAAI/bge-large-en-v1.5")
    medcodeonto.save(querylib_file=medcodeonto_file)
    print(f"Embedding calculated and saved to {medcodeonto_file}")

    print(f"Loading embedding from {medcodeonto_file}")
    medcodeonto = medcodeonto.load(querylib_file=medcodeonto_file)
    print(f"Ontology length: {len(medcodeonto)}")
    print("Done")

    df = asyncio.run(medcodeonto.get_similar_codes_from_onto(question_masked="atopic dermatitis",
        top_k_screening=10,
        top_k_prompt=4,
        sim_threshold=0.0))

    print(df)