import sys
import asyncio
from dotenv import load_dotenv
import argparse
import os
from os.path import join, dirname
import time

main_path = dirname(os.getcwd())
src_folder = os.path.join(main_path, "text2sql_epi")

sys.path.append(main_path)
sys.path.append(src_folder)

dotenv_path = join(main_path, ".env.local")
load_dotenv(dotenv_path)

from text2sql_epi.settings import settings
from text2sql_epi.rag import AgentRag
from text2sql_epi.sql_post_processor import MedicalSQLProcessor
from text2sql_epi import helpers
from text2sql_epi.snowflake_session import get_db


async def end2end_pred_pipeline_ds(
    input_question, main_path_rag, querylib_file_rag, log_folder, med_coding=False, use_db=False,
        medcodeonto_file=None
):

    print(f"Use medical coding: {med_coding}")

    print(f"Use Snowflake database: {use_db}")

    selected_coding = {
        "condition": ["SNOMED"],
        "procedure": ["CPT4", "SNOMED"],
        "drug": ["RxNorm", "RxNorm Extension"],
    }

    time.sleep(1)

    rag_agent = AgentRag(
        main_path=main_path_rag, log_folder=log_folder, querylib_file=querylib_file_rag
    )

    med_sql_processor = MedicalSQLProcessor(assistant=rag_agent.assistant)

    initial_prompt, text_sql_template, df_recs_list_out, question_masked = (
        await helpers.prepare_gpt_call(input_question, rag_agent)
    )
    gpt_answer = await rag_agent.assistant.get_response()
    df_recs_list_out = df_recs_list_out.astype({"DATE_LABELLED": str})

    query_template_pred = med_sql_processor.parse_sql_from_response(gpt_answer)

    print(f"Question: {input_question}\n")
    print(f"SQL template:\n {query_template_pred}\n")

    if med_coding:
        from text2sql_epi.query_library import MedCodingOnto
        medcodeonto = MedCodingOnto(
            ontolib_name="medcodes_mockup",
            source="medcodes_mockup",
            ontolib_source_file=None,
            col_text="CONCEPT_NAME"
        )

        print(f"Loading embedding from {medcodeonto_file}")
        medcodeonto = medcodeonto.load(querylib_file=medcodeonto_file)

        query_filled_pred = await med_sql_processor.post_process_sql_query(
            query_template_pred,
            sleep_sec=rag_agent.sleep_sec,
            explorer_concepts=None,
            selected_coding=selected_coding,
            rag=rag_agent,
            medcodeonto=medcodeonto
        )

        print(f"SQL filled:\n {query_filled_pred}\n")
    else:
        query_filled_pred = None

    if use_db and query_filled_pred is not None:
        db = next(get_db(settings.SNOWFLAKE_DATABASE))

        new_prompt = rag_agent.assistant.conversation

        rwd_request_pred = helpers.prepare_rwd_request(
            input_question,
            query_filled_pred,
            query_template_pred,
            question_masked,
            df_recs_list_out,
            new_prompt,
        )

        df = await rwd_request_pred.run_query(
            query_filled_pred,
            db=db,
            assistant=rag_agent.assistant,
            max_retries=5,
            reset_conversation=False,
        )

        await rwd_request_pred.get_answer(rag_agent.assistant_answers)
        answer = rwd_request_pred.answer

        print(f"Database: {settings.SNOWFLAKE_DATABASE}\n")
        print(f"Answer: {answer}\n")


if __name__ == "__main__":

    # in_folder = os.path.join(main_path, "dataset")
    out_folder = os.path.join(main_path, "data_out")

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--output_path",
        default=out_folder,
        help="path where the query lib will be generated",
        type=str,
    )
    parser.add_argument(
        "--med_coding",
        default=False,
        help="Retrieve medical codes for the medical entities in the generated query"
    )
    parser.add_argument(
        "--use_db",
        default=False,
        help="Use Snowflake database for querying. Only works if a Snowflake database is connected."
    )

    parser.add_argument(
        "--question",
        help="Add here your question",
        type=str
    )

    args = parser.parse_args()

    querylib_file = os.path.join(out_folder, "querylib.pkl")
    medcodeonto_file_loaded = os.path.join(out_folder, "medcodes_onto.pkl")

    asyncio.run(
        end2end_pred_pipeline_ds(
            input_question=args.question,
            use_db=args.use_db,
            main_path_rag=main_path,
            log_folder=out_folder,
            med_coding=args.med_coding,
            querylib_file_rag=os.path.join(out_folder, "querylib.pkl"),
            medcodeonto_file=medcodeonto_file_loaded,
        )
    )
