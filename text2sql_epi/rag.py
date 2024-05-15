import glob
import os
import sys
from datetime import datetime
import logging

from text2sql_epi import prompts
from text2sql_epi.assistants import create_assistant
from text2sql_epi.query_library import QueryLibrary

logger = logging.getLogger(__name__)


class Rag:
    querylib = None

    def __init__(self, main_path=None, log_folder=None, querylib_file=None):
        # Set default main_path if not provided
        self.main_path = main_path if main_path is not None else os.getcwd()
        sys.path.append(self.main_path)

        # Set default log_folder if not provided
        self.log_folder = (
            log_folder
            if log_folder is not None
            else os.path.join(self.main_path, "logs")
        )

        if querylib_file is not None:
            # Use the provided querylib_file
            self.querylib_file = querylib_file
        else:
            # Get a list of all files that match the pattern 'querylib_*.pkl'
            querylib_files = glob.glob(os.path.join(self.main_path, "querylib_*.pkl"))
            # Extract dates from the filenames and sort them
            querylib_files.sort(
                key=lambda filename: datetime.strptime(
                    filename.split("_")[-1].split(".")[0], "%Y%m%d"
                ),
                reverse=True,
            )
            # Pick the most recent file if any are found
            self.querylib_file = querylib_files[0] if querylib_files else None

        self.sleep_sec = 2
        self.assistant_type = "gpt4turbo"
        self.top_k_prompt = 2
        self.top_k_screening = 10
        self.sim_threshold = 0.0

        if Rag.querylib is None:
            Rag.querylib = self.load_querylib()

    def load_querylib(self):
        # Assuming QueryLibrary is a class defined elsewhere
        querylib = QueryLibrary(
            querylib_name="patient_counts",
            source="gold_label_dec_2023",
            querylib_source_file=None,
            col_question="QUESTION",
            col_question_masked="QUESTION_MASKED",
            col_query_w_placeholders="QUERY_SNOWFLAKE_WITH_PLACEHOLDERS",
            col_query_executable="QUERY_SNOWFLAKE_RUNNABLE",
        )

        querylib = querylib.load(querylib_file=self.querylib_file)

        # Assuming an embedding model and logger are defined elsewhere
        querylib.load_embedding_model(embedding_model_name="BAAI/bge-large-en-v1.5")

        logging.info(f"Embedding loaded from {self.querylib_file}")

        return querylib


class AgentRag(Rag):
    def __init__(self, **kwargs):
        super().__init__(
            main_path=kwargs.get("main_path"),
            log_folder=kwargs.get("log_folder"),
            querylib_file=kwargs.get("querylib_file"),
        )
        # Override specific properties for AgentRag
        self.sleep_sec = 2
        self.top_k_prompt = 2
        self.top_k_screening = 10
        self.sim_threshold = 0.0

        # Override the prompt method
        self.prompt = prompts.prompt_gpt

        # Create new instances for the assistant and med_sql_processor
        self.database = kwargs.get("database")
        self.assistant = kwargs.get(
            "assistant", create_assistant(assistant_type=self.assistant_type)
        )
        self.assistant_answers = kwargs.get(
            "assistant_answers", create_assistant(assistant_type=self.assistant_type)
        )
