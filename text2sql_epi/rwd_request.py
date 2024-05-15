# coding=utf-8
__author__ = "Angelo Ziletti"
__maintainer__ = "Angelo Ziletti"
__email__ = "angelo.ziletti@bayer.com"
__date__ = "24/11/23"

import asyncio
import logging
import re

import pandas as pd
from sqlalchemy import exc as sa_exc, text
from sqlalchemy.exc import SQLAlchemyError

logger = logging.getLogger(__name__)


class RWDRequest:
    def __init__(
        self,
        question,
        query_filled=None,
        query_template=None,
        retrieved_data=None,
        answer=None,
    ):
        self.question = question
        self.query_filled = query_filled
        self.query_template = query_template
        self.retrieved_data = retrieved_data
        self.answer = answer
        self.sql_executed = False
        self.sql_executed_self_healing_attempts = 0
        self.rag = None
        self.query_df_retrieved_rag = None
        self.prompt = None
        self.rag_top_similarity = 0.0
        self.question_masked = None

    async def get_answer(self, assistant, max_lines=100):
        if self.retrieved_data is not None:
            prompt = f"""
                This is the data retrieved from our database: {self.retrieved_data[:max_lines].to_markdown()} which is the sufficient to answer the question "{self.question}".\n
                - Please provide a concise answer to the following question: {self.question}
                - Assume all provided data is relevant and necessary for the response.
                - If the question refers to a distribution, please only include summary statistics in your answer.
                - Please refrain from offering data comparisons, conducting trend analysis, or attempting to create plots or visualizations in your response.
                - Please specify if the response contains an approximation rather than the precise result.
                - Please include all relevant data in your answer.
                """
            answer = await assistant.get_response(prompt)
            logger.info(f"Getting answer for '{self.question}'...")
            self.answer = answer

    async def run_query(
        self, sql_query, db, assistant, max_retries=5, reset_conversation=True
    ):
        if reset_conversation:
            assistant.reset_conversation()

        if sql_query is None:
            logger.info("Error in post processing SQL query")
            return None

        loop = asyncio.get_running_loop()

        for attempt in range(max_retries):
            try:
                # Run the blocking db.execute call in a separate thread
                results = await loop.run_in_executor(
                    None, lambda: db.execute(text(sql_query)).fetchall()
                )
                df = pd.DataFrame(results)
                self.sql_executed = True
                self.sql_executed_self_healing_attempts = attempt
                self.retrieved_data = df
                return df
            except (SQLAlchemyError, sa_exc.ProgrammingError) as db_ex:
                logger.error("Error in SQL detected")
                logger.error("sql query that failed:")
                logger.warning(sql_query)
                logger.info(
                    f"Self-healing process in progress. Attempt: {attempt}/{max_retries}"
                )
                if assistant is not None:
                    sql_query = await self.handle_invalid_sql(
                        sql_query, assistant, db_ex.args[0]
                    )
                else:
                    logger.warning(
                        "gpt assistant required for self-healing process. Continuing without."
                    )
            except Exception as e:
                logger.exception("Error in SQL could not be resolved")
                logger.info(e)
                logger.info(f"post_processed_sql: {sql_query}")
                return None

        logger.info("Max retries reached without successful SQL execution")
        return None

    async def handle_invalid_sql(self, sql_text, assistant, error):
        prompt = f"""Generated SQL query: \n {sql_text} \n 
                Error returned: {error}.\n 
                **IMPORTANT**: Analyze the error. Review the generated SQL. Think about the OMOP CDM schema. Fix and rewrite the SQL query.\n
                **IMPORTANT**: Please only return the corrected SQL query. Do not return any extraneous data or information.\n
                **IMPORTANT:** Return the SQL query ONLY within ```sql ``` code block.
                **IMPORTANT**: Do not replace or remove the provided concept id's, especially within the WHERE clause like in: `condition_concept_id IN (...some numbers)`. Preserve these as they are.\n
                **IMPORTANT**: Never assign concept_id's with the equal sign (=), always use `IN` when working with concept_id's. This contributes to code readability and SQL best practices.
            """
        completed_prompt = await assistant.get_response(prompt)
        logger.info(completed_prompt)
        # logger.info("Trying again")
        new_query = self.parse_sql_from_response(completed_prompt)
        logger.info("new_query")
        logger.info(new_query)
        logger.info("-------------")
        return new_query

    @staticmethod
    def parse_sql_from_response(resp=""):
        pattern1 = r"(?:Snowflake )?SQL query:\s*\n\n([\s\S]+?);"
        pattern2 = r"(?:```sql|```) ?\n([\s\S]+?)\n```"
        # pattern3 = r"(WITH\s[\s\S]+?LIMIT \d+;)"
        match1 = re.search(pattern1, resp)
        match2 = re.search(pattern2, resp)
        # match3 = re.search(pattern3, resp)
        # if match3:
        #     return match3.group(1)
        if match2:
            return match2.group(1)
        elif match1:
            return match1.group(1) + ";"
        else:
            logger.info("No SQL code found.")

    @classmethod
    def from_dict(cls, data):
        obj = cls(
            question=data["question"],
            query_filled=data["query_filled"],
            query_template=data["query_template"],
            retrieved_data=(
                pd.read_json(data["retrieved_data"])
                if data["retrieved_data"] is not None
                else None
            ),
            answer=data["answer"],
        )
        obj.sql_executed = data["sql_executed"]
        obj.sql_executed_self_healing_attempts = data[
            "sql_executed_self_healing_attempts"
        ]
        obj.rag = data["rag"]
        obj.query_df_retrieved_rag = data["query_df_retrieved_rag"]
        obj.prompt = data["prompt"]
        obj.rag_top_similarity = data["rag_top_similarity"]
        obj.question_masked = data["question_masked"]
        return obj

    def to_dict(self):
        return {
            "question": self.question,
            "query_filled": self.query_filled,
            "query_template": self.query_template,
            "retrieved_data": (
                self.retrieved_data.to_json(orient="records")
                if self.retrieved_data is not None
                else None
            ),
            "answer": self.answer,
            "sql_executed": self.sql_executed,
            "sql_executed_self_healing_attempts": self.sql_executed_self_healing_attempts,
            "rag": self.rag,
            "query_df_retrieved_rag": (
                self.query_df_retrieved_rag.to_json(orient="records")
                if self.query_df_retrieved_rag is not None
                else None
            ),
            "prompt": self.prompt,
            "rag_top_similarity": self.rag_top_similarity,
            "question_masked": self.question_masked,
        }
