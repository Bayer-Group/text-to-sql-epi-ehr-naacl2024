import logging
from typing import Optional

import pandas as pd

from text2sql_epi.rag import Rag
from text2sql_epi.rwd_request import RWDRequest

from text2sql_epi import prompts

logger = logging.getLogger(__name__)


def prepare_prediction(user_input: str, prompt: str):
    new_prompt = prompt.replace("${question}", user_input.replace("'", "\\'"))
    return new_prompt


async def get_text_sql_template_for_rag(
    question_masked: str,
    rag: Rag,
    rag_random: Optional[bool] = None,
    drop_first: Optional[bool] = None,
):
    params_dict = {
        "question_masked": question_masked,
        "top_k_screening": rag.top_k_screening,
        "top_k_prompt": rag.top_k_prompt,
        "sim_threshold": rag.sim_threshold,
    }

    if rag_random is not None:
        params_dict["rag_random"] = rag_random
    if drop_first is not None:
        params_dict["drop_first"] = drop_first

    (text_sql_template, df_recs_list_out) = (
        await rag.querylib.text_sql_template_for_rag(**params_dict)
    )
    return text_sql_template, df_recs_list_out


def prepare_rwd_request(
    question: str,
    query_filled_pred: str,
    query_template_pred: str,
    question_masked: str,
    df_recs_list_out: pd.DataFrame,
    new_prompt: str,
):
    rwd_request_pred = RWDRequest(
        question=question,
        query_filled=query_filled_pred,
        query_template=query_template_pred,
    )
    rwd_request_pred.rag = True
    rwd_request_pred.question_masked = question_masked
    rwd_request_pred.query_df_retrieved_rag = df_recs_list_out
    rwd_request_pred.rag_top_similarity = df_recs_list_out["Score"].max()
    rwd_request_pred.prompt = new_prompt
    return rwd_request_pred


async def prepare_gpt_call(user_input: str, rag_agent):
    question_masked, question = await rag_agent.querylib.get_masked_question(
        prompts=prompts, question=user_input, assistant=rag_agent.assistant
    )
    initial_prompt = prepare_prediction(
        question, prompt=prompts.prompt_gpt
    )
    text_sql_template, df_recs_list_out = (
        await get_text_sql_template_for_rag(
            question_masked=question_masked, rag=rag_agent
        )
    )
    add_messages_to_assistant([initial_prompt, text_sql_template], rag_agent.assistant)
    return initial_prompt, text_sql_template, df_recs_list_out, question_masked


def add_messages_to_assistant(messages: list, assistant=None):
    for message in messages:
        role = "system"
        assistant.add_message(role=role, message=message)
