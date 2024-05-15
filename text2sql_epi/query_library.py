# coding=utf-8
__author__ = "Angelo Ziletti"
__maintainer__ = "Angelo Ziletti"
__email__ = "angelo.ziletti@bayer.com"
__date__ = "24/11/23"

import logging
import os.path
import pickle
import time
from concurrent.futures import as_completed, ThreadPoolExecutor
from datetime import date
from typing import Optional

import numpy as np
import pandas as pd
from sentence_transformers import SentenceTransformer
from sklearn.preprocessing import normalize
from tqdm import tqdm

tqdm.pandas()

logger = logging.getLogger(__name__)


class QueryLibrary:
    """Collection of queries for retrieval augmented generation"""

    def __init__(
        self,
        querylib_name: str,
        source: str,
        querylib_source_file: object,
        col_question: str,
        col_question_masked: str,
        col_query_w_placeholders: str,
        col_query_executable: Optional[str] = None,
        date_live: Optional[date] = None,
    ) -> None:
        self.querylib_name = querylib_name
        self.date_live = date_live
        self.source = source
        self.col_question = col_question
        self.col_question_masked = col_question_masked
        self.col_query_w_placeholders = col_query_w_placeholders
        self.col_query_executable = col_query_executable

        if querylib_source_file:
            df_querylib = pd.read_excel(querylib_source_file)
            self.df_querylib = df_querylib
        else:
            self.df_querylib = pd.DataFrame()

        self.embeddings = []

        self.embedding_model = None

    def __len__(self):
        return len(self.df_querylib)

    def calc_embedding(
        self, embedding_model_name="BAAI/bge-large-en-v1.5", use_masked=True
    ):
        # check this: https://github.com/FlagOpen/FlagEmbedding/tree/master/FlagEmbedding/llm_embedder
        embedding_model = SentenceTransformer(embedding_model_name)
        if use_masked:
            col_txt = self.col_question_masked
        else:
            col_txt = self.col_question

        embed_series = self.df_querylib[col_txt].progress_apply(
            lambda x: embedding_model.encode(x, normalize_embeddings=True)
        )

        # as output you get a series of arrays, convert it to a matrix
        # https://stackoverflow.com/questions/40824601/how-to-convert-a-series-of-arrays-into-a-single-matrix-in-pandas-numpy
        embed_matrix = np.stack(embed_series.values)

        logger.info("Dataset embedded. Shape: {}".format(embed_matrix.shape))

        embedding = {
            "model_name": str(embedding_model_name),
            "embed_matrix": embed_matrix,
        }
        self.embeddings.append(embedding)

        logger.info("Embedding calculated with model {}".format(embedding_model_name))
        logger.info(" Embedding matrix shape: {}".format(embed_matrix.shape))
        self.embedding_model = embedding_model

    def save(self, querylib_file):
        # Extract the directory part from the file path
        directory = os.path.dirname(querylib_file)

        # Check if the directory exists, and create it if it doesn't
        if not os.path.exists(directory):
            os.makedirs(directory, exist_ok=True)  # exist_ok=True is to avoid error if the directory already exists

        # Now save the file as before
        with open(querylib_file, "wb") as out_file:
            pickle.dump(self, out_file)

    def load_embedding_model(self, embedding_model_name):
        self.embedding_model = SentenceTransformer(embedding_model_name)

    @staticmethod
    def load(querylib_file):
        try:
            with open(querylib_file, "rb") as out_file:
                query_lib_data = pickle.load(out_file)
                logger.info("Query library data read from {}".format(querylib_file))
            return query_lib_data
        except Exception as e:
            logger.exception("An error occured {}".format(e))
            return None

    def extract_idx_records(self, values_to_extract, source_col):
        idx_records = self.df_querylib.index[
            self.df_querylib[source_col].isin(values_to_extract)
        ].tolist()
        return idx_records

    def extract_embed_matrix(self, value_rows_to_extract, extract_col_name, embedding):
        # Find the rows in the ontology that match name_rows_to_extract
        idx_records = self.extract_idx_records(value_rows_to_extract, extract_col_name)

        # Get the corresponding embedding matrix
        embed_matrix = embedding["embed_matrix"][idx_records]

        # Get the names from the matrix so they match the embeddings
        value_rows_embed = self.df_querylib.loc[idx_records][
            extract_col_name
        ].reset_index(drop=True)

        return embed_matrix, value_rows_embed

    @staticmethod
    def add_separator_to_input_entities(lst, sep="[SEP_P]"):
        joined_list = []
        for inner_list in lst:
            joined_list.append(f" {sep} ".join(inner_list))
        return joined_list

    def get_similar_questions(
        self,
        samples,
        top_k=5,
        sim_threshold=0.95,
        normalize_score=True,
        col_search=None,
        max_rows=1000,
        tmp_dir=None,
        export_txt=False,
    ):
        if col_search is None:
            col_search = self.col_question

        df_querylib_selected = self.df_querylib

        embed_matrix, names_avail = self.extract_embed_matrix(
            value_rows_to_extract=df_querylib_selected[self.col_question].tolist(),
            extract_col_name=self.col_question,
            embedding=self.embeddings[0],
        )

        # Cast the input samples in a dataframe for convenience
        samples_with_sep = self.add_separator_to_input_entities(samples)
        df_input_names = pd.DataFrame(samples_with_sep, columns=[self.col_question])

        df_input_names_list = [
            df_input_names[i : i + max_rows]
            for i in range(0, df_input_names.shape[0], max_rows)
        ]

        df_recap_recs_list = []
        df_recs_list = []

        outfile_recap_recs_list = []
        outfile_recs_list = []

        # Parallel processing for get_similar_names
        with ThreadPoolExecutor() as executor:
            futures = {
                executor.submit(
                    self.get_similar_names,
                    df_input_chunk,
                    names_avail,
                    embed_matrix=embed_matrix,
                    embedding_model=self.embedding_model,
                    col_search=col_search,
                    normalize_score=normalize_score,
                    top_k=top_k,
                    sim_threshold=sim_threshold,
                    export_txt=export_txt,
                ): idx
                for idx, df_input_chunk in enumerate(df_input_names_list)
            }

            for future in as_completed(futures):
                idx = futures[future]
                df_recap_recs, df_recs = future.result()

                if tmp_dir is not None:
                    outfile_recap_recs = os.path.join(tmp_dir, f"recap_recs_{idx}")
                    outfile_recs = os.path.join(tmp_dir, f"recs_{idx}")

                    with open(outfile_recap_recs, "wb") as out_file:
                        pickle.dump(df_recap_recs, out_file)

                    with open(outfile_recs, "wb") as out_file:
                        pickle.dump(df_recs, out_file)

                    outfile_recap_recs_list.append(outfile_recap_recs)
                    outfile_recs_list.append(outfile_recs)

                    del df_recap_recs, df_recs
                else:
                    df_recap_recs_list.append(df_recap_recs)
                    df_recs_list.extend(df_recs)

        if tmp_dir is not None:
            df_recap_recs_list = []
            for outfile_recap in outfile_recap_recs_list:
                with open(outfile_recap, "rb") as out_file:
                    df_recap_recs_list.append(pickle.load(out_file))

            df_recs_list = []
            for outfile_rec in outfile_recs_list:
                with open(outfile_rec, "rb") as out_file:
                    df_recs_list.extend(pickle.load(out_file))

        df_recap_recs = pd.concat(df_recap_recs_list)

        return df_recap_recs, df_recs_list

    def get_similar_names(
        self,
        df,
        classes,
        embed_matrix,
        embedding_model,
        col_search,
        suffix="unsupervised",
        normalize_score=True,
        top_k=20,
        sim_threshold=0.0,
        top_k_limit=None,
        export_txt=True,
    ):
        if top_k_limit is None:
            top_k_limit = len(classes)

        logger.debug("Retrieving the most similar classes")

        # remove leading and trailing spaces
        df[col_search] = df[col_search].astype(str).str.strip()

        # Compute embeddings in batches (assuming embedding_model can handle batch input)
        text_embeddings = embedding_model.encode(
            df[self.col_question].tolist(), normalize_embeddings=True
        )

        logger.debug(f"Text embedding size: {text_embeddings.nbytes / 10 ** 6} (Mb)")
        logger.debug(f"text_embedding shape: {text_embeddings.shape}")

        if normalize_score:
            text_embeddings = normalize(text_embeddings)
            embed_matrix = normalize(embed_matrix)

        # Efficient matrix multiplication
        sim_matrix = text_embeddings @ embed_matrix.T

        logger.debug(f"Similarity matrix size: {sim_matrix.nbytes / 10 ** 6} (Mb)")
        logger.debug(f"sim_matrix shape: {sim_matrix.shape}")

        # Efficient top-k selection
        idx_match_sorted = np.argpartition(-sim_matrix, kth=top_k_limit - 1, axis=1)[
            :, :top_k_limit
        ]

        matched_classes_list = []
        scores_list = []
        code_list = []
        df_class_score_list = []

        for idx_input in range(sim_matrix.shape[0]):
            idx_match_input = idx_match_sorted[idx_input]
            sim_matrix_row_sorted = -np.partition(
                -sim_matrix[idx_input], kth=top_k_limit - 1
            )[:top_k_limit]
            class_text_sorted = classes[idx_match_input]

            df_class_scores = pd.DataFrame(
                zip(class_text_sorted, sim_matrix_row_sorted),
                columns=["Class", "Score"],
            )
            df_class_scores = df_class_scores.nlargest(top_k, "Score")

            matched_classes_list.append(df_class_scores["Class"].tolist())
            scores_list.append(df_class_scores["Score"].tolist())

            df_class_scores[self.col_question] = df_class_scores["Class"]
            code_list.append(df_class_scores[self.col_question].tolist())

            if not export_txt:
                df_class_scores = df_class_scores.drop(
                    ["Class"], axis=1, errors="ignore"
                )

            df_class_score_list.append(df_class_scores)

        df[f"rec_{suffix}_questions"] = code_list
        df[f"rec_{suffix}_scores"] = scores_list

        return df, df_class_score_list

    def get_df_recs(self, question, top_k, sim_threshold):
        df_recap_recs, df_recs_list = self.get_similar_questions(
            question,
            top_k=top_k,
            sim_threshold=sim_threshold,
        )

        # here the list is only one element long because we pass only one question
        df_recs_list_merged = []
        for df_recs in df_recs_list:
            df_merged = df_recs.merge(self.df_querylib, on=self.col_question, how="left")
            df_recs_list_merged.append(df_merged)

        # here we only have one question thus the list is always one element
        # TO DO: this is not ideal and it should be improve later
        df_recs_list_out = df_recs_list_merged[0]
        return df_recs_list_out

    async def text_sql_template_for_rag(
        self,
        question_masked,
        top_k_screening,
        top_k_prompt,
        sim_threshold,
        reverse_order=False,
        rag_random=False,  # Parameter for random retrieval
        drop_first=False,  # Parameter to drop the first element
    ):
        df_recs_list_out = self.get_df_recs(
            [[question_masked]],
            top_k=top_k_screening,
            sim_threshold=sim_threshold,
        )

        # If reverse_order is True, reverse the order of the DataFrame
        if reverse_order:
            df_recs_list_out = df_recs_list_out.sort_index(ascending=False)

        # If drop_first is True, drop the first element from the DataFrame
        if drop_first:
            logger.warning("Dropping first element of the retrieved queries")
            df_recs_list_out = df_recs_list_out.drop(df_recs_list_out.index[0])

        # If rag_random is True, randomly select one sample from the top-k
        if rag_random:
            logger.warning("Using random retrieval for RAG")
            df_recs_list_out = df_recs_list_out.sample(n=1)

        # Keep only the top_k_prompt elements
        df_recs_list_out = df_recs_list_out.head(top_k_prompt)

        initial_sentence = "You might find these example queries helpful: "
        # include both question and query in the prompt
        text_sql_template = (
            initial_sentence
            + "\n\n"
            + "\n\n".join(
                f"#Question:\n{rec[self.col_question]}\n#SQL query:\n{rec[self.col_query_w_placeholders]}"
                for rec in df_recs_list_out.to_dict("records")
            )
        )

        return text_sql_template, df_recs_list_out

    @staticmethod
    async def get_masked_question(
        prompts,
        question,
        assistant,
        sleep_sec=3,
        reset_conversation=True,
        mask="DRUG_CLASS",
    ):
        """
        :param prompts: List of prompts
        :param question: User question
        :param assistant: Assistant to use
        :param sleep_sec: Number of seconds to sleep
        :param reset_conversation: True or False to reset the conversation
        :param mask: Mask to apply
        :return: masked question, question
        """
        if reset_conversation:
            assistant.reset_conversation()
        prompt = prompts.entity_masking.format(question=question)
        assistant.add_message(role="user", message=prompt)
        masked_question = await assistant.get_response()
        if mask in masked_question:
            prompt_drug_class = prompts.drug_class_keep.format(question=question)
            question = await assistant.get_response(prompt_drug_class)
            if masked_question.count(mask) > 1 or (
                masked_question.count(mask) == 1 and masked_question.count("DRUG") > 1
            ):
                question += (
                    " Can you output also intermediate results for each drug class?"
                )
            prompt = prompts.entity_masking.format(question=question)
            masked_question = await assistant.get_response(prompt)

        logger.info(f"Masked question: {masked_question}")
        time.sleep(sleep_sec)
        return masked_question, question


class MedCodingOnto(QueryLibrary):
    def __init__(
        self,
        ontolib_name: str,
        source: str,
        ontolib_source_file: object,
        col_text: str,
        date_live: Optional[date] = None,
        # Add any additional parameters specific to your new class here
    ) -> None:
        super().__init__(
            querylib_name=ontolib_name,
            source=source,
            querylib_source_file=ontolib_source_file,
            col_question=col_text,
            col_question_masked=col_text,
            date_live=date_live,
            col_query_w_placeholders=None,
            col_query_executable=None
        )
        self.querylib_name = ontolib_name
        self.date_live = date_live
        self.source = source
        self.col_question = col_text
        self.col_question_masked = col_text
        self.col_query_w_placeholders = None
        self.col_query_executable = None

        if ontolib_source_file:
            df_querylib = pd.read_excel(ontolib_source_file)
            self.df_querylib = df_querylib
        else:
            self.df_querylib = pd.DataFrame()

        self.embeddings = []

        self.embedding_model = None
        # Add any additional initialization specific to your new class here

    async def get_similar_codes_from_onto(
            self,
            question_masked,
            top_k_screening,
            top_k_prompt,
            sim_threshold
    ):
        df_recs_list_out = self.get_df_recs(
            [[question_masked]],
            top_k=top_k_screening,
            sim_threshold=sim_threshold,
        )

        # Keep only the top_k_prompt elements
        df_recs_list_out = df_recs_list_out.head(top_k_prompt)

        return df_recs_list_out
