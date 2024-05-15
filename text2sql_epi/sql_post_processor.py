import asyncio
import logging
import re


logger = logging.getLogger(__name__)

ICD_VOCABULARIES = ["ICD10CM", "ICD9CM"]
SNOMED_VOCABULARIES = ["SNOMED"]


class MedicalSQLProcessor:
    def __init__(self, assistant=None):
        self.assistant = assistant
        self.condition = []
        self.procedure = []
        self.drug = []
        self.measurement = []
        self.concept_not_found = []

    async def get_replacement_value(self, entity, name, medcodeonto=None):
        entity_to_domain_id = {
            "condition": "Condition",
            "procedure": "Procedure",
            "drug": "Drug",
            "measurement": "Measurement",
        }

        if entity in entity_to_domain_id:
            entity_codes_df = await medcodeonto.get_similar_codes_from_onto(question_masked=name,
                                                                         top_k_screening=10,
                                                                         top_k_prompt=4,
                                                                         sim_threshold=0.0)

            entity_codes = {name: entity_codes_df.to_dict('records')}
            print(f"Retrieved codes: {entity_codes}")
            print("Please note that the medical coding is based on a mockup ontology. "
                  "Results will not be reliable")
            # Get the current list for the entity
            current_list = getattr(self, entity, [])
            # Append the new entity codes to the list
            current_list.append(entity_codes)
            # Update the attribute on the object
            setattr(self, entity, current_list)
            return entity_codes
        else:
            return []

    async def get_entity_codes(self, name, domain_id, vocabularies):
        tasks = [
            self.get_codes(name, domain_id, vocabulary_name)
            for vocabulary_name in vocabularies
        ]
        results = await asyncio.gather(*tasks)
        return self.merge_results(vocabularies, results)

    def merge_results(self, vocabularies, results):
        merged_codes = {}
        for vocabulary_name, codes in zip(vocabularies, results):
            for key in codes:
                if key in merged_codes:
                    merged_codes[key].extend(codes[key])
                else:
                    merged_codes[key] = codes[key]
        return merged_codes

    async def replace_function(self, match, medcodeonto):
        entity, name = match
        replacement_value = await self.get_replacement_value(
            entity, name, medcodeonto
        )
        return self.format_replacement_result(replacement_value[name])

    def format_replacement_result(self, result):
        if len(result) > 0 and "error" not in result:
            concept_ids = [str(concept["CONCEPT_ID"]) for concept in result]
            return ",".join(concept_ids)
        else:
            self.concept_not_found.append(str(result))
            return "NO_CONCEPT_IDS_FOUND"

    def get_concept_id_not_found(self):
        return self.concept_not_found

    async def post_process_sql_query(
        self,
        sql_text,
        max_retries=5,
        sleep_sec=0,
        explorer_concepts=None,
        selected_coding=None,
        rag=None,
        medcodeonto=None
    ):
        """
        Post-processes an SQL query by replacing placeholders with actual values based on the selected coding system
        and explorer concepts. It also handles retries and user prompts if the SQL query does not meet certain criteria.

        Parameters
        ----------
        sql_text : str
            The initial SQL query text with placeholders for dynamic replacement.
        max_retries : int
            The maximum number of retries allowed to correct the SQL query.
        sleep_sec : int
            The number of seconds to wait before retrying.
        explorer_concepts : dict
            A dictionary of user selected concepts used for replacing placeholders in the SQL query.
        selected_coding : dict
            A dictionary containing the selected coding system information. Can be condition, drug, procedure.
        rag : Rag
            Contains the assistant for adding messages and getting responses from ai.

        Returns
        -------
        str or None
            The post-processed SQL query text ready for execution, or None if no valid concept IDs are found.

        Raises
        ------
        asyncio.TimeoutError
            If the user does not respond within the given time frame.

        """
        pattern = r"\[([a-z]+)@([a-zA-Z0-9_/\-\(\)\'\\ ]+)\]"
        attempts = 0

        while attempts <= max_retries:
            if selected_coding.get("condition") == ICD_VOCABULARIES:
                # Replace condition_concept_id to condition_source_concept_id
                sql_text = self.replace_condition_concept_id_to_condition_source(
                    sql_text
                )

            matches = re.findall(pattern, str(sql_text))
            modified_sql = await self.process_matches(
                matches, sql_text, explorer_concepts, medcodeonto
            )

            if "NO_CONCEPT_IDS_FOUND" in modified_sql:
                return None

            # Check if there are any concept_name in text inside the query
            if not self.is_sql_for_concept_name_in(sql_text):
                return modified_sql

            # Make the sql correct
            sql_text = await self.handle_invalid_sql(rag, sleep_sec)
            attempts += 1

        return sql_text

    async def process_matches(
        self, matches, sql_text, explorer_concepts, medcodeonto=None
    ):
        """
        Process all regex matches and replace them in the SQL text.
        """
        coroutines = [
            self.get_replacement(match, explorer_concepts, medcodeonto)
            for match in matches
        ]
        replacements = await asyncio.gather(*coroutines)
        return self.apply_replacements_to_sql(matches, replacements, sql_text)

    async def get_replacement(self, match, explorer_concepts, medcodeonto):
        """
        Get the replacement for a given match.
        """
        category, group_key = match
        if not explorer_concepts or group_key not in explorer_concepts:
            return await self.replace_function(match, medcodeonto)

        values = explorer_concepts[group_key]["value"]
        formatted_ids = self.format_replacement_result(values)
        grouped_values = self.create_grouped_values(group_key, values)
        self.update_attribute(category, grouped_values)

        return formatted_ids

    def create_grouped_values(self, group_key, values):
        """
        Group values by their categories.
        """
        return {
            group_key: [
                {
                    key: value[key]
                    for key in ("CONCEPT_ID", "CONCEPT_NAME", "CONCEPT_CODE")
                }
                for value in values
            ]
        }

    def update_attribute(self, attribute_name, grouped_values):
        """
        Update the attribute of the class with the grouped values.
        """
        current_attribute_value = getattr(self, attribute_name, [])
        current_attribute_value.append(grouped_values)
        setattr(self, attribute_name, current_attribute_value)

    def apply_replacements_to_sql(self, matches, replacements, sql_text):
        """
        Apply the replacements to the SQL text.
        """
        modified_sql = sql_text
        for match, replacement in zip(matches, replacements):
            if isinstance(replacement, list):
                replacement = ", ".join(map(str, replacement))
            modified_sql = modified_sql.replace(f"[{match[0]}@{match[1]}]", replacement)
        return modified_sql

    async def handle_invalid_sql(self, rag, sleep_sec):
        """
        Handle the case where the SQL does not meet the required criteria.
        """
        prompt = """Your generated SQL query doesn't meet the requirements.
            Please correct the sql query based on the previously given instructions.
            [!IMPORTANT] Do not include conditions, such as 'WHERE concept_name IN ...' or  'WHERE concept_name = "..."'
            [!IMPORTANT] Do not return anything except the sql query"""

        rag.assistant.add_message(prompt)
        completed_prompt = await self.assistant.get_response()
        sql_text = self.parse_sql_from_response(completed_prompt)
        if sleep_sec > 0:
            await asyncio.sleep(sleep_sec)
        return sql_text

    @staticmethod
    def parse_sql_from_response(resp=""):
        if resp is None:
            resp = ""

        pattern1 = r"(?:Snowflake )?SQL query:\s*\n\n([\s\S]+?);"
        pattern2 = r"(?:```sql|```) ?\n([\s\S]+?)\n```"
        match1 = re.search(pattern1, resp)
        match2 = re.search(pattern2, resp)
        if match2:
            return match2.group(1)
        elif match1:
            return match1.group(1) + ";"
        else:
            logger.info("No SQL code found.")

    @staticmethod
    def parse_python_from_response(resp=""):
        pattern = r"(?:```python|```) ?\n([\s\S]+?)\n```"
        match = re.search(pattern, resp)
        if match:
            return match.group(1)
        else:
            logger.info("No python code found.")

    @staticmethod
    def parse_json_from_response(resp=""):
        pattern = r"(?:```json|```) ?\n([\s\S]+?)\n```"
        match = re.search(pattern, resp)
        if match:
            return match.group(1)
        else:
            logger.info("No Json code found.")

    @staticmethod
    def save_string_to_file(text="", filename="log.txt"):
        with open(filename, "a") as text_file:
            text_file.write("\n")
            if text:
                text_file.write(text)

    @staticmethod
    def is_sql_for_concept_name_in(sql_text):
        pattern = r"WHERE\s+(?:[a-zA-Z]\.)?concept_name\s*(?:=|IN)\s*\(?(?:'[^']+',\s*)*'?[^']+'?\)?"
        match = re.search(pattern, str(sql_text))
        return bool(match)

    @staticmethod
    def replace_condition_concept_id_to_condition_source(sql_text):
        pattern = re.compile(r"condition_concept_id", re.IGNORECASE)
        replacement = lambda match: (
            "condition_source_concept_id"
            if match.group().islower()
            else "CONDITION_SOURCE_CONCEPT_ID"
        )
        return pattern.sub(replacement, sql_text)