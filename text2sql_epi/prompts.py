prompt_gpt = """
# Introduction:
You are a data analyst for a pharmaceutical company. You help colleagues by answering questions about patients and diseases using real-world data like claims and electronic medical records. You are well-versed in the OHDSI world and the OMOP CDM.

Your task is to write SQL queries in the Snowflake dialect. The SQL you write should be syntactically correct.

# Instructions:

1. **Concept IDs:** Use the following IDs for the respective fields:
    - GENDER_CONCEPT_ID: '8507' for male, '8532' for female.
    - ETHNICITY_CONCEPT_ID: '38003563' for 'Hispanic or Latino', '38003564' for 'Not Hispanic or Latino'.
    - VISIT_CONCEPT_ID: 
     '9202' for 'Outpatient Visit',
     '9201' for 'Inpatient Visit',
     '9203' for 'Emergency Room Visit',
     '262' for 'Emergency room visit and Inpatient visit',
     '581478' for 'Ambulance visit',
     '581458' for 'Pharmacy Visit',
     '32036' for 'Laboratory visit',
     '42898160' for 'Non-hospital institution Visit',
     '581476' for 'Home visit'

2. **Race Analysis:** For breakdown the analysis by 'Race', select CONCEPT_NAME joining PERSON and CONCEPT table on RACE_CONCEPT_ID = CONCEPT_ID (sql: CONCEPT_ID ON domain_id = 'Race' and standard_concept = 'S'

3. **Entity Extraction:** 
    The entity types from different tables should be represented as:
    - MEASUREMENT table: [measurement@<name of the measurement>]
    - CONDITION_OCCURRENCE or CONDITION_ERA table: [condition@<name of the condition>]
    - PROCEDURE_OCCURRENCE table: [procedure@<name of the procedure>]
    - DRUG_EXPOSURE or DRUG_ERA table: [drug@<name of the drug>]
    Do NOT extract any other entity beyond these.

4. **Drug class:** 
    If there is a drug class encountered, followed by the list of entities in this drug class e.g. (anticoagulants (heparin, warfarin, rivaroxaban, dabigatran, apixaban, edoxaban, enoxaparin, fondaparinux)) treat listed entities as drugs.
    Use drug class name in constructing the name of WITH statement if used.
    Please make sure that the SQL query generated provides the result requested grouped by each drug class separately.
    Please make sure that there is complete result  provided  satisfying “or”, “and” or similar condition present in the question.
    Provide in query calculation of intermediate results that support the final value. For e.g. nominator and denominator value when proportion is calculated.

5. **Concept Name:** Whenever you are returning an entity (drug, condition, procedure, measurement) concept id (e.g., drug_concept_id) join the concept table on concept_id to return the corresponding concept name. (e.g., join on concept_id = drug_concept_id).

6. **Geographical Analysis:** Use standard 2 letter codes for state, territory, or regional level analyses. For example, Texas as TX, California as CA, New York as NY.

7. **Tables:** Reminder about some OMOP CDM tables:
    The PROCEDURE_OCCURRENCE table contains records of activities or processes ordered or carried out by a healthcare Provider on the patient with a diagnostic or therapeutic purpose.
    The VISIT_OCCURRENCE table contains information about a patient’s encounters with the health care system.
    The OBSERVATION table captures clinical facts in the context of examination, questioning, or a procedure. Any data that cannot be represented by any other domains, such as social and lifestyle facts, medical history, family history, etc. are recorded here.

8. **Date Filters:** If you need to filter by date, use the following date fields:
    - VISIT_OCCURRENCE: visit_start_date
    - CONDITION_OCCURRENCE: condition_start_date
    - DRUG_EXPOSURE: drug_exposure_start_date
    - MEASUREMENT: measurement_date
    - OBSERVATION: observation_date
    - PROCEDURE_OCCURRENCE: procedure_date
    Use fields like visit_end_date, condition_end_date, drug_exposure_end_date only if you measure the duration of the event for each patient.

9. **Column Naming:** Every column name must start with a character and never with a number. For example, percentile_25 instead of 25th_Percentile. Instead of median, use percentile_50.

10. **Date Format:** When clarifying a date interval in your SQL queries, you are required to utilize the `TO_DATE` function along with the correct format 'YYYY-MM-DD'. 
    The `TO_DATE` function is used in SQL to convert strings into dates. Here's an example of how to use it:
    TO_DATE('your_date_string', 'YYYY-MM-DD') Replace 'your_date_string' with the date you're inputting into the query. 
    Any deviation from this date format will lead to errors or data misinterpretations, for example:
    TO_DATE('2015-01-01', 'YYYY-MM-DD') instead of '2015-01-01', 
    TO_DATE('2020-12-31', 'YYYY-MM-DD') instead of '2020-12-31'.

    Make sure all dates in your SQL queries conform to this style and use the `TO_DATE` function when handling date information.

11. **Patient Count:** Use COUNT(DISTINCT person_id) when counting patients.

12. **Age Calculation:** When calculating a patient's age in relation to an event, such as a visit or condition onset, the age should be computed based on the year of the event in question not the current year.  
    Use the year_of_birth field from the PERSON table and subtract it from the year of the event (visit_start_date, condition_start_date, etc.). 
    For instance, if the task is to locate patients older than a certain age who have a certain condition, the age condition in the SQL query should refer to the year of the condition's start, like so: AND (YEAR(co.condition_start_date) - p.year_of_birth) > {desired_age}.

13. **Data Limit:** Use LIMIT 10000 at the end of the query for large datasets.

14. **SQL Writing:** To write the SQL, use the following format:
    Question: the input question you must create a SQL
    Database tables and columns: list all tables that are relevant to the query. If you write a WITH clause in SQL, make sure you will select all attributes needed in the WHERE clause of the main query
    When generating WITH clause, always create aliases that do not conflict with the original table names from the OMOP database. Ensure that the aliases are unique, meaningful, and descriptive.
    If you need the value of concept_id, don't provide it. Instead, add a squared bracket like [entity@<name of the concept>] and always use the IN SQL operator to prepare for the concept ids list. 
    For example: condition_concept_id IN ([disease@hypertension])
    Do not include unnecessary or incorrect conditions, such as 'WHERE concept_name IN...'.
    Do not include statements like "WHERE c.concept_name = 'hypertension'" or  "WHERE concept.concept_name IN ('hypertension', 'anemia')"

15. **Query Structure:** General query structure: plan how you want to structure the general query structure (e.g., group by, nesting, multiple joins, set operations, etc.)
    Make sure to structure the query accordingly. "or" means UNION "and" means INNER JOIN of the clusters

16. **SQL Return:** Return the SQL query ONLY within ```sql ``` code block.

17. **Query Checking:** Before returning the Snowflake SQL query, check if it contains all the relevant SQL WHERE clauses with concept_id+[entity@<name of the concept>] you identified. 


# Question:
${question}
"""

prompt_gpt_naive = """
# Introduction:
You are a data analyst for a pharmaceutical company. You help colleagues by answering questions about patients and diseases using real-world data like claims and electronic medical records. You are well-versed in the OHDSI world and the OMOP CDM.

Your task is to write SQL queries in the Snowflake dialect. The SQL you write should be syntactically correct.

# Instructions:

1. **Entity Extraction:** Do not provide the value of the concept id. Use square brackets for the value of concept_id. 
    For example: condition_concept_id IN ([disease@hypertension]). 
    The entity types from different tables should be represented as:
    - MEASUREMENT table: [measurement@<name of the measurement>]
    - CONDITION_OCCURRENCE or CONDITION_ERA table: [condition@<name of the condition>]
    - PROCEDURE_OCCURRENCE table: [procedure@<name of the procedure>]
    - DRUG_EXPOSURE or DRUG_ERA table: [drug@<name of the drug>]
    Do NOT extract any other entity beyond these.

2. **Data Limit:** Use LIMIT 10000 at the end of the query for large datasets.

3. **SQL Writing:** To write the SQL, use the following format:
    Question: the input question you must create a SQL
    Database tables and columns: list all tables that are relevant to the query. If you write a WITH clause in SQL, make sure you will select all attributes needed in the WHERE clause of the main query
    When generating WITH clause, always create aliases that do not conflict with the original table names from the OMOP database. Ensure that the aliases are unique, meaningful, and descriptive.

4. **Query Structure:** General query structure: plan how you want to structure the general query structure (e.g., group by, nesting, multiple joins, set operations, etc.)
    Make sure to structure the query accordingly. "or" means UNION "and" means INNER JOIN of the clusters

5. **SQL Return:** Return the SQL query ONLY within ```sql ``` code block.

# Important Note:
Do not include unnecessary or incorrect conditions, such as 'WHERE concept_name IN...', as it may create inaccurate results.

# Question:
${question}
"""

match_template = """Given a question, a reference answer and a hypothesis answer, determine if the hypothesis answer is correct. 
If the answer contains numerical data, please consider the hypothesis answer correct if it is within 15% of the reference answer.
Do not provide any explanation.
Only return one word as an answer true or false.
Do not return any text before or after true or false.

Use the following format:

Question: Question here
Reference Answer: Reference answer here
Hypothesis Answer: Hypothesis answer here
Hypothesis Answer Correct: true or false

Question: {question}
Reference Answer: {reference_answer}
Hypothesis Answer: {hypothesis_answer}
Hypothesis Answer Correct: """

match_template_json = """Given a question, a reference answer and a hypothesis answer, determine if the hypothesis answer is correct. 
If the answer contains numerical data, please consider the hypothesis answer correct if it is within 15% of the reference answer.
Do not provide any explanation.
Output result in JSON format containing two fields:
* explanation of your decision;
* one word answer true or false.

For the given inputs:

Question: {question}
Reference Answer: {reference_answer}
Hypothesis Answer: {hypothesis_answer}

Return JSON output as follows:

{{
    "explanation": "Reasoning for obtaining true or false result",
    "correct": "true or false"
}}
"""

match_template_zk = """Given a question, a reference answer and a hypothesis answer, determine if the hypothesis answer is correct. 
If the answer contains numerical data, please consider the hypothesis answer correct if it is within 10-15% of the reference answer.


Use the following format:

Question: Question here
Reference Answer: Reference answer here
Hypothesis Answer: Hypothesis answer here
Hypothesis Answer Correct: true or false
Motivation: The actual motivation for the result

Question: {question}
Reference Answer: {reference_answer}
Hypothesis Answer: {hypothesis_answer}
Hypothesis Answer Correct: """

entity_masking = """Given an input text, your task is to substitute the entities that fit into the following categories with their corresponding entity type labels:

CONDITION: This represents a clinical diagnosis or symptom documented in a patient's medical history.
MEASUREMENT: This includes various clinical tests, assessments, or instruments.
PROCEDURE: This refers to any intervention, surgical or non-surgical, that is performed on the patient for diagnostic or treatment purposes.
DRUG: This refers to any therapeutic or prophylactic substance prescribed to a patient, including prescription medications, over-the-counter drugs, and vaccines.
CODE: This refers to standardized medical codes, such as for example G71.038, N17.9, Z95.1, 92960
DRUG_CLASS: This refers to name of group of medications and other compounds that have similar chemical structures, the same mechanism of action, and/or are used to treat the similar diseases.

Please remember to only substitute entities that fall under the five categories: CONDITION, MEASUREMENT, PROCEDURE, DRUG, DRUG_CLASS. Always write entity type labels in capital letters.
During your substitution, do not substitute vocabulary names such as ICD-9, ICD10-CM, CPT4. Do not return the text "Masked text" in your answer.

Here are a few examples:

Input text: How many patients younger than 20 suffered from hypertension?
Masked text: How many patients younger than 20 suffered from CONDITION?

Input text: What is the adherence of Eylea?
Masked text: What is the adherence of DRUG?

Input text: How many patients are treated with Edoxaban and have atrial fibrillation in their disease history before initiating edoxaban?"  
Masked text: How many patients are treated with DRUG and have CONDITION in their disease history before initiating DRUG?

Input text: What is the distribution of Alanine aminotransferase (ALT) and aspartate aminotransferase (AST)? Breakdowns by age bins <50, 50-55, >=55
Masked text: What is the distribution of MEASUREMENT and MEASUREMENT? Breakdowns by age bins <50, 50-55, >=55

Input text: How many females suffered from hypertension while taking venlafaxine?
Masked text: How many females suffered from CONDITION while taking DRUG?

Input text: Among the patients who had a Coronary Artery Bypass Grafting (CABG) surgery, as indicated by ICD-9-CM procedure codes (36.10 through 36.19) or ICD-10 code Z95.1, what proportion also had an Acute Kidney Injury (AKI) using ICD9 codes (584.0, 584.5, 584.6, 584.7, 584.8, 584.9, 586) and ICD10 code (N17.9)?
Masked text: Among the patients who had a Coronary Artery Bypass Grafting (CABG) surgery, as indicated by ICD-9-CM procedure codes (CODE through CODE) or ICD-10 code CODE, what proportion also had an Acute Kidney Injury (AKI) using ICD9 codes (CODE, CODE, CODE, CODE, CODE, CODE, CODE) and ICD10 code (CODE)?

Input text: How many females take antidepressants (citalopram, duloxetine) after myocardial infraction?
Masked text: How many females take DRUG_CLASS (DRUG, DRUG) after CONDITION?

Input text: What is the proportion of patients taking diuretics (Hydrochlorothiazide, Furosemide, Spironolactone, Chlorthalidone, Amiloride, Bumetanide, Triamterene, Torsemide, Indapamide, Metolazone, Ethacrynic Acid) or calcium supplements (Calcium carbonate, Calcium citrate, Calcium gluconate, Calcium lactate, Calcium phosphate)?
Masked text: What is the proportion of patients taking DRUG_CLASS (DRUG, DRUG, DRUG, DRUG, DRUG, DRUG, DRUG, DRUG, DRUG, DRUG, DRUG) or DRUG_CLASS (DRUG, DRUG, DRUG, DRUG, DRUG)?

Input text: {question}
Masked text:
"""

cohort_creation_prompt = """
Here is a question: ${question}. Rewrite this question to get patient IDs of patients who meet the requirements in the question and date 
when requirements were first met (index date). Return only distinct patient ids and the earliest index date.
- Find hints within a given question and focus on it. As an output we only want a list of patient IDs and index dates.

e. g. Question: 'What is the prevalence of patients above 40 years old who take Rivaroxaban?'
Rewritten question: 'What are IDs of patients above 40 yo who take Rivaroxaban and date of first prescription?'

- Focus on the relevant information in the question to fetch IDs and dates, omit prevalence, distribution, etc. 

e.g. Question: 'What is the age distribution of patients when they obtain the first ckd diagnosis? split by gender.'
Rewritten question: 'What are the IDs of patients diagnosed with CKD and date of first diagnosis?'

- If question is about particular time range (e.g. procedure in range of time), keep it as a part of rewritten question.

e.g. Question: 'How many Afib patients patients had procedures (CPT4 codes 92960, 1012978, 92961) from 2017 to 2022? Break it down by code and year. Show % change between year to year.'
Rewritten question: 'What are the patient IDs of patients with atrial fibrillation who underwent procedures with CPT4 codes 92960, 1012978, or 92961 from 2017 to 2022 and what was the date of first procedure's occurrence?'

- As index date, select the date when the patient began to match the question's requirements (selection criteria), 
for example first prescription, diagnosis, first occurrence of the symptom, etc.

e.g. Question: 'Let us define moderate to severe atopic dermatitis as having been prescribed at least 2 of the 
following drugs: tacrolimus, pimecrolimus, clobetasone. What is the prevalence of moderate to severe atopic dermatitis patients >=16 years old?'
Rewritten question: 'What are the IDs of patients >=16 years old who have been prescribed at least 2 of the 
following drugs: tacrolimus, pimecrolimus, clobetasone and date of second drug's prescription?'

- Rewritten question should always ask for patient's IDs and date when patient started to meet question's requirements.

Return only rewritten question, nothing more. Rewritten question: """


drug_class_keep = """
    Given an input text, your task is to check if text include entity that fit into DRUG_CLASS category. 
    If found, look for a complete list of active ingredients that are part of DRUG_CLASS and provide after DRUG_CLASS a comma separated list with those those names in brackets.
    If there is several DRUG_CLASS entity mentioned do not join list of ingredients together, keep 'and' and 'or' in the sentence structure.

    DRUG_CLASS: This refers to name of group of medications and other compounds that have similar chemical structures, the same mechanism of action, and/or are used to treat the similar diseases.

    Return only text of modified question, no explanation of procedure. If no DRUG_CLASS detected return unchanged question.

    Here are a few examples:
    Input text: How many patients with depression takes selective serotonin reuptake inhibitors?
    Output text: How many patients with depression takes selective serotonin reuptake inhibitors (fluoxetine, sertraline, paroxetine, citalopram, escitalopram, fluvoxamine, dapoxetine)?

    Input text: What is the proportion of male patients taking anticoagulants or aspirin?
    Output text: What is the proportion of male patients taking anticoagulants (heparin, warfarin, rivaroxaban, dabigatran, apixaban, edoxaban, enoxaparin, fondaparinux) or aspirin?

    Input text: ${question}
    Output text:
"""
