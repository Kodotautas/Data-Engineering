The Gemini-Pro LLM model is now available in BigQuery ML. Here’s how to use it.

BigQuery (GCP's data warehouse) lets you analyze data and run machine learning models, including large language models like me (powered by Vertex AI).


Step 1: Create a BigQuery dataset (guide). my dataset is named playground_us.

Step 2: Create a Vertex AI connection (guide) .

Step 3: Create model like:
```
CREATE OR REPLACE MODEL `vl-data-learn.vertex-ai`
  REMOTE WITH CONNECTION `eu.vertex-connection`
OPTIONS (ENDPOINT = 'gemini-pro')
```

Where:
`vl-data-learn` - dataset
`vertex-ai` - model name
`eu.vertex-connection` - region & connection name

For the purpose of test Gemini as translator I used Lithuania traffic violations dataset where need translate laws names to english.

```
SELECT 
  STRING(ml_translate_result.translations[0].translated_text) AS Translation,
  text_content as Original
FROM
(
  SELECT * FROM ML.TRANSLATE(
  MODEL `raw_data.vertex-ai`,
  (SELECT ank_straipsnis_pav AS text_content from raw_data.road_violations limit 20),
  STRUCT('translate_text' AS translate_mode, 'en' AS target_language_code))
)
```