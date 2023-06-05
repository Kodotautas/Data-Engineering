python3 -m \
    /home/vytautas/Documents/GitHub/Data-Engineering/4_Lithuania_statistics/dataflow_pipeline/main.py \
    --region europe-central2 --input \
    gs://dataflow-samples/shakespeare/kinglear.txt \
    --output \
    gs://lithuania_statistics/output \
    --runner DataflowRunner \
    --project vl-data-learn \
    --temp_location \
    gs://lithuania_statistics/temp/