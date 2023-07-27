python3 -m \
    /home/vytautas/Documents/GitHub/Data-Engineering/4_Lithuania_statistics/dataflow_pipeline/main \
    --region europe-central2 \
    --output gs://lithuania_statistics/output \
    --runner DataflowRunner \
    --project vl-data-learn \
    --temp_location  gs://lithuania_statistics/temp/ \
    --setup_file  gs://setup-dataflow/my_dataflow_pipeline-1.0.tar.gz 