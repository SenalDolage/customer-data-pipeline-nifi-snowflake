-- define stage
CREATE OR REPLACE STAGE SCD_DEMO.SCD2.customer_stage
url='s3://...'
credentials=(aws_key_id='...' aws_secret_key='...');

-- define file format
CREATE OR REPLACE FILE FORMAT SCD_DEMO.SCD2.csv_file_format
TYPE=CSV,
FIELD_DELIMITER = ","
SKIP_HEADER = 1;

SHOW STAGES;
LIST @customer_stage

CREATE OR REPLACE PIPE customer_s3_pipe
auto_ingest = true
AS
COPY INTO customer_raw
FROM @customer_stage
FILE_FORMAT = csv_file_format;

SHOW PIPES;
SELECT SYSTEM$PIPE_STATUS('customer_s3_pipe');


SELECT count(*) FROM customer_raw;

TRUNCATE customer_raw;
