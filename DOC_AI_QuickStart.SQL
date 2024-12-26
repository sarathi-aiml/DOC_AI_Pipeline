CREATE DATABASE DS_DEV_DB;

CREATE SCHEMA DS_DEV_DB.DOC_AI_SCHEMA;

use database ds_dev_db;

CREATE STAGE INVOICE_DOCS 
	DIRECTORY = ( ENABLE = true ) 
	ENCRYPTION = ( TYPE = 'SNOWFLAKE_SSE' );

CREATE STAGE IGNOIRED_DOCS 
	DIRECTORY = ( ENABLE = true ) 
	ENCRYPTION = ( TYPE = 'SNOWFLAKE_SSE' );

CREATE STAGE MANUAL_REVIEW 
	DIRECTORY = ( ENABLE = true ) 
	ENCRYPTION = ( TYPE = 'SNOWFLAKE_SSE' );
    
create or replace stream DS_DEV_DB.DOC_AI_SCHEMA.PREPROCESS_STREAM on directory(@INVOICE_DOCS);

CREATE OR REPLACE TABLE DS_DEV_DB.DOC_AI_SCHEMA.MODEL_METADATA (
    MODEL_NAME VARCHAR(255),          
    FLATTEN_TABLE VARCHAR(255),       
    VALIDATED_TABLE VARCHAR(255),     
    PATTERN VARCHAR(255),             
    PREDICTION_TYPE NUMBER(1)         
);

create or replace TABLE DS_DEV_DB.DOC_AI_SCHEMA.DOCAI_PREFILTER (
	ROWID NUMBER(38,0) autoincrement start 1 increment 1 noorder,
	MODEL_NAME VARCHAR(255),
	FILENAME VARCHAR(16777216),
	FILESIZE VARCHAR(255),
	NUMBER_OF_PAGES NUMBER(38,0),
	DATECREATED TIMESTAMP_LTZ(9),
	COMMENT VARCHAR(16777216),
	STATUS VARCHAR(16777216),
	PROCESS_START_TIME TIMESTAMP_LTZ(9),
	PROCESS_END_TIME TIMESTAMP_LTZ(9)
);


create or replace TABLE DS_DEV_DB.DOC_AI_SCHEMA.DOCAI_ORDERFORM_EXTRACTION (
	RELATIVEPATH VARCHAR(16777216),
	MODEL_NAME VARCHAR(255),
	SIZE NUMBER(38,0),
	FILE_URL VARCHAR(16777216),
	JSON VARIANT,
	COMMENTS VARCHAR(16777216),
	STATUS VARCHAR(16777216),
	PROCESS_START_TIME TIMESTAMP_LTZ(9),
	PROCESS_END_TIME TIMESTAMP_LTZ(9)
);


CREATE OR REPLACE TABLE Invoice_Flatten (
    RELATIVEPATH STRING,
    MODEL_NAME STRING,
    OCR_SCORE FLOAT,
    DELIVERY_DATE_SCORE FLOAT,
    DELIVERY_DATE_VALUE STRING,
    ORDER_DATE_SCORE FLOAT,
    ORDER_DATE_VALUE STRING,
    PO_NUMBER_SCORE FLOAT,
    PO_NUMBER_VALUE STRING,
    SUB_TOTAL_SCORE FLOAT,
    SUB_TOTAL_VALUE STRING,
    TAX_AMOUNT_SCORE FLOAT,
    TAX_AMOUNT_VALUE STRING,
    TOTAL_AMOUNT_SCORE FLOAT,
    TOTAL_AMOUNT_VALUE STRING,
    COMMENTS STRING,
    STATUS STRING,
    PROCESSED_TIMESTAMP TIMESTAMP,
    PROCESS_START_TIME TIMESTAMP,
    PROCESS_END_TIME TIMESTAMP
);

CREATE OR REPLACE TABLE DS_DEV_DB.DOC_AI_SCHEMA.invoice_col_score_failed_history (
    SCORE_NAME STRING,         
    SCORE_VALUE FLOAT,         
    DATE_FAILED TIMESTAMP,      
    FILENAME STRING,            
    COMMENTS STRING             
);

CREATE OR REPLACE TABLE Purchase_Flatten (
    RELATIVEPATH STRING,                         
    MODEL_NAME STRING,                           
    OCR_SCORE FLOAT,                             
    AMOUNT_SCORE FLOAT,                          
    AMOUNT_VALUE STRING,                         
    DISCOUNT_SCORE FLOAT,                       
    DISCOUNT_VALUE STRING,                       
    INVOICE_DATE_SCORE FLOAT,                   
    INVOICE_DATE_VALUE STRING,                   
    INV_NUMBER_SCORE FLOAT,                     
    INV_NUMBER_VALUE STRING,                     
    ITEM_DESCRIPTIONS_SCORE FLOAT,              
    ITEM_DESCRIPTIONS_VALUE STRING,              
    PO_NUMBER_SCORE FLOAT,                       
    PO_NUMBER_VALUE STRING,                      
    QUANTITY_SCORE FLOAT,                        
    QUANTITY_VALUE STRING,                       
    RATE_SCORE FLOAT,                            
    RATE_VALUE STRING,                           
    COMMENTS STRING,                             
    STATUS STRING,                               
    PROCESSED_TIMESTAMP TIMESTAMP,               
    PROCESS_START_TIME TIMESTAMP,                
    PROCESS_END_TIME TIMESTAMP                   
);

CREATE OR REPLACE TABLE DS_DEV_DB.DOC_AI_SCHEMA.purchase_col_score_failed_history (
    SCORE_NAME STRING,         
    SCORE_VALUE FLOAT,          
    DATE_FAILED TIMESTAMP,      
    FILENAME STRING,            
    COMMENTS STRING             
);

CREATE OR REPLACE TABLE Invoice_Validated (
    RELATIVEPATH STRING,
    DELIVERY_DATE_VALUE STRING,
    ORDER_DATE_VALUE STRING,
    PO_NUMBER_VALUE STRING,
    SUB_TOTAL_VALUE STRING,
    TAX_AMOUNT_VALUE STRING,
    TOTAL_AMOUNT_VALUE STRING,
    PROCESS_START_TIME TIMESTAMP,
    PROCESS_END_TIME TIMESTAMP
);

CREATE OR REPLACE TABLE Purchase_Validated (
    RELATIVEPATH STRING,                    
    AMOUNT_VALUE STRING,                    
    DISCOUNT_VALUE STRING,                  
    INVOICE_DATE_VALUE STRING,               
    INV_NUMBER_VALUE STRING,                 
    ITEM_DESCRIPTIONS_VALUE STRING,          
    PO_NUMBER_VALUE STRING,                  
    QUANTITY_VALUE STRING,                  
    RATE_VALUE STRING,                      
    PROCESS_START_TIME TIMESTAMP,           
    PROCESS_END_TIME TIMESTAMP              
);

CREATE OR REPLACE TABLE DS_DEV_DB.DOC_AI_SCHEMA.SCORE_THRESHOLD (
    MODEL_NAME VARCHAR(255),         
    SCORE_NAME VARCHAR(255),         
    SCORE_VALUE FLOAT              
);

CREATE OR REPLACE TABLE DS_DEV_DB.DOC_AI_SCHEMA.manual_review_history_log (
    ID BIGINT AUTOINCREMENT PRIMARY KEY,  
    FILENAME VARCHAR(255) NOT NULL,       
    ACTION VARCHAR(100) NOT NULL,         
    TIMESTAMP TIMESTAMP_NTZ NOT NULL,     
    USER_NAME VARCHAR(100) DEFAULT CURRENT_USER, 
    COMMENTS VARCHAR(500) NULL        
);

INSERT INTO DS_DEV_DB.DOC_AI_SCHEMA.MODEL_METADATA (MODEL_NAME, FLATTEN_TABLE, VALIDATED_TABLE, PATTERN, PREDICTION_TYPE)
VALUES
    ('INVOICE_MODEL', 'INVOICE_FLATTEN', 'INVOICE_VALIDATED', '%inv-%', 2),
    ('INVOICE_MODEL', 'INVOICE_FLATTEN', 'INVOICE_VALIDATED', '%INV%', 2),
    ('INVOICE_MODEL', 'INVOICE_FLATTEN', 'INVOICE_VALIDATED', '%invoice%', 2),
    ('INVOICE_MODEL', 'INVOICE_FLATTEN', 'INVOICE_VALIDATED', '%Invoice%', 2),
    ('PURCHASE_MODEL', 'PURCHASE_FLATTEN', 'PURCHASE_VALIDATED', '%Purchase%', 2),
    ('PURCHASE_MODEL', 'PURCHASE_FLATTEN', 'PURCHASE_VALIDATED', '%PURCHASE%', 2),
    ('PURCHASE_MODEL', 'PURCHASE_FLATTEN', 'PURCHASE_VALIDATED', '%purchase%', 2);

INSERT INTO DS_DEV_DB.DOC_AI_SCHEMA.SCORE_THRESHOLD (MODEL_NAME, SCORE_NAME, SCORE_VALUE)
VALUES
    ('INVOICE_MODEL', 'DELIVERY_DATE', 0.999),
    ('INVOICE_MODEL', 'ORDER_DATE', 0.999),
    ('INVOICE_MODEL', 'PO_NUMBER', 0.999),
    ('INVOICE_MODEL', 'SUB_TOTAL', 0.998),
    ('INVOICE_MODEL', 'TAX_AMOUNT', 0.251), 
    ('INVOICE_MODEL', 'TOTAL_AMOUNT', 0.776),
    ('INVOICE_MODEL', 'ocrScore', 0.9); 

INSERT INTO DS_DEV_DB.DOC_AI_SCHEMA.SCORE_THRESHOLD (MODEL_NAME, SCORE_NAME, SCORE_VALUE)
VALUES
    ('PURCHASE_MODEL', 'AMOUNT', 0.706),                      
    ('PURCHASE_MODEL', 'INVOICE_DATE', 1.0),            
    ('PURCHASE_MODEL', 'INV_NUMBER', 1.0),             
    ('PURCHASE_MODEL', 'ITEM_DESCRIPTIONS', 1.0),       
    ('PURCHASE_MODEL', 'PO_NUMBER', 0.945),             
    ('PURCHASE_MODEL', 'QUANTITY', 1.0),                
    ('PURCHASE_MODEL', 'RATE', 1.0),                    
    ('PURCHASE_MODEL', 'ocrScore', 0.887);     

CREATE OR REPLACE PROCEDURE DS_DEV_DB.DOC_AI_SCHEMA.COUNT_PDF_PAGES_PROC()
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.9'
PACKAGES = ('snowflake-snowpark-python', 'PyPDF2')
HANDLER = 'count_pages'
EXECUTE AS OWNER
AS '
import snowflake.snowpark as snowpark
import PyPDF2
import os
from datetime import datetime

def count_pages(session):
    # Temporary directory for downloading files
    temp_dir = "/tmp/pdf_files/"
    os.makedirs(temp_dir, exist_ok=True)

    # Stream and table details
    stream_name = "DS_DEV_DB.DOC_AI_SCHEMA.PREPROCESS_STREAM"
    temp_table_name = "STREAMDATA_TEMP"
    current_db = session.sql("SELECT CURRENT_DATABASE()").collect()[0][0]
    current_schema = session.sql("SELECT CURRENT_SCHEMA()").collect()[0][0]
    full_temp_table_name = f"{current_db}.{current_schema}.{temp_table_name}"
    prefilter_table_name = f"{current_db}.{current_schema}.DOCAI_PREFILTER"
    metadata_table_name = f"{current_db}.{current_schema}.MODEL_METADATA"

    # Step 1: Create a temporary table to store stream data
    session.sql(f"""
        CREATE OR REPLACE TABLE {full_temp_table_name} AS
        SELECT * FROM {stream_name} where metadata$action=''INSERT'';
    """).collect()

    # Step 2: Fetch data from the temporary table
    temp_data = session.sql(f"SELECT * FROM {full_temp_table_name}").collect()
    if not temp_data:
        session.sql(f"DROP TABLE IF EXISTS {full_temp_table_name}").collect()
        return "No files to process in the stream."
    
    processed_count = 0

    for row in temp_data:
        file_name = row["RELATIVE_PATH"].split("/")[-1]  # Extract the file name
        file_size = row["SIZE"]
        stage_file_path = f"@INVOICE_DOCS/{row[''RELATIVE_PATH'']}"
        process_start_time = datetime.now()

        # Step 3: Determine MODEL_NAME dynamically from metadata
        normalized_file_name = file_name.lower()
        
        # Match MODEL_NAME dynamically using case-insensitive LIKE
        metadata_query = f"""
            SELECT MODEL_NAME 
            FROM {metadata_table_name}
            WHERE LOWER(''{normalized_file_name}'') LIKE LOWER(PATTERN)
            LIMIT 1
        """
        metadata_result = session.sql(metadata_query).collect()
        
        # Fallback to UNKNOWN if no match is found
        if metadata_result:
            model_name = metadata_result[0]["MODEL_NAME"]
        else:
            model_name = "UNKNOWN"
            session.sql(f"""
                UPDATE {prefilter_table_name}
                SET STATUS = ''SKIPPED'',
                    COMMENT = ''File skipped: Unknown file type.'',
                    MODEL_NAME = ''UNKNOWN''
                WHERE FILENAME = ''{file_name}''
            """).collect()
            continue  # Skip to the next file

        # Step 4: Skip files with size 0 bytes
        if file_size == 0:
            session.sql(f"""
                INSERT INTO {prefilter_table_name} (
                    MODEL_NAME, FILENAME, FILESIZE, NUMBER_OF_PAGES, DATECREATED, COMMENT, STATUS, PROCESS_START_TIME, PROCESS_END_TIME
                ) VALUES (
                    ''{model_name}'', ''{file_name}'', {file_size}, NULL, CURRENT_TIMESTAMP,
                    ''File size is 0 bytes.'', ''SKIPPED'', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
                )
            """).collect()
            continue

        try:
            # Step 5: Insert or update prefilter table for processing
            session.sql(f"""
                INSERT INTO {prefilter_table_name} (
                    MODEL_NAME, FILENAME, FILESIZE, NUMBER_OF_PAGES, DATECREATED, COMMENT, STATUS, PROCESS_START_TIME
                ) SELECT 
                    ''{model_name}'', ''{file_name}'', {file_size}, NULL, CURRENT_TIMESTAMP,
                    ''Validated Successfully'', ''NOT PROCESSED'', CURRENT_TIMESTAMP
                WHERE NOT EXISTS (
                    SELECT 1 FROM {prefilter_table_name} WHERE FILENAME = ''{file_name}''
                )
            """).collect()

            # Step 6: Download the file from the stage and count pages
            session.file.get(stage_file_path, temp_dir)
            local_file_path = os.path.join(temp_dir, file_name)
            
            if not os.path.exists(local_file_path):
                raise FileNotFoundError(f"File {file_name} not found.")

            with open(local_file_path, "rb") as file:
                pdf_reader = PyPDF2.PdfReader(file)
                total_pages = len(pdf_reader.pages)

            # Update the prefilter table with page count
            process_end_time = datetime.now()
            session.sql(f"""
                UPDATE {prefilter_table_name}
                SET 
                    NUMBER_OF_PAGES = {total_pages},
                    STATUS = ''NOT PROCESSED'',
                    COMMENT = ''Page count updated successfully.'',
                    PROCESS_END_TIME = CURRENT_TIMESTAMP
                WHERE FILENAME = ''{file_name}''
            """).collect()

        except Exception as e:
            # Handle errors during file processing
            process_end_time = datetime.now()
            session.sql(f"""
                UPDATE {prefilter_table_name}
                SET STATUS = ''ERROR'',
                    COMMENT = ''Processing failed: {str(e)}''
                WHERE FILENAME = ''{file_name}''
            """).collect()
        
        finally:
            # Clean up the local file
            if os.path.exists(local_file_path):
                os.remove(local_file_path)

        processed_count += 1

    # Step 7: Drop the temporary table
    session.sql(f"DROP TABLE IF EXISTS {full_temp_table_name}").collect()
    return f"Processed a total of {processed_count} files successfully."
';


CREATE OR REPLACE PROCEDURE DS_DEV_DB.DOC_AI_SCHEMA.HANDLE_PDF_FILES()
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '
from snowflake.snowpark.session import Session
from snowflake.snowpark.exceptions import SnowparkSQLException
from datetime import datetime

def main(session: Session) -> str:
    try:
        # Query to fetch all files with `NOT PROCESSED` status
        get_file_name_query = """
            SELECT 
                FILENAME, 
                ROWID, 
                CAST(FILESIZE AS FLOAT) AS FILESIZE, 
                CAST(NUMBER_OF_PAGES AS INT) AS NUMBER_OF_PAGES
            FROM DOCAI_PREFILTER
            WHERE STATUS = ''NOT PROCESSED''
        """
        
        # Execute the query
        rows = session.sql(get_file_name_query).collect()
        
        # Check if there are records to process
        if not rows:
            return "No files to process."

        for row in rows:
            file_name = row["FILENAME"]
            row_id = row["ROWID"]
            file_size_mb = row["FILESIZE"] / (1024 * 1024)  # Convert bytes to MB
            number_of_pages = row["NUMBER_OF_PAGES"]

            # Update status to IN PROGRESS
            session.sql(f"""
                UPDATE DOCAI_PREFILTER
                SET STATUS = ''IN PROGRESS'',
                    COMMENT = ''Processing in progress.''
                WHERE ROWID = ''{row_id}''
            """).collect()

            try:
                # Record the start time for processing
                process_start_time = session.sql("SELECT CURRENT_TIMESTAMP").collect()[0][0]

                # Query metadata table to determine model and prediction type
                metadata_query = f"""
                    SELECT MODEL_NAME, PREDICTION_TYPE
                    FROM DS_DEV_DB.DOC_AI_SCHEMA.MODEL_METADATA
                    WHERE LOWER(''{file_name}'') LIKE LOWER(PATTERN)
                    LIMIT 1
                """
                metadata_result = session.sql(metadata_query).collect()

                # If no matching metadata found, skip the file
                if not metadata_result:
                    session.sql(f"""
                        UPDATE DOCAI_PREFILTER
                        SET STATUS = ''SKIPPED'',
                            COMMENT = ''No matching metadata pattern found.'',
                            PROCESS_END_TIME = CURRENT_TIMESTAMP
                        WHERE ROWID = ''{row_id}''
                    """).collect()
                    continue

                # Extract model details
                model_name = metadata_result[0]["MODEL_NAME"]
                prediction_type = metadata_result[0]["PREDICTION_TYPE"]

                # Process files meeting criteria
                json_result = session.sql(f"""
                    SELECT DS_DEV_DB.DOC_AI_SCHEMA.{model_name}!PREDICT(
                        GET_PRESIGNED_URL(@INVOICE_DOCS, ''{file_name}''), {prediction_type}
                    )
                """).collect()[0][0]

                # Insert into extraction table
                session.sql(f"""
                    INSERT INTO DocAI_OrderForm_Extraction (
                        RelativePath, Model_Name, Size, File_Url, JSON, Comments, Status, PROCESS_START_TIME, PROCESS_END_TIME
                    )
                    SELECT 
                        ''{file_name}'' AS RelativePath, 
                        ''{model_name}'' AS Model_Name,
                        {row["FILESIZE"]} AS Size, 
                        GET_PRESIGNED_URL(@INVOICE_DOCS, ''{file_name}'') AS File_Url, 
                        PARSE_JSON(''{json_result}'') AS JSON,
                        ''File processed successfully.'' AS Comments,
                        ''NOT PROCESSED'' AS Status,
                        ''{process_start_time}'' AS PROCESS_START_TIME,
                        CURRENT_TIMESTAMP AS PROCESS_END_TIME
                """).collect()

                # Update status to PROCESSED
                session.sql(f"""
                    UPDATE DOCAI_PREFILTER
                    SET STATUS = ''PROCESSED'',
                        COMMENT = ''File processed and moved to DocAI_OrderForm_Extraction.''
                    WHERE ROWID = ''{row_id}''
                """).collect()

            except Exception as e:
                # Log error in extraction table
                session.sql(f"""
                    INSERT INTO DocAI_OrderForm_Extraction (
                        RelativePath, Model_Name, Size, File_Url, JSON, Comments, Status, PROCESS_START_TIME, PROCESS_END_TIME
                    )
                    SELECT 
                        ''{file_name}'' AS RelativePath, 
                        NULL AS Model_Name,
                        {row["FILESIZE"]} AS Size, 
                        NULL AS File_Url, 
                        NULL AS JSON, 
                        ''Processing failed: {str(e)}'' AS Comments,
                        ''ERROR'' AS Status,
                        ''{process_start_time}'' AS PROCESS_START_TIME,
                        CURRENT_TIMESTAMP AS PROCESS_END_TIME
                    """).collect()
                raise e

        return "Files processed successfully."

    except SnowparkSQLException as e:
        return f"SQL Error: {str(e)}"
    except Exception as e:
        return f"General Error: {str(e)}"
';

CREATE OR REPLACE PROCEDURE DS_DEV_DB.DOC_AI_SCHEMA.MANAGE_MANUAL_REVIEW_FILES()

RETURNS VARCHAR(16777216)

LANGUAGE JAVASCRIPT

EXECUTE AS CALLER

AS '

try {

    // Create temporary table for file tracking

    var create_temp_table = `

        CREATE OR REPLACE TEMPORARY TABLE temp_files_to_move AS

        SELECT FILENAME

        FROM DOCAI_PREFILTER

        WHERE STATUS = ''MANUAL REVIEW''`;

    snowflake.execute({ sqlText: create_temp_table });

    // Check if there are any files to process

    var check_files = `SELECT COUNT(*) AS file_count FROM temp_files_to_move`;

    var files_result = snowflake.execute({ sqlText: check_files });

    files_result.next();

    var file_count = files_result.getColumnValue(1);

    if (file_count === 0) {

        return "No files found with MANUAL_REVIEW status";

    }

    // Get the pattern string for file operations

    var get_pattern = `SELECT LISTAGG(FILENAME, ''|'') FROM temp_files_to_move`;

    var pattern_result = snowflake.execute({ sqlText: get_pattern });

    pattern_result.next();

    var file_pattern = pattern_result.getColumnValue(1);

    // Create the complete pattern string in JavaScript

    var complete_pattern = ''.*('' + file_pattern + '').*'';

    // Copy files to Manual_Review stage

    var copy_files = `COPY FILES INTO @Manual_Review 

                      FROM @INVOICE_DOCS

                      PATTERN = ''${complete_pattern}''`;

    snowflake.execute({ sqlText: copy_files });

    // Remove files from source stage

    var remove_files = `REMOVE @INVOICE_DOCS 

                       PATTERN = ''${complete_pattern}''`;

    snowflake.execute({ sqlText: remove_files });

    // Update status to Failed

    var update_status = `UPDATE DOCAI_PREFILTER

                        SET STATUS = ''FAILED''

                        WHERE STATUS = ''MANUAL REVIEW''

                        AND FILENAME IN (SELECT FILENAME FROM temp_files_to_move)`;

    var update_result = snowflake.execute({ sqlText: update_status });

    // Clean up temporary table

    var cleanup = `DROP TABLE IF EXISTS temp_files_to_move`;

    snowflake.execute({ sqlText: cleanup });

    return `Successfully processed ${file_count} files`;

} catch (err) {

    // Clean up temporary table in case of error

    try {

        snowflake.execute({ sqlText: `DROP TABLE IF EXISTS temp_files_to_move` });

    } catch (cleanup_err) {

        // Ignore cleanup errors

    }

    return `Failed to process files: ${err}`;

}

';


CREATE OR REPLACE PROCEDURE DS_DEV_DB.DOC_AI_SCHEMA.LOAD_INVOICE_FLATTEN()
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS
$$
import json
from snowflake.snowpark.session import Session

class InvoiceProcessor:

    def __init__(self, session):
        self.session = session
        self.total_processed = 0
        self.total_failed = 0

    def check_not_processed_status(self):
        """
        Check if there are records with STATUS = 'NOT PROCESSED' in the DocAI_OrderForm_Extraction table.
        """
        status_query = """
            SELECT COUNT(*) AS COUNT
            FROM DS_DEV_DB.DOC_AI_SCHEMA.DocAI_OrderForm_Extraction
            WHERE STATUS = 'NOT PROCESSED' AND MODEL_NAME = 'INVOICE_MODEL';
        """
        result = self.session.sql(status_query).collect()
        return result[0]['COUNT'] > 0 if result else False

    def load_thresholds(self):
        threshold_query = """
            SELECT SCORE_NAME, CAST(SCORE_VALUE AS FLOAT) AS SCORE_VALUE
            FROM DS_DEV_DB.DOC_AI_SCHEMA.SCORE_THRESHOLD
            WHERE MODEL_NAME = 'INVOICE_MODEL'
        """
        thresholds = self.session.sql(threshold_query).collect()
        if not thresholds:
            raise ValueError("No thresholds found for the INVOICE_MODEL.")
        return {row['SCORE_NAME']: row['SCORE_VALUE'] for row in thresholds}

    def load_table_schema(self):
        schema_query = "DESCRIBE TABLE DS_DEV_DB.DOC_AI_SCHEMA.Invoice_Flatten"
        schema = self.session.sql(schema_query).collect()
        return {row['name'].upper() for row in schema}

    def insert_failed_history(self, filename, score_name, score_value, comments):
        insert_query = f"""
            INSERT INTO DS_DEV_DB.DOC_AI_SCHEMA.invoice_col_score_failed_history (
                score_name, 
                score_value, 
                date_failed, 
                filename, 
                comments
            ) VALUES (
                '{score_name.replace("'", "''")}', 
                {score_value}, 
                CURRENT_TIMESTAMP, 
                '{filename.replace("'", "''")}', 
                '{comments.replace("'", "''")}')
        """
        self.session.sql(insert_query).collect()

    def insert_flatten_table(self, record, json_data, ocr_score, failed_fields, start_time, end_time):
        insert_columns = ["RELATIVEPATH", "MODEL_NAME", "OCR_SCORE"]
        insert_values = [record["RELATIVEPATH"], "INVOICE_MODEL", ocr_score]

        for field, threshold in self.thresholds.items():
            if field == "ocrScore":
                continue
            field_score_col = f"{field.upper()}_SCORE"
            field_value_col = f"{field.upper()}_VALUE"
            if field_score_col in self.valid_columns and field_value_col in self.valid_columns:
                field_data = json_data.get(field, [])
                score = field_data[0].get("score", 0) if field_data else 0
                concatenated_values = ", ".join(
                    [item.get("value", "").replace("'", "''") for item in field_data]
                )
                insert_columns.extend([field_score_col, field_value_col])
                insert_values.extend([score, concatenated_values])

        status = "FAILED" if failed_fields else "PROCESSED"
        comments = f"Failed: {', '.join(failed_fields)}" if failed_fields else "All scores passed"

        insert_columns.extend(["STATUS", "COMMENTS", "PROCESSED_TIMESTAMP", "PROCESS_START_TIME", "PROCESS_END_TIME"])
        insert_values.extend([status, comments, "CURRENT_TIMESTAMP()", start_time, end_time])

        columns = ", ".join(insert_columns)
        placeholders = ", ".join(
            ["?" if val != "CURRENT_TIMESTAMP()" else "CURRENT_TIMESTAMP()" for val in insert_values]
        )
        insert_query = f"INSERT INTO DS_DEV_DB.DOC_AI_SCHEMA.Invoice_Flatten ({columns}) VALUES ({placeholders})"
        self.session.sql(insert_query, [val for val in insert_values if val != "CURRENT_TIMESTAMP()"]).collect()

    def update_extraction_status(self, relative_path, status):
        update_query = f"""
            UPDATE DS_DEV_DB.DOC_AI_SCHEMA.DocAI_OrderForm_Extraction
            SET STATUS = '{status}'
            WHERE RELATIVEPATH = '{relative_path.replace("'", "''")}'
        """
        self.session.sql(update_query).collect()

    def process_record(self, record):
        try:
            json_data = json.loads(record['JSON'])
            self.thresholds = self.load_thresholds()
            self.valid_columns = self.load_table_schema()

            process_start_time = self.session.sql("SELECT CURRENT_TIMESTAMP").collect()[0][0]
            ocr_score = float(json_data.get("__documentMetadata", {}).get("ocrScore", 0))

            failed_fields = []
            if ocr_score < self.thresholds.get("ocrScore", 0):
                failed_fields.append("OCR_Score")
                self.insert_failed_history(record["RELATIVEPATH"], "OCR_Score", ocr_score, "OCR_Score failed validation")

            for field, threshold in self.thresholds.items():
                if field == "ocrScore":
                    continue
                field_data = json_data.get(field, [])
                score = field_data[0].get("score", 0) if field_data else 0
                if score < threshold:
                    failed_fields.append(field)
                    self.insert_failed_history(
                        record["RELATIVEPATH"], field, score, f"{field} failed validation"
                    )

            process_end_time = self.session.sql("SELECT CURRENT_TIMESTAMP").collect()[0][0]

            self.insert_flatten_table(record, json_data, ocr_score, failed_fields, process_start_time, process_end_time)

            self.update_extraction_status(record["RELATIVEPATH"], "PROCESSED")
            self.total_processed += 1 if not failed_fields else 0
            self.total_failed += 1 if failed_fields else 0

        except Exception as e:
            print(f"Error processing record {record['RELATIVEPATH']}: {str(e)}")
            self.update_extraction_status(record["RELATIVEPATH"], "ERROR")
            self.total_failed += 1

    def process_all_records(self):
        records_query = """
            SELECT RELATIVEPATH, JSON
            FROM DS_DEV_DB.DOC_AI_SCHEMA.DocAI_OrderForm_Extraction
            WHERE STATUS = 'NOT PROCESSED' AND MODEL_NAME = 'INVOICE_MODEL';
        """
        records = self.session.sql(records_query).collect()
        if not records:
            return "No records to process."

        for record in records:
            self.process_record(record)

        return f"Processed {self.total_processed} records. Failed to process {self.total_failed} records."

def main(session: Session) -> str:
    processor = InvoiceProcessor(session)

    # Check for "NOT PROCESSED" status
    if not processor.check_not_processed_status():
        return "No records with 'NOT PROCESSED' status. Procedure terminated."

    return processor.process_all_records()
$$;



CREATE OR REPLACE PROCEDURE DS_DEV_DB.DOC_AI_SCHEMA.LOAD_PURCHASE_FLATTEN()
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS
$$
import json
from snowflake.snowpark.session import Session

class InvoiceProcessor:

    def __init__(self, session):
        self.session = session
        self.total_processed = 0
        self.total_failed = 0

    def check_not_processed_status(self):
        """
        Check if there are records with STATUS = 'NOT PROCESSED' in the DocAI_OrderForm_Extraction table.
        """
        status_query = """
            SELECT COUNT(*) AS COUNT
            FROM DS_DEV_DB.DOC_AI_SCHEMA.DocAI_OrderForm_Extraction
            WHERE STATUS = 'NOT PROCESSED' AND MODEL_NAME = 'PURCHASE_MODEL';
        """
        result = self.session.sql(status_query).collect()
        return result[0]['COUNT'] > 0 if result else False

    def load_thresholds(self):
        threshold_query = """
            SELECT SCORE_NAME, CAST(SCORE_VALUE AS FLOAT) AS SCORE_VALUE
            FROM DS_DEV_DB.DOC_AI_SCHEMA.SCORE_THRESHOLD
            WHERE MODEL_NAME = 'PURCHASE_MODEL'
        """
        thresholds = self.session.sql(threshold_query).collect()
        if not thresholds:
            raise ValueError("No thresholds found for the PURCHASE_MODEL.")
        return {row['SCORE_NAME']: row['SCORE_VALUE'] for row in thresholds}

    def load_table_schema(self):
        schema_query = "DESCRIBE TABLE DS_DEV_DB.DOC_AI_SCHEMA.Purchase_Flatten"
        schema = self.session.sql(schema_query).collect()
        return {row['name'].upper() for row in schema}

    def insert_failed_history(self, filename, score_name, score_value, comments):
        insert_query = f"""
            INSERT INTO DS_DEV_DB.DOC_AI_SCHEMA.purchase_col_score_failed_history (
                score_name, 
                score_value, 
                date_failed, 
                filename, 
                comments
            ) VALUES (
                '{score_name.replace("'", "''")}', 
                {score_value}, 
                CURRENT_TIMESTAMP, 
                '{filename.replace("'", "''")}', 
                '{comments.replace("'", "''")}')
        """
        self.session.sql(insert_query).collect()

    def insert_flatten_table(self, record, json_data, ocr_score, failed_fields, start_time, end_time):
        insert_columns = ["RELATIVEPATH", "MODEL_NAME", "OCR_SCORE"]
        insert_values = [record["RELATIVEPATH"], "PURCHASE_MODEL", ocr_score]

        for field, threshold in self.thresholds.items():
            if field == "ocrScore":
                continue
            field_score_col = f"{field.upper()}_SCORE"
            field_value_col = f"{field.upper()}_VALUE"
            if field_score_col in self.valid_columns and field_value_col in self.valid_columns:
                field_data = json_data.get(field, [])
                score = field_data[0].get("score", 0) if field_data else 0
                concatenated_values = ", ".join(
                    [item.get("value", "").replace("'", "''") for item in field_data]
                )
                insert_columns.extend([field_score_col, field_value_col])
                insert_values.extend([score, concatenated_values])

        status = "FAILED" if failed_fields else "PROCESSED"
        comments = f"Failed: {', '.join(failed_fields)}" if failed_fields else "All scores passed"

        insert_columns.extend(["STATUS", "COMMENTS", "PROCESSED_TIMESTAMP", "PROCESS_START_TIME", "PROCESS_END_TIME"])
        insert_values.extend([status, comments, "CURRENT_TIMESTAMP()", start_time, end_time])

        columns = ", ".join(insert_columns)
        placeholders = ", ".join(
            ["?" if val != "CURRENT_TIMESTAMP()" else "CURRENT_TIMESTAMP()" for val in insert_values]
        )
        insert_query = f"INSERT INTO DS_DEV_DB.DOC_AI_SCHEMA.Purchase_Flatten ({columns}) VALUES ({placeholders})"
        self.session.sql(insert_query, [val for val in insert_values if val != "CURRENT_TIMESTAMP()"]).collect()

    def update_extraction_status(self, relative_path, status):
        update_query = f"""
            UPDATE DS_DEV_DB.DOC_AI_SCHEMA.DocAI_OrderForm_Extraction
            SET STATUS = '{status}'
            WHERE RELATIVEPATH = '{relative_path.replace("'", "''")}'
        """
        self.session.sql(update_query).collect()

    def process_record(self, record):
        try:
            json_data = json.loads(record['JSON'])
            self.thresholds = self.load_thresholds()
            self.valid_columns = self.load_table_schema()

            process_start_time = self.session.sql("SELECT CURRENT_TIMESTAMP").collect()[0][0]
            ocr_score = float(json_data.get("__documentMetadata", {}).get("ocrScore", 0))

            failed_fields = []
            if ocr_score < self.thresholds.get("ocrScore", 0):
                failed_fields.append("OCR_Score")
                self.insert_failed_history(record["RELATIVEPATH"], "OCR_Score", ocr_score, "OCR_Score failed validation")

            for field, threshold in self.thresholds.items():
                if field == "ocrScore":
                    continue
                field_data = json_data.get(field, [])
                score = field_data[0].get("score", 0) if field_data else 0
                if score < threshold:
                    failed_fields.append(field)
                    self.insert_failed_history(
                        record["RELATIVEPATH"], field, score, f"{field} failed validation"
                    )

            process_end_time = self.session.sql("SELECT CURRENT_TIMESTAMP").collect()[0][0]

            self.insert_flatten_table(record, json_data, ocr_score, failed_fields, process_start_time, process_end_time)

            self.update_extraction_status(record["RELATIVEPATH"], "PROCESSED")
            self.total_processed += 1 if not failed_fields else 0
            self.total_failed += 1 if failed_fields else 0

        except Exception as e:
            print(f"Error processing record {record['RELATIVEPATH']}: {str(e)}")
            self.update_extraction_status(record["RELATIVEPATH"], "ERROR")
            self.total_failed += 1

    def process_all_records(self):
        records_query = """
            SELECT RELATIVEPATH, JSON
            FROM DS_DEV_DB.DOC_AI_SCHEMA.DocAI_OrderForm_Extraction
            WHERE STATUS = 'NOT PROCESSED' AND MODEL_NAME = 'PURCHASE_MODEL';
        """
        records = self.session.sql(records_query).collect()
        if not records:
            return "No records to process."

        for record in records:
            self.process_record(record)

        return f"Processed {self.total_processed} records. Failed to process {self.total_failed} records."

def main(session: Session) -> str:
    processor = InvoiceProcessor(session)

    # Check for "NOT PROCESSED" status
    if not processor.check_not_processed_status():
        return "No records with 'NOT PROCESSED' status. Procedure terminated."

    return processor.process_all_records()
$$;


CREATE OR REPLACE PROCEDURE DS_DEV_DB.DOC_AI_SCHEMA.LOAD_INVOICE_VALIDATED()
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS
$$
def main(session):
    try:
        total_processed = 0

        # Fetch all processed records from Invoice_Flatten
        select_query = """
            SELECT 
                RELATIVEPATH,
                DELIVERY_DATE_VALUE,
                ORDER_DATE_VALUE,
                PO_NUMBER_VALUE,
                SUB_TOTAL_VALUE,
                TAX_AMOUNT_VALUE,
                TOTAL_AMOUNT_VALUE,
                PROCESS_START_TIME,
                PROCESS_END_TIME
            FROM DS_DEV_DB.DOC_AI_SCHEMA.Invoice_Flatten
            WHERE STATUS = 'PROCESSED'
        """
        rows_to_insert = session.sql(select_query).collect()

        # If no records to process, exit
        if not rows_to_insert:
            return "No records to process from Invoice_Flatten."

        print(f"Found {len(rows_to_insert)} records to process.")

        for record in rows_to_insert:
            try:
                # Insert into Invoice_Validated
                insert_query = f"""
                    INSERT INTO DS_DEV_DB.DOC_AI_SCHEMA.Invoice_Validated (
                        RELATIVEPATH,
                        DELIVERY_DATE_VALUE,
                        ORDER_DATE_VALUE,
                        PO_NUMBER_VALUE,
                        SUB_TOTAL_VALUE,
                        TAX_AMOUNT_VALUE,
                        TOTAL_AMOUNT_VALUE,
                        PROCESS_START_TIME,
                        PROCESS_END_TIME
                    ) VALUES (
                        '{record["RELATIVEPATH"].replace("'", "''")}',
                        '{record["DELIVERY_DATE_VALUE"].replace("'", "''") if record["DELIVERY_DATE_VALUE"] else ""}',
                        '{record["ORDER_DATE_VALUE"].replace("'", "''") if record["ORDER_DATE_VALUE"] else ""}',
                        '{record["PO_NUMBER_VALUE"].replace("'", "''") if record["PO_NUMBER_VALUE"] else ""}',
                        '{record["SUB_TOTAL_VALUE"].replace("'", "''") if record["SUB_TOTAL_VALUE"] else ""}',
                        '{record["TAX_AMOUNT_VALUE"].replace("'", "''") if record["TAX_AMOUNT_VALUE"] else ""}',
                        '{record["TOTAL_AMOUNT_VALUE"].replace("'", "''") if record["TOTAL_AMOUNT_VALUE"] else ""}',
                        '{record["PROCESS_START_TIME"]}',
                        '{record["PROCESS_END_TIME"]}'
                    )
                """
                session.sql(insert_query).collect()

                # Update the record in Invoice_Flatten
                update_query = f"""
                    UPDATE DS_DEV_DB.DOC_AI_SCHEMA.Invoice_Flatten
                    SET STATUS = 'VALIDATED'
                    WHERE RELATIVEPATH = '{record["RELATIVEPATH"].replace("'", "''")}'
                """
                session.sql(update_query).collect()

                total_processed += 1

            except Exception as e:
                # Handle errors and update the record status
                error_message = f"Error processing record {record['RELATIVEPATH']}: {str(e)}"
                print(error_message)

                update_error_query = f"""
                    UPDATE DS_DEV_DB.DOC_AI_SCHEMA.Invoice_Flatten
                    SET STATUS = 'ERROR',
                        COMMENTS = '{error_message.replace("'", "''")}'
                    WHERE RELATIVEPATH = '{record["RELATIVEPATH"].replace("'", "''")}'
                """
                session.sql(update_error_query).collect()

        return f"Successfully validated {total_processed} records."

    except Exception as e:
        return f"Procedure failed: {str(e)}"
$$;


CREATE OR REPLACE PROCEDURE DS_DEV_DB.DOC_AI_SCHEMA.LOAD_PURCHASE_VALIDATED()
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS
$$
def main(session):
    try:
        total_processed = 0

        # Fetch all processed records from Purchase_Flatten
        select_query = """
            SELECT 
                RELATIVEPATH,
                AMOUNT_VALUE,
                DISCOUNT_VALUE,
                INVOICE_DATE_VALUE,
                INV_NUMBER_VALUE,
                ITEM_DESCRIPTIONS_VALUE,
                PO_NUMBER_VALUE,
                QUANTITY_VALUE,
                RATE_VALUE
            FROM DS_DEV_DB.DOC_AI_SCHEMA.Purchase_Flatten
            WHERE STATUS = 'PROCESSED'
            ORDER BY RELATIVEPATH;
        """
        rows_to_insert = session.sql(select_query).collect()

        # If no records to process, exit
        if not rows_to_insert:
            return "No records to process from Purchase_Flatten."

        print(f"Found {len(rows_to_insert)} records to process.")

        for record in rows_to_insert:
            try:
                # Start processing time
                process_start_time = session.sql("SELECT CURRENT_TIMESTAMP").collect()[0][0]

                # Helper function to handle NULL values
                def handle_null(value):
                    return value.replace("'", "''") if value else ""

                # Insert into Purchase_Validated
                insert_query = f"""
                    INSERT INTO DS_DEV_DB.DOC_AI_SCHEMA.Purchase_Validated (
                        RELATIVEPATH,
                        AMOUNT_VALUE,
                        DISCOUNT_VALUE,
                        INVOICE_DATE_VALUE,
                        INV_NUMBER_VALUE,
                        ITEM_DESCRIPTIONS_VALUE,
                        PO_NUMBER_VALUE,
                        QUANTITY_VALUE,
                        RATE_VALUE,
                        PROCESS_START_TIME,
                        PROCESS_END_TIME
                    ) VALUES (
                        '{handle_null(record["RELATIVEPATH"])}',
                        '{handle_null(record["AMOUNT_VALUE"])}',
                        '{handle_null(record["DISCOUNT_VALUE"])}',
                        '{handle_null(record["INVOICE_DATE_VALUE"])}',
                        '{handle_null(record["INV_NUMBER_VALUE"])}',
                        '{handle_null(record["ITEM_DESCRIPTIONS_VALUE"])}',
                        '{handle_null(record["PO_NUMBER_VALUE"])}',
                        '{handle_null(record["QUANTITY_VALUE"])}',
                        '{handle_null(record["RATE_VALUE"])}',
                        '{process_start_time}',
                        CURRENT_TIMESTAMP
                    )
                """
                session.sql(insert_query).collect()

                # Update the record in Purchase_Flatten
                update_query = f"""
                    UPDATE DS_DEV_DB.DOC_AI_SCHEMA.Purchase_Flatten
                    SET STATUS = 'VALIDATED'
                    WHERE RELATIVEPATH = '{handle_null(record["RELATIVEPATH"])}';
                """
                session.sql(update_query).collect()

                total_processed += 1

            except Exception as e:
                # Handle errors and update the record status
                error_message = f"Error processing record {record['RELATIVEPATH']}: {str(e)}"
                print(error_message)

                update_error_query = f"""
                    UPDATE DS_DEV_DB.DOC_AI_SCHEMA.Purchase_Flatten
                    SET STATUS = 'ERROR',
                        COMMENTS = '{error_message.replace("'", "''")}'
                    WHERE RELATIVEPATH = '{handle_null(record["RELATIVEPATH"])}';
                """
                session.sql(update_error_query).collect()

        return f"Successfully validated {total_processed} records."

    except Exception as e:
        return f"Procedure failed: {str(e)}"
$$;


CREATE OR REPLACE PROCEDURE DS_DEV_DB.DOC_AI_SCHEMA.MOVEFILES_TO_SOURCE(selected_file VARCHAR)
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
try {
    // Fetch the selected_file parameter
    var selectedFile = arguments[0];

    // Escape special characters in the filename for regex
    var escapedFileName = selectedFile.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");

    // Create a regex pattern for the file
    var completePattern = `.*${escapedFileName}.*`;

    // Copy the file from manual_review stage to INVOICE_DOCS stage
    var copy_file_query = `
        COPY FILES INTO @INVOICE_DOCS
        FROM @manual_review
        PATTERN = ''${completePattern}''`;
    snowflake.execute({ sqlText: copy_file_query });

    // Remove the file from manual_review stage
    var remove_file_query = `
        REMOVE @manual_review
        PATTERN = ''${completePattern}''`;
    snowflake.execute({ sqlText: remove_file_query });

    // Return success message
    return `File "${selectedFile}" successfully moved from manual_review to INVOICE_DOCS stage and removed from the database.`;
} catch (err) {
    return `Failed to process file "${selectedFile}": ${err}`;
}
';


CREATE OR REPLACE PROCEDURE DS_DEV_DB.DOC_AI_SCHEMA.IGNOREFILES_TO_STAGE(selected_file VARCHAR)
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
try {
    // Fetch the selected_file parameter
    var selectedFile = arguments[0];

    // Escape special characters in the filename for regex
    var escapedFileName = selectedFile.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");

    // Create a regex pattern for the file
    var completePattern = `.*${escapedFileName}.*`;

    // Copy the file from manual_review stage to orderform_doc stage
    var copy_file_query = `
        COPY FILES INTO @IGNORED_DOCS
        FROM @manual_review
        PATTERN = ''${completePattern}''`;
    snowflake.execute({ sqlText: copy_file_query });

    // Remove the file from manual_review stage
    var remove_file_query = `
        REMOVE @manual_review
        PATTERN = ''${completePattern}''`;
    snowflake.execute({ sqlText: remove_file_query });

    // Return success message
    return `File "${selectedFile}" successfully moved from manual_review to orderform_doc stage and removed from the database.`;
} catch (err) {
    return `Failed to process file "${selectedFile}": ${err}`;
}
';

select * from SCORE_THRESHOLD;
select * from MODEL_METADATA;

create or replace task DS_DEV_DB.DOC_AI_SCHEMA.EXTRACT_DATA
	warehouse=DS_DEV_WH
	schedule='1 MINUTE'
	as BEGIN
    CALL Handle_PDF_Files();
END;


CREATE OR REPLACE TASK DS_DEV_DB.DOC_AI_SCHEMA.FLATTEN_DATA
WAREHOUSE = DS_DEV_WH
AFTER DS_DEV_DB.DOC_AI_SCHEMA.EXTRACT_DATA
AS
BEGIN
    -- Call procedures for both models
    CALL DS_DEV_DB.DOC_AI_SCHEMA.LOAD_INVOICE_FLATTEN();
    CALL DS_DEV_DB.DOC_AI_SCHEMA.LOAD_PURCHASE_FLATTEN();
END;

CREATE OR REPLACE TASK DS_DEV_DB.DOC_AI_SCHEMA.VALIDATE_DATA
WAREHOUSE = DS_DEV_WH
AFTER DS_DEV_DB.DOC_AI_SCHEMA.FLATTEN_DATA
AS
BEGIN
    -- Call procedures for both models
    CALL DS_DEV_DB.DOC_AI_SCHEMA.LOAD_INVOICE_VALIDATED();
    CALL DS_DEV_DB.DOC_AI_SCHEMA.LOAD_PURCHASE_VALIDATED();
END;


create or replace task DS_DEV_DB.DOC_AI_SCHEMA.PREPROCESSING
	warehouse=DS_DEV_WH
	schedule='1 MINUTE'
	when SYSTEM$STREAM_HAS_DATA('PREPROCESS_STREAM')
	as BEGIN
    CALL COUNT_PDF_PAGES_PROC();
END;

-- Resume the Flatten Data Task
ALTER TASK DS_DEV_DB.DOC_AI_SCHEMA.FLATTEN_DATA RESUME;

-- Resume the Validate Data Task
ALTER TASK DS_DEV_DB.DOC_AI_SCHEMA.VALIDATE_DATA RESUME;

-- Resume the Preprocessing Task
ALTER TASK DS_DEV_DB.DOC_AI_SCHEMA.PREPROCESSING RESUME;