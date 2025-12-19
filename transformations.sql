--405217


-- Bronze to silver (parsing JSON content into structured columns)


-- This table holds the flattened and cleaned data, ready for the Stream.
CREATE OR REPLACE TABLE ELT_PROJECT.GOVDATA.SILVER_BORDER_FLAT (
    PORT_NAME           VARCHAR,
    STATE_NAME          VARCHAR,
    PORT_CODE           VARCHAR, 
    BORDER_TYPE         VARCHAR,
    DATE_KEY            DATE, 
    MEASURE             VARCHAR, 
    VALUE               INTEGER,
    LATITUDE            FLOAT,
    LONGITUDE           FLOAT,
    LOCATION_TYPE       VARCHAR,
    LOCATION_POINT      GEOGRAPHY
)


-- The Stream is now created on the static table (SILVER_BORDER_FLAT)
CREATE OR REPLACE STREAM ELT_PROJECT.GOVDATA.BORDER_STREAM 
ON TABLE ELT_PROJECT.GOVDATA.SILVER_BORDER_FLAT 
SHOW_INITIAL_ROWS = FALSE; -- Set to FALSE because we just loaded it



-- Create a task to refresh the SILVER_BORDER_FLAT table from the BRONZE_BORDER table every 15 minutes
CREATE OR REPLACE TASK ELT_PROJECT.GOVDATA.REFRESH_SILVER_FLAT_TASK
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '11000 MINUTE'
AS
MERGE INTO ELT_PROJECT.GOVDATA.SILVER_BORDER_FLAT AS target
USING (
    -- We define exactly what we are bringing in here
    SELECT
      f.value[8]::STRING      AS PORT_NAME,
      f.value[9]::STRING      AS STATE_NAME,
      f.value[10]::STRING     AS PORT_CODE,
      f.value[11]::STRING     AS BORDER_TYPE,
      f.value[12]::DATE       AS DATE_KEY,
      f.value[13]::STRING     AS MEASURE,
      f.value[14]::INTEGER    AS VALUE,
      f.value[15]::FLOAT      AS LATITUDE,
      f.value[16]::FLOAT      AS LONGITUDE,
      TO_GEOGRAPHY('POINT(' || f.value[16]::STRING || ' ' || f.value[15]::STRING || ')') AS LOCATION_POINT
    FROM ELT_PROJECT.GOVDATA.BRONZE_BORDER b,
    LATERAL FLATTEN(input => b.content:"data") f
    WHERE f.value[10] IS NOT NULL
) AS source
ON  target.PORT_CODE = source.PORT_CODE 
AND target.MEASURE   = source.MEASURE 
AND target.DATE_KEY  = source.DATE_KEY

-- Only compare simple types (Strings, Dates, Integers)
WHEN MATCHED AND (target.VALUE != source.VALUE OR target.PORT_NAME != source.PORT_NAME) THEN
    UPDATE SET 
        target.VALUE = source.VALUE,
        target.LOCATION_POINT = source.LOCATION_POINT

-- Insert everything including geography
WHEN NOT MATCHED THEN
    INSERT (PORT_NAME, STATE_NAME, PORT_CODE, BORDER_TYPE, DATE_KEY, MEASURE, VALUE, LATITUDE, LONGITUDE, LOCATION_POINT)
    VALUES (source.PORT_NAME, source.STATE_NAME, source.PORT_CODE, source.BORDER_TYPE, source.DATE_KEY, source.MEASURE, source.VALUE, source.LATITUDE, source.LONGITUDE, source.LOCATION_POINT);