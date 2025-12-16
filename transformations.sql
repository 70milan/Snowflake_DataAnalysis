-- Bronze to silver (parsing JSON content into structured columns)

CREATE OR REPLACE VIEW ELT_PROJECT.GOVDATA.SILVER_BORDER_CROSSING AS
SELECT
    -- Extract specific fields by their list index
    value[8]::STRING                    AS Port_Name,
    value[9]::STRING                    AS State,
    value[10]::STRING                   AS Port_Code,
    value[11]::STRING                   AS Border,
    value[12]::TIMESTAMP                AS Date,
    value[13]::STRING                   AS Measure,
    value[14]::INTEGER                  AS Value,
    value[15]::FLOAT                    AS Latitude,
    value[16]::FLOAT                    AS Longitude,
    value[17]::STRING                   AS Location_Point,
    
    -- Keep original metadata columns for lineage
    source_file,
    ingestion_time
FROM ELT_PROJECT.GOVDATA.BRONZE_BORDER,
LATERAL FLATTEN(input => content:data);