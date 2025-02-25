CREATE OR REPLACE TASK RULES
    WAREHOUSE = DATAWH
    SCHEDULE = '35 minutes'
AS
BEGIN
    UPDATE DATA_TEST.RAW_DATA.JSON_RAW
    SET
    RAW_DATA = OBJECT_INSERT(OBJECT_INSERT(RAW_DATA,'analyst','John Doe',true),surveyor','Jane Doe',true)
    WHERE RAW_DATA:business_unit = 'EAST';

    UPDATE DATA_TEST.RAW_DATA.JSON_RAW
    SET
    RAW_DATA = OBJECT_INSERT(OBJECT_INSERT(RAW_DATA,'Status', 'Not Complete',true)
    WHERE RAW_DATA:readiness != '100%' or RAW_DATA:regulatory is null or RAW_DATA:compliance is null;

    UPDATE DATA_TEST.RAW_DATA.JSON_RAW
    SET
    RAW_DATA = OBJECT_INSERT(OBJECT_INSERT(RAW_DATA,'hiddenFields', PARSE_JSON('["hazard","risk","compliance"]'),true)
    WHERE RAW_DATA:business_unit = 'WEST';
    END;
    