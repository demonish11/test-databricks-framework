-- Recommended transactional config shape for CAPS export.
-- Current wrapper logic supports per-entity JDBC settings and transform-time joins across
-- global_temp views, so this is best modeled as two extracts plus one composed transform.
-- Source 1: DRVPAY.CAPS_EXPORT from DTMSPB20.
-- Source 2: TMSUCL.FACILITY_ALIAS from DTMSDB20.
-- Final transform: join on FACILITY_ID = DS_FAC_ID to resolve DispatchAlias.

-- Source extract from DTMSPB20
SELECT
    CAST(ce.DS_FAC_ID AS NUMBER(38,0)) AS DS_FAC_ID,
    ce.CE_WK_ENDDT,
    CAST(ce.CEST_CD AS NUMBER(38,0)) AS CEST_CD,
    CAST(ce.CE_DRIVER_CNT AS NUMBER(38,0)) AS CE_DRIVER_CNT,
    CAST(ce.CE_TRIP_CNT AS NUMBER(38,0)) AS CE_TRIP_CNT,
    CAST(ce.CE_TOT_PAY_MINS AS NUMBER(9,2)) AS CE_TOT_PAY_MINS,
    CAST(ce.CE_TOP_DRIVER_MINS AS NUMBER(9,2)) AS CE_TOP_DRIVER_MINS,
    ce.CREATE_TS,
    ce.LAST_UPD_TS,
    ce.LAST_UPD_USERID,
    TO_CHAR(NVL(ce.LAST_UPD_TS, ce.CREATE_TS), 'YYYY-MM-DD HH24:MI:SS.FF6') AS LAST_UPDATED_DTTM
FROM DRVPAY.CAPS_EXPORT ce;


-- Lookup extract from DTMSDB20
SELECT
    FACILITY_ALIAS_ID,
    CAST(FACILITY_ID AS NUMBER(38,0)) AS FACILITY_ID
FROM TMSUCL.FACILITY_ALIAS
WHERE NVL(IS_PRIMARY, 0) = 1
  AND NVL(MARK_FOR_DELETION, 0) = 0;


-- Target-facing transform example.
-- ExportLogId is omitted because the Azure SQL target will generate it as IDENTITY.

SELECT
    CAST(alias.FACILITY_ALIAS_ID AS STRING) AS DispatchAlias,
    caps.CE_WK_ENDDT AS WeekEndDate,
    CASE TRY_CAST(caps.CEST_CD AS INT)
        WHEN 100 THEN 'SentToCAPs'
        WHEN 200 THEN 'ResentToCAPs'
        WHEN 300 THEN 'Fail'
        WHEN 400 THEN 'Preparing'
        WHEN 500 THEN 'InsertingInDB2'
        WHEN 600 THEN 'ReadyForPayroll'
        WHEN 700 THEN 'SentForPayroll'
        ELSE CAST(caps.CEST_CD AS STRING)
    END AS Status,
    CAST(caps.CE_DRIVER_CNT AS INT) AS DriverCount,
    CAST(caps.CE_TRIP_CNT AS INT) AS TripCount,
    CAST(caps.CE_TOT_PAY_MINS AS DECIMAL(9,2)) AS TotalPayMinutes,
    CAST(caps.CE_TOP_DRIVER_MINS AS DECIMAL(9,2)) AS TopDriverMinutes,
    caps.CREATE_TS AS CreatedDateTime,
    COALESCE(caps.LAST_UPD_TS, caps.CREATE_TS) AS UpdatedDateTime,
    CAST(caps.LAST_UPD_USERID AS STRING) AS UpdatedUserID
FROM global_temp.oracle_extract_transactional_capsexportsource_preview caps
LEFT JOIN global_temp.oracle_extract_transactional_facilityaliassource_preview alias
    ON caps.DS_FAC_ID = alias.FACILITY_ID;