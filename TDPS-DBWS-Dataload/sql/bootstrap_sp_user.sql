-- Run this in the target database as the configured Microsoft Entra admin.
-- This script creates the Azure SQL user for the service principal if it does not exist
-- and grants the baseline reader and writer roles used by the smoke path.
-- Update @principal_name for the environment you are bootstrapping.

DECLARE @principal_name sysname = N'cgbdaprgsqls0tdpsxx01';

IF NOT EXISTS (
    SELECT 1
    FROM sys.database_principals
    WHERE name = @principal_name
)
BEGIN
    EXEC(N'CREATE USER [' + REPLACE(@principal_name, ']', ']]') + N'] FROM EXTERNAL PROVIDER;');
END;

IF NOT EXISTS (
    SELECT 1
    FROM sys.database_role_members drm
    INNER JOIN sys.database_principals role_principal
        ON drm.role_principal_id = role_principal.principal_id
    INNER JOIN sys.database_principals member_principal
        ON drm.member_principal_id = member_principal.principal_id
    WHERE role_principal.name = 'db_datareader'
            AND member_principal.name = @principal_name
)
BEGIN
        EXEC(N'ALTER ROLE db_datareader ADD MEMBER [' + REPLACE(@principal_name, ']', ']]') + N'];');
END;

IF NOT EXISTS (
    SELECT 1
    FROM sys.database_role_members drm
    INNER JOIN sys.database_principals role_principal
        ON drm.role_principal_id = role_principal.principal_id
    INNER JOIN sys.database_principals member_principal
        ON drm.member_principal_id = member_principal.principal_id
    WHERE role_principal.name = 'db_datawriter'
            AND member_principal.name = @principal_name
)
BEGIN
        EXEC(N'ALTER ROLE db_datawriter ADD MEMBER [' + REPLACE(@principal_name, ']', ']]') + N'];');
END;

SELECT SUSER_SNAME() AS executing_identity;

SELECT name, type_desc, authentication_type_desc
FROM sys.database_principals
WHERE name = @principal_name;
GO