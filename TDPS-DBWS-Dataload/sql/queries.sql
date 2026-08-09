-- Initial Azure SQL stage tables for daily Driver and AdGroup loads.
-- These definitions follow the transformed column names and use dbo tables
-- with a stg_ prefix so the table names remain aligned with the staging role.
-- Adjust lengths or constraints once final target DDL is confirmed.

IF OBJECT_ID('dbo.stg_Driver', 'U') IS NOT NULL
    DROP TABLE dbo.stg_Driver;
GO

CREATE TABLE dbo.stg_Driver (
    DriverId INT NOT NULL,
    PersonnelNumber NVARCHAR(20) NOT NULL,
    FirstName NVARCHAR(25) NOT NULL,
    MiddleInitial NVARCHAR(25) NULL,
    LastName NVARCHAR(25) NOT NULL,
    OtherTitle NVARCHAR(15) NULL,
    IsPernrDriver BIT NOT NULL,
    HireDate DATE NULL,
    RehireDate DATE NULL,
    TerminationDate DATE NULL,
    IsActive BIT NOT NULL,
    CreatedDateTime DATETIME2(7) NOT NULL,
    UpdatedDateTime DATETIME2(7) NULL,
    UpdatedUserID NVARCHAR(10) NULL
);
GO

IF OBJECT_ID('dbo.stg_AdGroup', 'U') IS NOT NULL
    DROP TABLE dbo.stg_AdGroup;
GO
CREATE TABLE dbo.stg_AdGroup (
    AdGroupId INT NOT NULL,
    AdGroupName NVARCHAR(50) NOT NULL,
    IsActive BIT NOT NULL,
    CreatedDateTime DATETIME2(7) NOT NULL,
    UpdatedDateTime DATETIME2(7) NOT NULL,
    UpdatedUserID NVARCHAR(10) NULL
);
GO

IF OBJECT_ID('dbo.stg_Permissions', 'U') IS NOT NULL
    DROP TABLE dbo.stg_Permissions;
GO
CREATE TABLE dbo.stg_Permissions (
    PermissionId INT NOT NULL,
    DispatchAlias NVARCHAR(50) NOT NULL,
    AdGroupId INT NOT NULL,
    UiFunctionID INT NOT NULL,
    AllowRead BIT NOT NULL,
    AllowCreate BIT NOT NULL,
    AllowUpdate BIT NOT NULL,
    AllowDelete BIT NOT NULL,
    AllowPrint BIT NOT NULL,
    ElevatedAccess BIT NOT NULL,
    Active BIT NOT NULL,
    CreatedDateTime DATETIME2(7) NOT NULL,
    UpdatedDateTime DATETIME2(7) NOT NULL,
    UpdatedUserID NVARCHAR(10) NULL
);
GO

IF OBJECT_ID('dbo.stg_DelayType', 'U') IS NOT NULL
    DROP TABLE dbo.stg_DelayType;
GO

CREATE TABLE dbo.stg_DelayType (
    DelayTypeCode NVARCHAR(25) NOT NULL,
    Description NVARCHAR(25) NOT NULL,
    TransportationActivityType NVARCHAR(25) NOT NULL,
    ThresholdMinutes DECIMAL(5,2) NOT NULL,
    IsDeleted BIT NOT NULL,
    CreatedDateTime DATETIME2(7) NOT NULL,
    UpdatedDateTime DATETIME2(7) NOT NULL,
    UpdatedUserID NVARCHAR(10) NOT NULL
);
GO

IF OBJECT_ID('dbo.stg_DeliveryUnitType', 'U') IS NOT NULL
    DROP TABLE dbo.stg_DeliveryUnitType;
GO

CREATE TABLE dbo.stg_DeliveryUnitType (
    DeliveryUnitDescription NVARCHAR(100) NOT NULL,
    PluralDescription NVARCHAR(100) NULL,
    CreatedDateTime DATETIME2(7) NOT NULL,
    UpdatedDateTime DATETIME2(7) NOT NULL,
    UpdatedUserID NVARCHAR(10) NOT NULL
);
GO

IF OBJECT_ID('dbo.stg_Division', 'U') IS NOT NULL
    DROP TABLE dbo.stg_Division;
GO

CREATE TABLE dbo.stg_Division (
    DivisionId INT NOT NULL,
    DivisionName NVARCHAR(50) NOT NULL,
    CreatedDateTime DATETIME2(7) NULL,
    UpdatedDateTime DATETIME2(7) NULL,
    UpdatedUserID NVARCHAR(10) NULL
);
GO

IF OBJECT_ID('dbo.stg_DrivingStandard', 'U') IS NOT NULL
    DROP TABLE dbo.stg_DrivingStandard;
GO

CREATE TABLE dbo.stg_DrivingStandard (
    DrivingStandardId INT NOT NULL,
    DispatchAlias NVARCHAR(10) NOT NULL,
    ScheduleType NVARCHAR(20) NOT NULL,
    DistanceThreshold DECIMAL(7,2) NOT NULL,
    LegThreshold INT NOT NULL,
    MinutesPerMile DECIMAL(7,2) NOT NULL,
    AdjustmentMinutes DECIMAL(7,2) NULL,
    EffectiveStartDate DATE NOT NULL,
    EffectiveEndDate DATE NULL,
    CreatedDateTime DATETIME2(7) NOT NULL,
    UpdatedDateTime DATETIME2(7) NULL,
    UpdatedUserID NVARCHAR(10) NULL
);
GO

IF OBJECT_ID('dbo.stg_ScheduleType', 'U') IS NOT NULL
    DROP TABLE dbo.stg_ScheduleType;
GO

CREATE TABLE dbo.stg_ScheduleType (
    ScheduleTypeID INT NOT NULL,
    ScheduleTypeName NVARCHAR(50) NOT NULL,
    CreatedDateTime DATETIME2(7) NOT NULL,
    CreatedUserID NVARCHAR(10) NOT NULL
);
GO

IF OBJECT_ID('dbo.stg_UiFunctions', 'U') IS NOT NULL
    DROP TABLE dbo.stg_UiFunctions;
GO

CREATE TABLE dbo.stg_UiFunctions (
    UiFunctionID INT NOT NULL,
    UiFunctionName NVARCHAR(50) NOT NULL,
    CreatedDateTime DATETIME2(7) NOT NULL,
    UpdatedDateTime DATETIME2(7) NOT NULL,
    UpdatedUserID NVARCHAR(10) NOT NULL
);
GO

IF OBJECT_ID('dbo.stg_ScheduleDetails', 'U') IS NOT NULL
    DROP TABLE dbo.stg_ScheduleDetails;
GO

CREATE TABLE dbo.stg_ScheduleDetails (
    ScheduleDetailID INT NOT NULL,
    ScheduleTypeID INT NOT NULL,
    DayOfWeek INT NOT NULL,
    StartTime TIME(7) NOT NULL,
    EndTime TIME(7) NOT NULL,
    CreatedDateTime DATETIME2(7) NULL,
    UpdatedDateTime DATETIME2(7) NULL,
    UpdatedUserID NVARCHAR(10) NULL
);
GO

IF OBJECT_ID('dbo.stg_DrivingStandardAdjustmentGroup', 'U') IS NOT NULL
    DROP TABLE dbo.stg_DrivingStandardAdjustmentGroup;
GO

CREATE TABLE dbo.stg_DrivingStandardAdjustmentGroup (
    DrivingStandardGroupId INT NOT NULL,
    DispatchAlias NVARCHAR(50) NOT NULL,
    GroupName NVARCHAR(50) NOT NULL,
    AdjustmentMinutes DECIMAL(9,2) NOT NULL,
    EffectiveStartDate DATE NOT NULL,
    EffectiveEndDate DATE NULL,
    CreatedDateTime DATETIME2(7) NOT NULL,
    UpdatedDateTime DATETIME2(7) NULL,
    UpdatedUserID NVARCHAR(10) NULL
);
GO

IF OBJECT_ID('dbo.stg_DrivingStandardAdjustmentRoute', 'U') IS NOT NULL
    DROP TABLE dbo.stg_DrivingStandardAdjustmentRoute;
GO

CREATE TABLE dbo.stg_DrivingStandardAdjustmentRoute (
    DrivingStandardGroupId INT NOT NULL,
    OriginFacilityAlias NVARCHAR(50) NOT NULL,
    DestinationFacilityAlias NVARCHAR(50) NOT NULL
);
GO

IF OBJECT_ID('dbo.stg_DispatchGateFacility', 'U') IS NOT NULL
    DROP TABLE dbo.stg_DispatchGateFacility;
GO

CREATE TABLE dbo.stg_DispatchGateFacility (
    DispatchGateFacilityID INT NOT NULL,
    DispatchAlias NVARCHAR(50) NOT NULL,
    GateId INT NOT NULL,
    FacilityAlias NVARCHAR(20) NOT NULL,
    DivisionID INT NOT NULL,
    CreatedDateTime DATETIME2(7) NOT NULL,
    UpdatedDateTime DATETIME2(7) NOT NULL,
    UpdatedUserID NVARCHAR(10) NULL
);
GO

IF OBJECT_ID('dbo.stg_DriverAssignment', 'U') IS NOT NULL
    DROP TABLE dbo.stg_DriverAssignment;
GO

CREATE TABLE dbo.stg_DriverAssignment (
    DriverAssignmentId INT NOT NULL,
    DriverId INT NOT NULL,
    DispatchAlias NVARCHAR(10) NOT NULL,
    DriverNumber NVARCHAR(10) NOT NULL,
    DisplayNumber NVARCHAR(10) NOT NULL,
    DriverType NVARCHAR(50) NULL,
    EffectiveStartDate DATE NOT NULL,
    EffectiveEndDate DATE NOT NULL,
    CreatedDateTime DATETIME2(7) NOT NULL,
    UpdatedDateTime DATETIME2(7) NULL,
    UpdatedUserID NVARCHAR(10) NULL
);
GO

IF OBJECT_ID('dbo.stg_DispatchNonDrivingStandard', 'U') IS NOT NULL
    DROP TABLE dbo.stg_DispatchNonDrivingStandard;
GO

CREATE TABLE dbo.stg_DispatchNonDrivingStandard (
    DispatchAlias NVARCHAR(50) NOT NULL,
    BeginDayMinutes DECIMAL(9,2) NOT NULL,
    EndDayMinutes DECIMAL(9,2) NOT NULL,
    EffectiveStartDate DATE NOT NULL,
    EffectiveEndDate DATE NULL,
    CreatedDateTime DATETIME2(7) NOT NULL,
    UpdatedDateTime DATETIME2(7) NULL,
    UpdatedUserID NVARCHAR(50) NULL
);
GO

IF OBJECT_ID('dbo.stg_WarehouseNonDrivingStandard', 'U') IS NOT NULL
    DROP TABLE dbo.stg_WarehouseNonDrivingStandard;
GO

CREATE TABLE dbo.stg_WarehouseNonDrivingStandard (
    FacilityAlias NVARCHAR(50) NOT NULL,
    ActivityType NVARCHAR(20) NOT NULL,
    ActivityMinutes DECIMAL(9,2) NOT NULL,
    EffectiveStartDate DATE NOT NULL,
    EffectiveEndDate DATE NULL,
    CreatedDateTime DATETIME2(7) NOT NULL,
    UpdatedDateTime DATETIME2(7) NOT NULL,
    UpdatedUserID NVARCHAR(10) NOT NULL
);
GO

IF OBJECT_ID('dbo.stg_Vehicle', 'U') IS NOT NULL
    DROP TABLE dbo.stg_Vehicle;
GO

CREATE TABLE dbo.stg_Vehicle (
    VehicleId NVARCHAR(25) NOT NULL,
    VehicleType NVARCHAR(10) NULL,
    CreatedDateTime DATETIME2(7) NULL,
    UpdatedDateTime DATETIME2(7) NULL,
    UpdatedUserId NVARCHAR(10) NULL
);
GO

IF OBJECT_ID('dbo.stg_Dispatch', 'U') IS NOT NULL
    DROP TABLE dbo.stg_Dispatch;
GO

CREATE TABLE dbo.stg_Dispatch (
    DispatchAlias NVARCHAR(50) NOT NULL,
    DispatchCode NVARCHAR(20) NOT NULL,
    DispatchName NVARCHAR(100) NOT NULL,
    PayRate DECIMAL(9,2) NOT NULL,
    IsActive BIT NOT NULL,
    Email NVARCHAR(250) NULL,
    TimeZoneCode NVARCHAR(10) NOT NULL,
    CreatedDateTime DATETIME2(7) NOT NULL,
    UpdatedDateTime DATETIME2(7) NULL,
    UpdatedUserID NVARCHAR(10) NULL,
    Dept NVARCHAR(10) NULL,
    PoAbbr NVARCHAR(5) NULL
);
GO

IF OBJECT_ID('dbo.stg_RouteGroup', 'U') IS NOT NULL
    DROP TABLE dbo.stg_RouteGroup;
GO

CREATE TABLE dbo.stg_RouteGroup (
    RouteGroupId INT NOT NULL,
    Name NVARCHAR(25) NOT NULL,
    ProductCategory NVARCHAR(100) NOT NULL,
    SequenceNumber INT NULL,
    DispatchAlias NVARCHAR(50) NOT NULL,
    SourceSystemCode INT NOT NULL,
    SentFlag BIT NULL,
    SentDateTime DATETIME2(7) NULL,
    CreatedDateTime DATETIME2(7) NOT NULL,
    UpdatedDateTime DATETIME2(7) NOT NULL,
    UserId NVARCHAR(10) NOT NULL
);
GO

IF OBJECT_ID('dbo.stg_RouteStop', 'U') IS NOT NULL
    DROP TABLE dbo.stg_RouteStop;
GO

CREATE TABLE dbo.stg_RouteStop (
    RouteStopId INT NOT NULL,
    RouteID INT NOT NULL,
    StopSequence INT NOT NULL,
    FacilityAlias NVARCHAR(20) NOT NULL,
    EstimatedArrival DATETIME2(7) NOT NULL,
    EstimatedDeparture DATETIME2(7) NOT NULL,
    PlannedArrival DATETIME2(7) NULL,
    PlannedDeparture DATETIME2(7) NULL,
    DeliveryType NVARCHAR(10) NULL,
    BackhaulFlag BIT NULL,
    BackhaulTrailerId NVARCHAR(50) NULL,
    WaitTimeMins INT NULL,
    CleanOutFlag NVARCHAR(1) NULL,
    TrailerSealNumber NVARCHAR(10) NULL,
    CreatedDateTime DATETIME2(7) NOT NULL,
    UpdatedDateTime DATETIME2(7) NOT NULL,
    UserId NVARCHAR(10) NOT NULL
);
GO

IF OBJECT_ID('dbo.stg_Route', 'U') IS NOT NULL
    DROP TABLE dbo.stg_Route;
GO

CREATE TABLE dbo.stg_Route (
    RouteID INT NOT NULL,
    RouteCode NVARCHAR(50) NOT NULL,
    RouteName NVARCHAR(25) NULL,
    DispatchAlias NVARCHAR(50) NOT NULL,
    RouteGroupId INT NOT NULL,
    EstimatedStartDateTime DATETIME2(7) NOT NULL,
    EstimatedEndDateTime DATETIME2(7) NOT NULL,
    PlannedStartDateTime DATETIME2(7) NULL,
    PlannedEndDateTime DATETIME2(7) NULL,
    PlannedDuration DECIMAL(7,4) NOT NULL,
    PlannedDistance DECIMAL(9,3) NULL,
    PlannedPay DECIMAL(9,2) NULL,
    LatestDepartureDateTime DATETIME2(7) NULL,
    TractorID NVARCHAR(25) NULL,
    TrailerID NVARCHAR(50) NULL,
    TrailerType NVARCHAR(25) NULL,
    TrailerTrackedIndicator NVARCHAR(20) NULL,
    DispatchSiteDriverFleetId INT NULL,
    OriginFacilityAlias NVARCHAR(50) NOT NULL,
    DestinationFacilityAlias NVARCHAR(50) NOT NULL,
    RouteStatusCode NVARCHAR(30) NOT NULL,
    PreviousRouteStatusCode NVARCHAR(30) NULL,
    RouteType NVARCHAR(20) NULL,
    SourceSystemId INT NOT NULL,
    RouteSequenceNumber INT NULL,
    ReleaseReasonID INT NULL,
    CarrierCode NVARCHAR(10) NULL,
    IsOverTheRoad BIT NULL,
    IsPrivateFleet BIT NULL,
    IsRelay BIT NULL,
    RelayShipmentCode NVARCHAR(50) NULL,
    NextShipmentId NVARCHAR(50) NULL,
    IsPrinted BIT NULL,
    PreTripDuration DECIMAL(7,4) NULL,
    PostTripDuration DECIMAL(7,4) NULL,
    LoaderInitials NVARCHAR(12) NULL,
    DoorNumber NVARCHAR(10) NULL,
    LoadStartTime DATETIME2(7) NULL,
    LoadCloseTime DATETIME2(7) NULL,
    TrailerTemperature DECIMAL(9,2) NULL,
    TrailerTempVerifiedTime DATETIME2(7) NULL,
    SpecialInstructions NVARCHAR(500) NULL,
    GateOutDateTime DATETIME2(7) NULL,
    GateOutInitials NVARCHAR(12) NULL,
    OriginalLoadCloseTime DATETIME2(7) NULL,
    OriginalLoaderInitials NVARCHAR(12) NULL,
    LoadCloseReasonCode INT NULL,
    SensorSetPoint DECIMAL(4,1) NULL,
    SensorReturn01 DECIMAL(4,1) NULL,
    SensorLastPing DATETIME2(7) NULL,
    DefrostIndicator BIT NULL,
    CreatedDateTime DATETIME2(7) NOT NULL,
    UpdatedDateTime DATETIME2(7) NOT NULL,
    UpdatedUserId NVARCHAR(10) NOT NULL
);
GO

IF OBJECT_ID('dbo.stg_TransportationOrder', 'U') IS NOT NULL
    DROP TABLE dbo.stg_TransportationOrder;
GO

CREATE TABLE dbo.stg_TransportationOrder (
    TransportationOrderID INT NOT NULL,
    PurchaseOrder NVARCHAR(32) NOT NULL,
    VendorName NVARCHAR(500) NULL,
    VendorNumber NVARCHAR(20) NULL,
    StoreName NVARCHAR(16) NULL,
    PickupNumber NVARCHAR(50) NULL,
    ProtectionLevel NVARCHAR(100) NULL,
    SourceSystemId INT NOT NULL,
    SplitOrderIndicator BIT NOT NULL,
    ProductCategory NVARCHAR(100) NULL,
    WindowStartDateTime DATETIME2(7) NULL,
    WindowEndDateTime DATETIME2(7) NULL,
    FacilityAlias NVARCHAR(20) NOT NULL,
    SequenceNumber INT NULL,
    BillDay INT NULL,
    CreatedDateTime DATETIME2(7) NOT NULL,
    UpdatedDateTime DATETIME2(7) NOT NULL,
    UpdatedUserId NVARCHAR(10) NOT NULL
);
GO

IF OBJECT_ID('dbo.stg_TransportationOrderLine', 'U') IS NOT NULL
    DROP TABLE dbo.stg_TransportationOrderLine;
GO

CREATE TABLE dbo.stg_TransportationOrderLine (
    TransportationOrderID INT NOT NULL,
    LineNumber INT NOT NULL,
    ShipmentID BIGINT NULL,
    PurchaseOrderNumber BIGINT NULL,
    PurchaseOrderLineNumber INT NULL,
    PurchaseOrderCreatedDate DATE NULL,
    DeliveryUnitType NVARCHAR(100) NOT NULL,
    UnitCount INT NOT NULL,
    Weight DECIMAL(9,2) NOT NULL,
    Volume DECIMAL(9,2) NULL,
    CaseCount INT NULL,
    CreatedDateTime DATETIME2(7) NOT NULL,
    UpdatedDateTime DATETIME2(7) NOT NULL,
    UpdatedUserID NVARCHAR(10) NOT NULL
);
GO

IF OBJECT_ID('dbo.stg_Trip', 'U') IS NOT NULL
    DROP TABLE dbo.stg_Trip;
GO

CREATE TABLE dbo.stg_Trip (
    TripID INT NOT NULL,
    ParentTripID INT NULL,
    RouteID INT NULL,
    RouteGroupID INT NULL,
    DispatchAlias NVARCHAR(50) NOT NULL,
    TripCode NVARCHAR(50) NOT NULL,
    TripName NVARCHAR(50) NULL,
    TripType INT NOT NULL,
    OriginFacilityAlias NVARCHAR(50) NOT NULL,
    DestinationFacilityAlias NVARCHAR(50) NOT NULL,
    TripStatus NVARCHAR(20) NOT NULL,
    PreviousTripStatus NVARCHAR(20) NULL,
    ActualStartDateTime DATETIME2(7) NULL,
    ActualEndDateTime DATETIME2(7) NULL,
    ActualDistance DECIMAL(9,3) NOT NULL,
    ActualPay DECIMAL(9,2) NOT NULL,
    PaidHours DECIMAL(7,4) NOT NULL,
    TractorID NVARCHAR(25) NULL,
    TrailerID NVARCHAR(50) NULL,
    NoDelays BIT NOT NULL,
    DispatchSiteDriverFleetId INT NULL,
    CarrierCode NVARCHAR(10) NULL,
    OverTheRoadIndicator BIT NULL,
    PayConcludedDateTime DATETIME2(7) NULL,
    CreatedDateTime DATETIME2(7) NOT NULL,
    UpdatedDateTime DATETIME2(7) NOT NULL,
    UpdatedUserID NVARCHAR(10) NOT NULL
);
GO

IF OBJECT_ID('dbo.stg_TripActivity', 'U') IS NOT NULL
    DROP TABLE dbo.stg_TripActivity;
GO

CREATE TABLE dbo.stg_TripActivity (
    TripActivityID INT NOT NULL,
    TripID INT NOT NULL,
    ActivityType NVARCHAR(20) NOT NULL,
    BeginDateTime DATETIME2(7) NULL,
    EndDateTime DATETIME2(7) NULL,
    Duration DECIMAL(9,2) NOT NULL,
    CreatedDateTime DATETIME2(7) NOT NULL,
    UpdatedDateTime DATETIME2(7) NOT NULL,
    UpdatedUserID NVARCHAR(10) NOT NULL
);
GO

IF OBJECT_ID('dbo.stg_TripStopActivity', 'U') IS NOT NULL
    DROP TABLE dbo.stg_TripStopActivity;
GO

CREATE TABLE dbo.stg_TripStopActivity (
    TripStopActivityID INT NOT NULL,
    TripStopID INT NOT NULL,
    ActivityType NVARCHAR(20) NOT NULL,
    BeginDateTime DATETIME2(7) NULL,
    EndDateTime DATETIME2(7) NULL,
    Duration DECIMAL(9,2) NOT NULL,
    Quantity INT NULL,
    DeliveryUnitType NVARCHAR(100) NULL,
    ProductCategory NVARCHAR(100) NULL,
    CreatedDateTime DATETIME2(7) NOT NULL,
    UpdatedDateTime DATETIME2(7) NOT NULL,
    UpdatedUserID NVARCHAR(10) NOT NULL
);
GO

IF OBJECT_ID('dbo.stg_TripStop', 'U') IS NOT NULL
    DROP TABLE dbo.stg_TripStop;
GO

CREATE TABLE dbo.stg_TripStop (
    TripStopID INT NOT NULL,
    TripID INT NOT NULL,
    RouteStopID INT NULL,
    SequenceNumber INT NOT NULL,
    FacilityAlias NVARCHAR(20) NOT NULL,
    ArrivalDateTime DATETIME2(7) NOT NULL,
    DepartureDateTime DATETIME2(7) NOT NULL,
    ReceivingType NVARCHAR(100) NULL,
    CalculatedMileage DECIMAL(9,3) NULL,
    CalculatedDriveMinutes DECIMAL(9,3) NULL,
    StandardAppliedUtc DATETIME2(7) NULL,
    CreatedDateTime DATETIME2(7) NOT NULL,
    UpdatedDateTime DATETIME2(7) NOT NULL,
    UpdatedUserID NVARCHAR(10) NOT NULL
);
GO

IF OBJECT_ID('dbo.stg_TripStopDelay', 'U') IS NOT NULL
    DROP TABLE dbo.stg_TripStopDelay;
GO

CREATE TABLE dbo.stg_TripStopDelay (
    TripStopDelayID INT NOT NULL,
    TripStopID INT NULL,
    DelayType NVARCHAR(25) NOT NULL,
    Duration DECIMAL(9,2) NOT NULL,
    BeginDateTime DATETIME2(6) NULL,
    EndDateTime DATETIME2(6) NULL,
    CreatedDateTime DATETIME2(7) NOT NULL,
    UpdatedDateTime DATETIME2(7) NOT NULL,
    UserID NVARCHAR(20) NOT NULL
);
GO

IF OBJECT_ID('dbo.stg_CapsExport', 'U') IS NOT NULL
    DROP TABLE dbo.stg_CapsExport;
GO

CREATE TABLE dbo.stg_CapsExport (
    DispatchAlias NVARCHAR(50) NOT NULL,
    WeekEndDate DATE NOT NULL,
    Status NVARCHAR(50) NOT NULL,
    DriverCount INT NULL,
    TripCount INT NULL,
    TotalPayMinutes DECIMAL(9,2) NULL,
    TopDriverMinutes DECIMAL(9,2) NULL,
    CreatedDateTime DATETIME2(7) NOT NULL,
    UpdatedDateTime DATETIME2(7) NOT NULL,
    UpdatedUserID NVARCHAR(10) NOT NULL
);
GO

IF OBJECT_ID('dbo.stg_DispatchActivityStandard', 'U') IS NOT NULL
    DROP TABLE dbo.stg_DispatchActivityStandard;
GO

CREATE TABLE dbo.stg_DispatchActivityStandard (
    DispatchAlias NVARCHAR(50) NOT NULL,
    ProductCategory NVARCHAR(100) NOT NULL,
    ReceivingType NVARCHAR(100) NOT NULL,
    DeliveryUnit NVARCHAR(100) NOT NULL,
    ActivityType NVARCHAR(50) NOT NULL,
    ActivityMinutes DECIMAL(9,2) NOT NULL,
    EffectiveStartDate DATE NOT NULL,
    EffectiveEndDate DATE NULL,
    CreatedDateTime DATETIME2(7) NOT NULL,
    UpdatedDateTime DATETIME2(7) NULL,
    UpdatedUserID NVARCHAR(10) NULL
);
GO

IF OBJECT_ID('dbo.stg_FacilityTransit', 'U') IS NOT NULL
    DROP TABLE dbo.stg_FacilityTransit;
GO

CREATE TABLE dbo.stg_FacilityTransit (
    OriginFacilityAlias NVARCHAR(50) NOT NULL,
    DestinationFacilityAlias NVARCHAR(50) NOT NULL,
    Mileage DECIMAL(9,3) NOT NULL,
    DriveMinutes DECIMAL(9,3) NOT NULL,
    CreatedDateTime DATETIME2(7) NOT NULL,
    UpdatedDateTime DATETIME2(7) NOT NULL,
    UpdatedUserID NVARCHAR(10) NOT NULL
);
GO

IF OBJECT_ID('dbo.stg_FacilityProductCategoryReceivingType', 'U') IS NOT NULL
    DROP TABLE dbo.stg_FacilityProductCategoryReceivingType;
GO

CREATE TABLE dbo.stg_FacilityProductCategoryReceivingType (
    FacilityProductCategoryReceivingTypeID INT PRIMARY KEY IDENTITY(1,1),
    FacilityAlias NVARCHAR(50) NOT NULL,
    ProductCategory NVARCHAR(100) NOT NULL,
    ReceivingType NVARCHAR(100) NOT NULL,
    CreatedDateTime DATETIME2(7) NOT NULL,
    UpdatedDateTime DATETIME2(7) NULL,
    UpdatedUserID NVARCHAR(50) NULL
);
GO

IF OBJECT_ID('dbo.stg_FacilityOrgUnitXref', 'U') IS NOT NULL
    DROP TABLE dbo.stg_FacilityOrgUnitXref;
GO

CREATE TABLE dbo.stg_FacilityOrgUnitXref (
    FacilityAlias DECIMAL(20,0) NOT NULL,
    OrgUnit INT NOT NULL PRIMARY KEY,
    Division TINYINT NOT NULL
);
GO

IF OBJECT_ID('dbo.stg_Gate', 'U') IS NOT NULL
    DROP TABLE dbo.stg_Gate;
GO

CREATE TABLE dbo.stg_Gate (
    GateId INT NOT NULL PRIMARY KEY,
    GateName NVARCHAR(50) NOT NULL,
    IsActive BIT NOT NULL
);
GO

IF OBJECT_ID('dbo.stg_SiteOverrideStandard', 'U') IS NOT NULL
    DROP TABLE dbo.stg_SiteOverrideStandard;
GO

CREATE TABLE dbo.stg_SiteOverrideStandard (
    FacilityAlias NVARCHAR(50) NOT NULL,
    ProductCategory NVARCHAR(100) NOT NULL,
    ReceivingType NVARCHAR(100) NOT NULL,
    ActivityType NVARCHAR(50) NOT NULL,
    ActivityMinutes DECIMAL(9,2) NOT NULL,
    EffectiveStartDate DATE NOT NULL,
    EffectiveEndDate DATE NULL,
    CreatedDateTime DATETIME2(7) NOT NULL,
    UpdatedDateTime DATETIME2(7) NOT NULL,
    UpdatedUserID NVARCHAR(10) NOT NULL
);
GO

IF OBJECT_ID('dbo.stg_DriverShiftActivity', 'U') IS NOT NULL
    DROP TABLE dbo.stg_DriverShiftActivity;
GO

CREATE TABLE dbo.stg_DriverShiftActivity (
    DriverShiftActivityID BIGINT NOT NULL,
    DriverId INT NOT NULL,
    ActivityType NVARCHAR(50) NOT NULL,
    ActivityTimeStamp DATETIME2(7) NOT NULL,
    CreatedDateTime DATETIME2(7) NOT NULL,
    UpdatedDateTime DATETIME2(7) NULL,
    UpdatedUserID NVARCHAR(10) NULL
);
GO

IF OBJECT_ID('dbo.stg_RouteStopActivity', 'U') IS NOT NULL
    DROP TABLE dbo.stg_RouteStopActivity;
GO

CREATE TABLE dbo.stg_RouteStopActivity (
    RouteStopActivityId INT NOT NULL,
    RouteStopId INT NOT NULL,
    TransportationOrderId INT NULL,
    TransportationOrderLineNumber INT NULL,
    ActivityType NVARCHAR(20) NOT NULL,
    PlannedMinutes DECIMAL(9,2) NOT NULL,
    EstimatedMinutes DECIMAL(9,2) NOT NULL,
    CreatedDateTime DATETIME2(7) NOT NULL,
    UpdatedDateTime DATETIME2(7) NOT NULL,
    UserId NVARCHAR(10) NOT NULL
);
GO


