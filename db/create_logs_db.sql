CREATE TABLE IF NOT EXISTS DealLogs (
    DealUUID TEXT,
    CreatedAt DateTime,
    LogLevel TEXT,
    LogMsg TEXT,
    LogParams TEXT,
    Subsystem TEXT
);

CREATE INDEX IF NOT EXISTS index_deal_logs_deal_uuid on DealLogs(DealUUID);
