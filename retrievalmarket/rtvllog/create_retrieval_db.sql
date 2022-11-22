CREATE TABLE IF NOT EXISTS RetrievalDealStates (
    CreatedAt DateTime,
    UpdatedAt DateTime,
    LocalPeerID Text,
    PeerID TEXT,
    DealID INT,
    TransferID INT,
    PayloadCID TEXT,
    PieceCID TEXT,
    PaymentInterval INT,
    PaymentIntervalIncrease INT,
    PricePerByte TEXT,
    UnsealPrice TEXT,
    Status TEXT,
    Message TEXT,
    TotalSent INT,
    DTStatus TEXT,
    DTMessage TEXT
);

CREATE INDEX IF NOT EXISTS index_retrieval_states_created_at on RetrievalDealStates(CreatedAt);
CREATE INDEX IF NOT EXISTS index_retrieval_states_deal_id on RetrievalDealStates(DealID);

CREATE TABLE IF NOT EXISTS RetrievalDataTransferEvents (
    PeerID TEXT,
    TransferID INT,
    CreatedAt DateTime,
    Name TEXT,
    Message TEXT
);

CREATE INDEX IF NOT EXISTS index_retrieval_dt_evts_created_at on RetrievalDataTransferEvents(CreatedAt);
CREATE INDEX IF NOT EXISTS index_retrieval_dt_evts_peer_transfer_id on RetrievalDataTransferEvents(PeerID, TransferID);

CREATE TABLE IF NOT EXISTS RetrievalMarketEvents (
    PeerID TEXT,
    DealID INT,
    CreatedAt DateTime,
    Name TEXT,
    Status TEXT,
    Message TEXT
);

CREATE INDEX IF NOT EXISTS index_retrieval_market_evts_created_at on RetrievalMarketEvents(CreatedAt);
CREATE INDEX IF NOT EXISTS index_retrieval_market_evts_peer_deal_id on RetrievalMarketEvents(PeerID, DealID);
