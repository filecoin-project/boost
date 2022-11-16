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
