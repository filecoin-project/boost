CREATE TABLE Deals (
    ID varchar(128),
    CreatedAt DateTime,
    PieceCID varchar(255),
    PieceSize int,
    Address varchar(255),
    Client varchar(255),
    Provider varchar(255),
    Label varchar(255),
    StartEpoch int,
    EndEpoch int,
    StoragePricePerEpoch int,
    ProviderCollateral int,
    ClientCollateral int,
    PRIMARY KEY(ID)
) WITHOUT ROWID;