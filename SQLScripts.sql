CREATE DATABASE TaxiData;

USE TaxiData;

CREATE TABLE TaxiRides (
    PickupDatetime DATETIME,
    DropoffDatetime DATETIME,
    PassengerCount INT,
    TripDistance FLOAT,
    StoreAndFwdFlag NVARCHAR(50),
    PULocationID INT,
    DOLocationID INT,
    FareAmount DECIMAL(18, 2),
    TipAmount DECIMAL(18, 2)
);
