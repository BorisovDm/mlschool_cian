USE dborisov;

DROP TABLE IF EXISTS announcement_lst;

CREATE TABLE announcement_lst(
    announcementid bigint,
    lat double,
    lng double,
    description string,
    address string,
    floornumber int,
    floorscount int,
    category string,
    roomscount int,
    totalarea double,
    price double,
    pricetype string,
    status int,
    dateinserted timestamp
)
PARTITIONED BY (ptn_dadd date)
STORED AS ORC;

INSERT OVERWRITE TABLE announcement_lst
PARTITION (ptn_dadd='2019-07-01')
SELECT *
FROM prod.announcement_lst;
