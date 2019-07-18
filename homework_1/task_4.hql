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
    dateinserted timestamp,
    ptn_dadd date
)
PARTITIONED BY (ptn_dadd date)
STORED AS ORC AS
SELECT announcementid, lat, lng, description, address, floornumber, floorscount, category, roomscount, totalarea, price, pricetype, status, dateinserted, TO_DATE('2019-07-01') AS ptn_dadd
FROM prod.announcement_lst;
