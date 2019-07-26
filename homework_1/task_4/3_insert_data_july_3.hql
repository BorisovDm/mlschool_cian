USE dborisov;

INSERT OVERWRITE TABLE announcement_lst
PARTITION (ptn_dadd='2019-07-03')
 SELECT
    t1.announcementid,
    t1.lat,
    t1.lng,
    t1.description,
    t1.address,
    t1.floornumber,
    t1.floorscount,
    t1.category,
    t1.roomscount,
    t1.totalarea,
    t1.price,
    t1.pricetype,
    t1.status,
    t1.dateinserted
 FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY announcementid ORDER BY dateinserted DESC) rn
    FROM prod.announcement_parsed
    WHERE ptn_dadd = '2019-07-03'
 ) t1
 WHERE t1.rn = 1

 UNION ALL

 SELECT
    t2.announcementid,
    t2.lat,
    t2.lng,
    t2.description,
    t2.address,
    t2.floornumber,
    t2.floorscount,
    t2.category,
    t2.roomscount,
    t2.totalarea,
    t2.price,
    t2.pricetype,
    t2.status,
    t2.dateinserted
 FROM announcement_lst t2
 WHERE (t2.ptn_dadd='2019-07-02') and t2.announcementid NOT IN (
    SELECT announcementid
    FROM prod.announcement_parsed
    WHERE ptn_dadd = '2019-07-03'
 )
;
