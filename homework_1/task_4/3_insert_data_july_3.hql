USE dborisov;

INSERT OVERWRITE TABLE announcement_lst
PARTITION (ptn_dadd='2019-07-03')
SELECT
    t.announcementid,
    t.lat,
    t.lng,
    t.description,
    t.address,
    t.floornumber,
    t.floorscount,
    t.category,
    t.roomscount,
    t.totalarea,
    t.price,
    t.pricetype,
    t.status,
    t.dateinserted
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY announcementid ORDER BY dateinserted DESC) rn
    FROM (
        SELECT *
        FROM prod.announcement_parsed
        WHERE ptn_dadd = '2019-07-03'

        UNION ALL

        SELECT *
        FROM announcement_lst
        WHERE ptn_dadd = '2019-07-02' 
    ) union_table
) t
WHERE t.rn = 1
;
