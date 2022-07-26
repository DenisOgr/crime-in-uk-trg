WITH total_count AS (
        SELECT
            districtName, COUNT(1) cn
        FROM
            {%table%}
        GROUP BY
            districtName
    )

    SELECT t2.districtName,  CONCAT(t2.crimeType, ' (',ROUND(t2.cn / tc.cn * 100, 1), '%)') crimeType, CONCAT('place:',t2.rank) rank
    FROM (
        SELECT
            districtName, crimeType, cn, ROW_NUMBER() OVER (PARTITION BY districtName ORDER BY cn DESC) AS rank
        FROM (
            SELECT
                districtName, crimeType, COUNT(1) cn
            FROM
                {%table%}
            GROUP BY
                districtName, crimeType ) t1
    ) t2
    INNER JOIN total_count tc ON tc.districtName=t2.districtName
    WHERE rank <= {%TOP%}