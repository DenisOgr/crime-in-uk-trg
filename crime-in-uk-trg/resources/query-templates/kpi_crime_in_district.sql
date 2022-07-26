SELECT
        crimeType, CONCAT(ROUND(t.cn / tc.cn * 100, 1), '%') cn
    FROM (
        SELECT
            crimeType, count(1) cn
        FROM
            {%table%}
        WHERE
            districtName='{%districtName%}'
        GROUP BY 1
        ORDER BY 2 DESC
    ) t, (
        SELECT
            COUNT(1) cn
        FROM
            {%table%}
        WHERE
            districtName='{%districtName%}'
        ) tc