WITH total_count AS (
    SELECT
        crimeType, COUNT(1) cn
    FROM {%table%}
    WHERE
        districtName='{%districtName%}'
    GROUP BY crimeType
)
SELECT
    t2.crimeType, CONCAT(lastOutcome, ' (',ROUND(t2.cn / tc.cn * 100, 1), '%)') outcome, CONCAT('place:',rank) rank
FROM (
    SELECT
        crimeType, lastOutcome, cn, ROW_NUMBER() OVER (PARTITION BY crimeType ORDER BY cn DESC) AS rank
    FROM (
        SELECT
            crimeType, lastOutcome, COUNT(1) cn
        FROM
            {%table%}
        WHERE
            districtName='{%districtName%}'
        GROUP BY
            crimeType, lastOutcome
    ) t1
) t2
INNER JOIN total_count tc ON tc.crimeType=t2.crimeType
WHERE rank <= {%TOP%}