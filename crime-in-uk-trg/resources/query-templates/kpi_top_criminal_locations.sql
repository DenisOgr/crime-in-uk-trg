 SELECT 
        CONCAT(t.latitude, '00000') latitude, CONCAT(t.longitude, '00000') longitude, CONCAT(ROUND(t.cn / tc.cn * 100, 1), '%') cn
    FROM (
        SELECT 
            ROUND(latitude, 1) latitude,  ROUND(longitude, 1) longitude, count(1) cn
        FROM 
            {%table%}  
        WHERE 
         districtName='{%districtName%}' AND latitude is not null 
        GROUP BY 1,2 
        ORDER BY count(1) DESC
    ) t, 
    (
        SELECT 
            COUNT(1) cn 
        FROM 
            {%table%} 
        WHERE 
            districtName='{%districtName%}' AND latitude is not null
        ) tc
    LIMIT {%TOP%}