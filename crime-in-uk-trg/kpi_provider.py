from argparse import Namespace
from collections import namedtuple
from typing import Sequence

from pyspark import Row
from pyspark.sql import functions as f, SparkSession
from data_provider import TABLE_NAME

# distribution of different crimes for district
KPI_CRIMES_IN_DISTRICT = """
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
"""

# TOP-N most criminal locations (hotspots) in districtName
KPI_TOP_CRIMINAL_LOCATIONS = """
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
"""

# TOP-N crimeType for each district
KPI_TOP_N_CRIMES_FOR_EACH_DISTRICT = """
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
"""

# TOP-N outcomes for each crimeType
KPI_TOP_OUTCOMES_FOR_CRIMES_IN_DISTRICT = """
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
"""
KPIRequest = namedtuple('KPIRequest', ['district_name', 'top'])


def build_request_from_args(args: Namespace) -> KPIRequest:
    """
    Build KPIRequest from the params from the STDIN
    Args:
        args: Required. Input params from the STDIN.

    Returns:
        KPIRequest object.
    """
    return KPIRequest(
        district_name=args.district_name,
        top=args.top
    )


def _build_query_from_template(template: str, params: KPIRequest) -> str:
    """
    Build Spark query from the template.
    Args:
        template: Required. String with template.
        params: Required. Params for template.

    Returns:
        Spark query.
    """
    return (template
            .replace('{%table%}', TABLE_NAME)
            .replace('{%districtName%}', params.district_name)
            .replace('{%TOP%}', str(params.top)))


def crime_in_district(spark: SparkSession, params: KPIRequest) -> Sequence[Row]:
    """
    Run query and collect data for the KPI `crime_in_district`.
    Args:
        spark: Required. Spark application.
        params: Required. Params for template.

    Returns:
        Sequence with crimes.
    """
    return spark.sql(_build_query_from_template(KPI_CRIMES_IN_DISTRICT, params)).collect()


def top_crimes_loc_in_district(spark: SparkSession, params: KPIRequest) -> Sequence[Row]:
    """
    Run query and collect data for the KPI `top_crimes_loc_in_district`.
    Args:
        spark: Required. Spark application.
        params: Required. Params for template.

    Returns:
        Sequence with locations.
    """
    return spark.sql(_build_query_from_template(KPI_TOP_CRIMINAL_LOCATIONS, params)).collect()


def top_crimes_for_each_district(spark: SparkSession, params: KPIRequest) -> Sequence[Row]:
    """
    Run query and collect data for the KPI `top_crimes_for_each_district`.
    Args:
        spark: Required. Spark application.
        params: Required. Params for template.

    Returns:
        Sequence with crimes.

    """
    query = _build_query_from_template(KPI_TOP_N_CRIMES_FOR_EACH_DISTRICT, params)
    return spark.sql(query).groupBy('districtName').pivot('rank').agg(f.first(f.col("crimeType"))).collect()


def top_outcomes_for_crimes_in_district(spark: SparkSession, params: KPIRequest) -> Sequence[Row]:
    """
    Run query and collect data for the KPI `top_outcomes_for_crimes_in_district`.
    Args:
        spark: Required. Spark application.
        params: Required. Params for template.

    Returns:
        Sequence with outcomes.

    """
    query = _build_query_from_template(KPI_TOP_OUTCOMES_FOR_CRIMES_IN_DISTRICT, params)
    return spark.sql(query).groupBy('crimeType').pivot('rank').agg(f.first(f.col("outcome"))).collect()
