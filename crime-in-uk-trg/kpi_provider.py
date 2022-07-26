from os import path
from argparse import Namespace
from collections import namedtuple
from typing import Sequence

from pyspark import Row
from pyspark.sql import functions as f, SparkSession
from data_provider import TABLE_NAME

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


def _get_template(template_name: str) -> str:
    """
    Get and read content from the query template with specific template_name.
    Args:
        template_name: Required. Name of the template/

    Returns:
        Content of the template.

    """
    path_to_dir = path.join(path.dirname(path.abspath(__file__)), 'resources', 'query-templates')
    with open(path.join(path_to_dir, f"{template_name}.sql"), 'r') as f:
        template = f.read()

    return template


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
    return spark.sql(_build_query_from_template(_get_template('kpi_crime_in_district'), params)).collect()


def top_crimes_loc_in_district(spark: SparkSession, params: KPIRequest) -> Sequence[Row]:
    """
    Run query and collect data for the KPI `top_crimes_loc_in_district`.
    Args:
        spark: Required. Spark application.
        params: Required. Params for template.

    Returns:
        Sequence with locations.
    """
    return spark.sql(_build_query_from_template(_get_template('kpi_top_criminal_locations'), params)).collect()


def top_crimes_for_each_district(spark: SparkSession, params: KPIRequest) -> Sequence[Row]:
    """
    Run query and collect data for the KPI `top_crimes_for_each_district`.
    Args:
        spark: Required. Spark application.
        params: Required. Params for template.

    Returns:
        Sequence with crimes.

    """
    query = _build_query_from_template(_get_template('kpi_top_n_crimes_for_each_district'), params)
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
    query = _build_query_from_template(_get_template('kpi_top_n_outcomes_for_crimes_in_district'), params)
    return spark.sql(query).groupBy('crimeType').pivot('rank').agg(f.first(f.col("outcome"))).collect()
