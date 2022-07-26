import argparse
from argparse import Namespace
from tabulate import tabulate
from typing import Sequence
from data_provider import seed_from_csv, create_view, get_data_from_query, TargetDirectoryExists
from kpi_provider import (build_request_from_args, crime_in_district, top_crimes_loc_in_district,
                          top_crimes_for_each_district, top_outcomes_for_crimes_in_district)
from logging_provider import get_logger
from pyspark.sql import SparkSession, Row

KPI_TASKS = {
    'crimes_in_district': crime_in_district,
    'top_crimes_loc_in_district': top_crimes_loc_in_district,
    'top_crimes_for_each_district': top_crimes_for_each_district,
    'top_outcomes_for_crimes_in_district': top_outcomes_for_crimes_in_district
}

logger = get_logger()


def pretty_table(rows: Sequence[Row], title: str = ''):
    """
    Print table to the CLI.
    Args:
        rows: Required. Sequences with rows.
        title: Optional. Title of the table.
    Raises:
        ValueError: in case the rows is not subscriptable or items in rows are not Row.
    """
    print(title)
    if not rows:
        return None

    if not hasattr(rows, '__getitem__'):
        raise ValueError("The rows should be subscriptable.")

    if not isinstance(rows[0], Row):
        raise ValueError("The row in rows should be Row objects.")

    headers = rows[0].asDict().keys()
    body = []
    for row in rows:
        body.append([row[name] for name in headers])

    print(tabulate(body, headers, tablefmt="fancy_grid"))


def seed(spark: SparkSession, args: Namespace):
    """
    Seed the application with the data.
    Args:
        spark: Required. The Spark application.
        args: Required. Input params from the STDIN.

    """
    try:
        seed_from_csv(spark)
    except TargetDirectoryExists:
        logger.warning(
            "This step has already been started. You can work with the data or restart this step using the command `make restart`")


def run_query(spark: SparkSession, args: Namespace):
    """
    Run query and print results in the table.
    Args:
        spark: Required. The Spark application.
        args: Required. Input params from the STDIN.
    """
    if not args.query:
        raise ValueError(f"The --query argument is required.")

    create_view(spark)
    pretty_table(get_data_from_query(spark, args.query), "Results from the query:")


def get_kpi(spark: SparkSession, args: Namespace):
    """
    Get data from the KPI provider and print results in the table.
    Args:
        spark: Required. The Spark application.
        args: Required. Input params from the STDIN.

    """
    if args.kpi not in KPI_TASKS:
        logger.error(f"The KPI task `{args.kpi}` is incorrect.")
        return None

    params = build_request_from_args(args)
    create_view(spark)
    title = f"Report: {args.kpi}.  District: {args.district_name}. Top results: {args.top}."
    pretty_table(KPI_TASKS[args.kpi](spark, params), title)


if __name__ == '__main__':
    TASKS = {
        'seed': seed,
        'run_query': run_query,
        'get_kpi': get_kpi,
    }

    parser = argparse.ArgumentParser(description='Interaction with the Apache Spark.')
    parser.add_argument('task', help=f"Available tasks: {', '.join(TASKS.keys())}")
    parser.add_argument('--kpi', help=f"Available KPI tasks: {', '.join(KPI_TASKS)}")
    parser.add_argument('--district_name', default='', help=f"Name of the district.")
    parser.add_argument('--top', default=0, help=f"Number of top rows for each group.")
    parser.add_argument('--query', help=f"Query for running over data. Use table `data`")

    args = parser.parse_args()

    if args.task not in TASKS:
        raise ValueError(f"The task `{args.task}` is incorrect.")

    spark = SparkSession \
        .builder \
        .appName("Main") \
        .getOrCreate()

    results = TASKS[args.task](spark, args)
