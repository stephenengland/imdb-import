#!/usr/bin/python3

import os
import logging
import psycopg2
import psycopg2.extras
import sys
import configargparse

from contextlib import contextmanager
from pathlib import Path


# Configure loging
root = logging.getLogger()
root.setLevel(logging.INFO)

ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(message)s - %(name)s - %(levelname)s', "%m-%d %H:%M:%S")
ch.setFormatter(formatter)
root.addHandler(ch)
logger = logging.getLogger(__name__)
logging.getLogger("aiohttp.access").setLevel(logging.WARNING)

def configure():
    config_dir = os.environ["NOMAD_TASK_DIR"] if "NOMAD_TASK_DIR" in os.environ else ".."
    config_path = str(Path(config_dir) / "config.yaml")
    parser = configargparse.ArgumentParser(default_config_files=[config_path], description="Get data for config")
    parser.add_argument("--rds_server", env_var="RDS_SERVER", type=str, help="RDS Server", required=True)
    parser.add_argument("--rds_database", env_var="RDS_DATABASE", type=str, help="RDS Database", required=True)
    parser.add_argument("--rds_user", env_var="RDS_USER", type=str, help="RDS User", required=True)
    parser.add_argument("--rds_password", env_var="RDS_PASSWORD", type=str, help="RDS Password", required=True)
    args, unknown = parser.parse_known_args()
    if unknown:
        logger.info("received unknown arguments ", unknown)
    return args

@contextmanager
def open_cursor(rds_server, rds_database, rds_user, rds_password, readonly=True):
    connection = psycopg2.connect(dbname=rds_database, user=rds_user, password=rds_password, host=rds_server)
    connection.set_session(readonly=readonly, autocommit=True)
    cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
    yield cursor
    cursor.close()
    connection.close()

def column_or_null(column):
    if column == "\\N":
        return None

    return column

def read_line(line):
    columns = line.split('\t')
    
    return {
        "id": columns[0],
        "titleType": column_or_null(columns[1]),
        "primaryTitle": column_or_null(columns[2]),
        "originalTitle": column_or_null(columns[3]),
        "isAdult": column_or_null(columns[4]),
        "startYear": column_or_null(columns[5]),
        "endYear": column_or_null(columns[6]),
        "runtimeMinutes": column_or_null(columns[7]),
        "genres": column_or_null(columns[8])
    }

def iterate_over_file(filename):
    with open(filename) as file_handle:
        i = 0
        for line in file_handle:
            if i > 0:
                yield read_line(line)
            i += 1

def store_result(cursor, result):
    cursor.execute("""
        INSERT INTO imdb.titlebasics (id, titleType, primaryTitle, originalTitle, isAdult, startYear, endYear, runtimeMinutes, genres)
        VALUES (%(id)s, %(titleType)s, %(primaryTitle)s, %(originalTitle)s, %(isAdult)s, %(startYear)s, %(endYear)s, %(runtimeMinutes)s, %(genres)s)
        ON CONFLICT DO UPDATE SET
        titleType = %(titleType)s,
        primaryTitle = %(primaryTitle)s,
        originalTitle = %(originalTitle)s,
        isAdult = %(isAdult)s,
        startYear = %(startYear)s,
        endYear = %(endYear)s,
        runtimeMinutes = %(runtimeMinutes)s,
        genres = %(genres)s;
    """, result)

def main(
    rds_server,
    rds_database,
    rds_user,
    rds_password,
    ):

    with open_cursor(rds_server, rds_database, rds_user, rds_password, readonly=False) as cursor:
        for item in iterate_over_file("data.tsv"):
            store_result(cursor, item)

if __name__ == "__main__":
    main(**configure().__dict__)