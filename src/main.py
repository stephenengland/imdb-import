#!/usr/bin/python3

import os
import logging
import psycopg2
import psycopg2.extras
import schema
from relation_type import RelationType
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
    parser.add_argument("--ingestion_type", env_var="INGESTION_TYPE", type=str, help="Ingestion Type", required=True)
    args, unknown = parser.parse_known_args()
    if unknown:
        logger.info("received unknown arguments " + unknown)
    return args

@contextmanager
def open_cursor(rds_server, rds_database, rds_user, rds_password, readonly=True):
    connection = psycopg2.connect(dbname=rds_database, user=rds_user, password=rds_password, host=rds_server)
    connection.set_session(readonly=readonly, autocommit=True)
    cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
    yield cursor
    cursor.close()
    connection.close()


def iterate_over_file(filename, read_line_function):
    with open(filename) as file_handle:
        i = 0
        for line in file_handle:
            if i > 0:
                yield read_line_function(line)
            i += 1

def main(
    rds_server,
    rds_database,
    rds_user,
    rds_password,
    ingestion_type
    ):
    ingestion_type = ingestion_type.lower()
    logger.info(ingestion_type)

    with open_cursor(rds_server, rds_database, rds_user, rds_password, readonly=False) as cursor:
        if ingestion_type == "titles":
            title_ids = set(schema.iterate_over_title_ids(cursor))
            for title in iterate_over_file("title.basics.tsv", schema.read_title_line):
                if title["titleId"] not in title_ids:
                    schema.store_title(cursor, title)
        else:
            title_name_ids = set(schema.iterate_over_title_name_ids(cursor))

        if ingestion_type == "names":
            name_ids = set(schema.iterate_over_name_ids(cursor))
            for name in iterate_over_file("name.basics.tsv", schema.read_name_line):
                if name["nameId"] not in name_ids:
                    schema.store_name(cursor, name)

                for known_for_title in name["knownForTitles"]:
                    if (known_for_title, name["nameId"]) not in title_name_ids:
                        schema.store_title_name(cursor, {
                            "nameId": name["nameId"],
                            "titleId": known_for_title.strip(),
                            "relationType": RelationType.KNOWN_FOR.value
                        })
        if ingestion_type == "principals":
            for titlePrincipals in iterate_over_file("title.principals.tsv", schema.read_title_principals_line):
                for nameId in titlePrincipals["nameIds"]:
                    titleId = titlePrincipals["titleId"] 
                    if (titleId, nameId) not in title_name_ids:
                        schema.store_title_name(cursor, {
                            "nameId": nameId.strip(),
                            "titleId": titlePrincipals["titleId"],
                            "relationType": RelationType.PRINCIPAL.value
                        })

if __name__ == "__main__":
    main(**configure().__dict__)
