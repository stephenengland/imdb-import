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
from batch_iterator import batch_iterator


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

def iterate_over_principals():
    for titlePrincipals in iterate_over_file("title.principals.tsv", schema.read_title_principals_line):
        for name_id in titlePrincipals["nameIds"]:
            name_id = name_id.strip()
            title_id = titlePrincipals["titleId"].strip()
            
            yield {
                "nameId": name_id,
                "titleId": title_id,
                "relationType": RelationType.PRINCIPAL.value
            }

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
        total = 0

        def title_exists(title):
            return title["titleId"] in title_ids

        if ingestion_type == "titles":
            for titles in batch_iterator(iterate_over_file("title.basics.tsv", schema.read_title_line)):
                schema.store_titles(cursor, titles)
                total += len(titles)
                if total % 100000 == 0:
                    print("Titles inserted: " + str(total))
            return

        if ingestion_type == "ratings":
            cursor.execute("truncate imdb.titleRatingsIngestion")
            
            for ratings in batch_iterator(iterate_over_file("title.ratings.tsv", schema.read_title_ratings_line)):
                schema.store_title_ratings_ingestion(cursor, ratings)

            for i in range(100):
                logger.info("Running title ratings ingestion insert partition :" + str(i))
                cursor.execute("""
                    insert into imdb.titleRatings (titleId, averageRating, numVotes) 
                    select i.titleId, i.averageRating, i.numVotes
                    from imdb.titleRatingsIngestion i
                        inner join imdb.titleBasics tb
                            on tb.titleId = i.titleId
                    where i.numVotes %% 100 = %(iterator_i)s
                    ON CONFLICT (titleId)
                    DO UPDATE SET
                        averageRating = excluded.averageRating,
                        numVotes = excluded.numVotes;
                """, {
                    "iterator_i": i
                })
            
            cursor.execute("truncate imdb.titleRatingsIngestion")

            return

        if ingestion_type == "names":
            cursor.execute("truncate imdb.titleNameIngestion")
            for names in batch_iterator(iterate_over_file("name.basics.tsv", schema.read_name_line)):
                schema.store_names(cursor, names)

                known_for_title_ids = [{
                    "titleId": known_for_title.strip(), 
                    "nameId": name["nameId"],
                    "relationType": RelationType.KNOWN_FOR.value
                } for name in names for known_for_title in name["knownForTitles"]]

                for name_title_ids in batch_iterator(known_for_title_ids):
                    schema.store_title_names_ingestion(cursor, name_title_ids)

                total += len(names)
                if total % 100000 == 0:
                    print("Names inserted: " + str(total))
            
            for i in range(1000):
                logger.info("Running title name ingestion insert partition :" + str(i))
                cursor.execute("""
                    insert into imdb.titleName (titleId, nameId, relationType)
                    select i.titleId, i.nameId, i.relationType
                    from imdb.titleNameIngestion i
                        inner join imdb.titleBasics tb
                            on tb.titleId = i.titleId
                        inner join imdb.nameBasics nb
                            on nb.nameId = i.nameId
                    where i.id %% 1000 = %(iterator_i)s
                    ON CONFLICT (titleId, nameId, relationType) DO NOTHING;
                """, {
                    "iterator_i": i
                })
            
            cursor.execute("truncate imdb.titleNameIngestion")
            return

        if ingestion_type == "principals":
            cursor.execute("truncate imdb.titleNameIngestion")

            for title_principals in batch_iterator(iterate_over_principals()):
                schema.store_title_names_ingestion(cursor, title_principals)
            
            for i in range(1000):
                logger.info("Running title name ingestion insert partition :" + str(i))
                cursor.execute("""
                    insert into imdb.titleName (titleId, nameId, relationType)
                    select i.titleId, i.nameId, i.relationType
                    from imdb.titleNameIngestion i
                        inner join imdb.titleBasics tb
                            on tb.titleId = i.titleId
                        inner join imdb.nameBasics nb
                            on nb.nameId = i.nameId
                    where i.id %% 1000 = %(iterator_i)s
                    ON CONFLICT (titleId, nameId, relationType) DO NOTHING;
                """, {
                    "iterator_i": i
                })
            
            cursor.execute("truncate imdb.titleNameIngestion")

if __name__ == "__main__":
    main(**configure().__dict__)
