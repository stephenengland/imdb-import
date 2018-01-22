from psycopg2.extras import execute_values

def column_or_null(column):
    if column == "\\N":
        return None

    return column

def split_if_not_null(column):
    if column:
        return column.split(',')

    return None

def read_title_line(line):
    columns = line.split('\t')
    
    return {
        "titleId": columns[0],
        "titleType": column_or_null(columns[1]),
        "primaryTitle": column_or_null(columns[2]),
        "originalTitle": column_or_null(columns[3]),
        "isAdult": column_or_null(columns[4]),
        "startYear": column_or_null(columns[5]),
        "endYear": column_or_null(columns[6]),
        "runtimeMinutes": column_or_null(columns[7]),
        "genres": column_or_null(columns[8])
    }

def read_name_line(line):
    columns = line.split('\t')
    
    return {
        "nameId": columns[0],
        "primaryName": column_or_null(columns[1]),
        "birthYear": column_or_null(columns[2]),
        "deathYear": column_or_null(columns[3]),
        "primaryProfession": split_if_not_null(column_or_null(columns[4])),
        "knownForTitles": split_if_not_null(column_or_null(columns[5]))
    }

def read_title_principals_line(line):
    columns = line.split('\t')
    
    return {
        "titleId": columns[0],
        "nameIds": split_if_not_null(columns[1])
    }

def read_title_ratings_line(line):
    columns = line.split('\t')

    return {
        "titleId": columns[0],
        "averageRating": columns[1],
        "numVotes": columns[2]
    }

def iterate_over_title_ids(cursor):
    cursor.execute("""
        select titleId
        from imdb.titleBasics
    """)
    
    for row in cursor:
        yield row["titleid"]

def iterate_over_name_ids(cursor):
    cursor.execute("""
        select nameId
        from imdb.nameBasics
    """)
    
    for row in cursor:
        yield row["nameid"]

def iterate_over_title_name_ids(cursor):
    cursor.execute("""
        select titleId, nameId, relationType
        from imdb.titleName
    """)
    
    for row in cursor:
        yield (row["titleid"], row["nameid"], row["relationtype"])

def store_title(cursor, result):
    cursor.execute("""
        INSERT INTO imdb.titleBasics (titleId, titleType, primaryTitle, originalTitle, isAdult, startYear, endYear, runtimeMinutes, genres)
        VALUES (%(titleId)s, %(titleType)s, %(primaryTitle)s, %(originalTitle)s, %(isAdult)s, %(startYear)s, %(endYear)s, %(runtimeMinutes)s, %(genres)s)
        ON CONFLICT (titleId) DO UPDATE SET
        titleType = %(titleType)s,
        primaryTitle = %(primaryTitle)s,
        originalTitle = %(originalTitle)s,
        isAdult = %(isAdult)s,
        startYear = %(startYear)s,
        endYear = %(endYear)s,
        runtimeMinutes = %(runtimeMinutes)s,
        genres = %(genres)s;
    """, result)

def store_titles(cursor, results):
    execute_values(cursor,
        "INSERT INTO imdb.titleBasics (titleId, titleType, primaryTitle, originalTitle, isAdult, startYear, endYear, runtimeMinutes, genres) VALUES %s ON CONFLICT (titleId) DO NOTHING",
        generate_tuple_values(["titleId", "titleType", "primaryTitle", "originalTitle", "isAdult", "startYear", "endYear", "runtimeMinutes", "genres"], results))

def store_name(cursor, result):
    cursor.execute("""
        INSERT INTO imdb.nameBasics (nameId, primaryName, birthYear, deathYear, primaryProfession)
        VALUES (%(nameId)s, %(primaryName)s, %(birthYear)s, %(deathYear)s, %(primaryProfession)s)
        ON CONFLICT (nameId) DO UPDATE SET
        primaryName = %(primaryName)s,
        birthYear = %(birthYear)s,
        deathYear = %(deathYear)s,
        primaryProfession = %(primaryProfession)s;
    """, result)

def store_names(cursor, results):
    execute_values(cursor,
        "INSERT INTO imdb.nameBasics (nameId, primaryName, birthYear, deathYear, primaryProfession) VALUES %s ON CONFLICT (nameId) DO NOTHING",
        generate_tuple_values(["nameId", "primaryName", "birthYear", "deathYear", "primaryProfession"], results))

def store_title_name(cursor, result):
    cursor.execute("""
        INSERT INTO imdb.titleName (nameId, titleId, relationType)
        VALUES (%(nameId)s, %(titleId)s, %(relationType)s)
        ON CONFLICT (titleId, nameId) DO NOTHING;
    """, result)

def store_title_names_ingestion(cursor, results):
    execute_values(cursor, "INSERT INTO imdb.titleNameIngestion (nameId, titleId, relationType) VALUES %s", generate_tuple_values(["nameId", "titleId", "relationType"], results))

def store_title_ratings_ingestion(cursor, results):
    execute_values(cursor, """
    INSERT INTO imdb.titleRatingsIngestion (titleId, averageRating, numVotes) 
    VALUES %s 
    ON CONFLICT (titleId)
    DO UPDATE SET
        averageRating = excluded.averageRating,
        numVotes = excluded.numVotes
    """, generate_tuple_values(["titleId", "averageRating", "numVotes"], results))

def generate_tuple_values(fields, results):
    return [tuple([result[field] for field in fields]) for result in results]
