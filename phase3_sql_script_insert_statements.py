
import pandas as pd

# Load cleaned datasets
data22 = pd.read_csv("cleaned_data_2022.csv")
data23 = pd.read_csv("cleaned_data_2023.csv")
data24 = pd.read_csv("cleaned_data_2024.csv")

def format_value(value):
    if pd.isna(value):
        return "NULL"
    if isinstance(value, str):
        value = value.strip().replace("'", "''")
        return f"'{value}'"
    return str(value)

def clean_number(value):
    if pd.isna(value):
        return None
    if isinstance(value, str):
        value = value.replace(",", "").strip()
        if value == "":
            return None
    return value

def split_artists(artist_string):
    if pd.isna(artist_string):
        return []
    return [a.strip() for a in str(artist_string).split(",") if a.strip() != ""]

# Collect unique artists
artist_set = set()

for _, row in data22.iterrows():
    for artist in split_artists(row["artist_names"]):
        artist_set.add(artist)

for _, row in data23.iterrows():
    for artist in split_artists(row["artist_names"]):
        artist_set.add(artist)

for _, row in data24.iterrows():
    for artist in split_artists(row["artist_names"]):
        artist_set.add(artist)

artist_list = sorted(list(artist_set))

with open("load_phase3_reduced.sql", "w", encoding="utf-8") as f:

    f.write("SET DEFINE OFF;\n\n")

    tables = [
        "PerformedBy2024", "PerformedBy2023", "PerformedBy2022",
        "Song2024", "Song2023", "Song2022", "Artist"
    ]

    for table in tables:
        f.write("BEGIN\n")
        f.write(f"  EXECUTE IMMEDIATE 'DROP TABLE {table} CASCADE CONSTRAINTS';\n")
        f.write("EXCEPTION\n")
        f.write("  WHEN OTHERS THEN\n")
        f.write("    IF SQLCODE != -942 THEN RAISE; END IF;\n")
        f.write("END;\n")
        f.write("/\n\n")

    f.write("PURGE RECYCLEBIN;\n\n")

    f.write("""
CREATE TABLE Artist (
    artist_name VARCHAR2(300) PRIMARY KEY
);

CREATE TABLE Song2022 (
    uri VARCHAR2(200) PRIMARY KEY,
    track_name VARCHAR2(300) NOT NULL,
    artist_names VARCHAR2(500) NOT NULL,
    peak_rank NUMBER NOT NULL,
    weeks_on_chart NUMBER NOT NULL,
    danceability NUMBER NOT NULL,
    energy NUMBER NOT NULL,
    tempo NUMBER NOT NULL,
    loudness NUMBER NOT NULL,
    acousticness NUMBER NOT NULL,
    speechiness NUMBER NOT NULL
);

CREATE TABLE Song2023 (
    track_name VARCHAR2(300) NOT NULL,
    artist_names VARCHAR2(500) NOT NULL,
    streams NUMBER NOT NULL,
    PRIMARY KEY (track_name, artist_names)
);

CREATE TABLE Song2024 (
    track_name VARCHAR2(300) NOT NULL,
    artist_names VARCHAR2(500) NOT NULL,
    spotify_streams NUMBER NOT NULL,
    PRIMARY KEY (track_name, artist_names)
);

CREATE TABLE PerformedBy2022 (
    uri VARCHAR2(200) NOT NULL,
    artist_name VARCHAR2(300) NOT NULL,
    PRIMARY KEY (uri, artist_name),
    FOREIGN KEY (uri) REFERENCES Song2022(uri),
    FOREIGN KEY (artist_name) REFERENCES Artist(artist_name)
);

CREATE TABLE PerformedBy2023 (
    track_name VARCHAR2(300) NOT NULL,
    artist_names VARCHAR2(500) NOT NULL,
    artist_name VARCHAR2(300) NOT NULL,
    PRIMARY KEY (track_name, artist_names, artist_name),
    FOREIGN KEY (track_name, artist_names) REFERENCES Song2023(track_name, artist_names),
    FOREIGN KEY (artist_name) REFERENCES Artist(artist_name)
);

CREATE TABLE PerformedBy2024 (
    track_name VARCHAR2(300) NOT NULL,
    artist_names VARCHAR2(500) NOT NULL,
    artist_name VARCHAR2(300) NOT NULL,
    PRIMARY KEY (track_name, artist_names, artist_name),
    FOREIGN KEY (track_name, artist_names) REFERENCES Song2024(track_name, artist_names),
    FOREIGN KEY (artist_name) REFERENCES Artist(artist_name)
);

""")

    # Insert artists
    for artist in artist_list:
        f.write(f"INSERT INTO Artist VALUES ({format_value(artist)});\n")

    f.write("\n")

    # Insert Song2022 + relationship
    for _, row in data22.iterrows():
        if pd.isna(row["uri"]) or pd.isna(row["track_name"]) or pd.isna(row["artist_names"]):
            continue

        sql = f"""INSERT INTO Song2022 VALUES (
{format_value(row["uri"])},
{format_value(row["track_name"])},
{format_value(row["artist_names"])},
{format_value(row["peak_rank"])},
{format_value(row["weeks_on_chart"])},
{format_value(row["danceability"])},
{format_value(row["energy"])},
{format_value(row["tempo"])},
{format_value(row["loudness"])},
{format_value(row["acousticness"])},
{format_value(row["speechiness"])}
);\n"""
        f.write(sql)

        for artist in split_artists(row["artist_names"]):
            f.write(f"INSERT INTO PerformedBy2022 VALUES ({format_value(row['uri'])}, {format_value(artist)});\n")

    f.write("\n")

    # Insert Song2023
    for _, row in data23.iterrows():
        if pd.isna(row["track_name"]) or pd.isna(row["artist_names"]) or pd.isna(row["streams"]):
            continue

        sql = f"""INSERT INTO Song2023 VALUES (
{format_value(row["track_name"])},
{format_value(row["artist_names"])},
{format_value(clean_number(row["streams"]))}
);\n"""
        f.write(sql)

        for artist in split_artists(row["artist_names"]):
            f.write(f"INSERT INTO PerformedBy2023 VALUES ({format_value(row['track_name'])}, {format_value(row['artist_names'])}, {format_value(artist)});\n")

    f.write("\n")

    # Insert Song2024
    for _, row in data24.iterrows():
        if pd.isna(row["track_name"]) or pd.isna(row["artist_names"]) or pd.isna(row["spotify_streams"]):
            continue

        sql = f"""INSERT INTO Song2024 VALUES (
{format_value(row["track_name"])},
{format_value(row["artist_names"])},
{format_value(clean_number(row["spotify_streams"]))}
);\n"""
        f.write(sql)

        for artist in split_artists(row["artist_names"]):
            f.write(f"INSERT INTO PerformedBy2024 VALUES ({format_value(row['track_name'])}, {format_value(row['artist_names'])}, {format_value(artist)});\n")

    f.write("\nCOMMIT;\n")

print("SQL file successfully generated: load_phase3_reduced.sql")
