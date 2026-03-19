import pandas as pd

# Load cleaned datasets
data22 = pd.read_csv("CleanedData/cleaned_data_2022.csv")
data23 = pd.read_csv("CleanedData/cleaned_data_2023.csv")
data24 = pd.read_csv("CleanedData/cleaned_data_2024.csv")


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

# ---------------------------------------------------
# 1. Build Song table from all unique song identities
#    Song PK = (track_name, artist_names)
# ---------------------------------------------------
song_cols = ["track_name", "artist_names"]

songs22 = data22[song_cols]
songs23 = data23[song_cols]
songs24 = data24[song_cols]

song_master = pd.concat([songs22, songs23, songs24], ignore_index=True).drop_duplicates()
song_master = song_master.reset_index(drop=True)

# ---------------------------------------------------
# 2. Add song_id to each year table as local PK only
# ---------------------------------------------------
data22["song_id"] = range(1, len(data22) + 1)
data23["song_id"] = range(1, len(data23) + 1)
data24["song_id"] = range(1, len(data24) + 1)

# ---------------------------------------------------
# 3. Generate SQL file
# ---------------------------------------------------
with open("load_phase3_reduced.sql", "w", encoding="utf-8") as f:

    f.write("SET DEFINE OFF;\n\n")

    tables = ["TOP_SONG_2024", "TOP_SONG_2023", "TOP_SONG_2022", "Song"]

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
CREATE TABLE Song (
    track_name VARCHAR2(300) NOT NULL,
    artist_names VARCHAR2(500) NOT NULL,
    PRIMARY KEY (track_name, artist_names)
);

CREATE TABLE TOP_SONG_2022 (
    song_id NUMBER PRIMARY KEY,
    track_name VARCHAR2(300) NOT NULL,
    artist_names VARCHAR2(500) NOT NULL,
    uri VARCHAR2(200) NOT NULL,
    peak_rank NUMBER NOT NULL,
    weeks_on_chart NUMBER NOT NULL,
    danceability NUMBER NOT NULL,
    energy NUMBER NOT NULL,
    music_key VARCHAR2(20) NOT NULL,
    loudness NUMBER NOT NULL,
    music_mode VARCHAR2(20) NOT NULL,
    speechiness NUMBER NOT NULL,
    acousticness NUMBER NOT NULL,
    tempo NUMBER NOT NULL,
    FOREIGN KEY (track_name, artist_names) REFERENCES Song(track_name, artist_names)
);

CREATE TABLE TOP_SONG_2023 (
    song_id NUMBER PRIMARY KEY,
    track_name VARCHAR2(300) NOT NULL,
    artist_names VARCHAR2(500) NOT NULL,
    streams NUMBER NOT NULL,
    bpm NUMBER NOT NULL,
    music_key VARCHAR2(20),
    music_mode VARCHAR2(20) NOT NULL,
    danceability_pct NUMBER NOT NULL,
    valence_pct NUMBER NOT NULL,
    energy_pct NUMBER NOT NULL,
    acousticness_pct NUMBER NOT NULL,
    instrumentalness_pct NUMBER NOT NULL,
    liveness_pct NUMBER NOT NULL,
    speechiness_pct NUMBER NOT NULL,
    FOREIGN KEY (track_name, artist_names) REFERENCES Song(track_name, artist_names)
);

CREATE TABLE TOP_SONG_2024 (
    song_id NUMBER PRIMARY KEY,
    track_name VARCHAR2(300) NOT NULL,
    artist_names VARCHAR2(500) NOT NULL,
    spotify_streams NUMBER NOT NULL,
    isrc VARCHAR2(50) NOT NULL,
    all_time_rank NUMBER,
    track_score NUMBER NOT NULL,
    FOREIGN KEY (track_name, artist_names) REFERENCES Song(track_name, artist_names)
);
""")

    # -----------------------------------------
    # Insert Song table
    # -----------------------------------------
    for _, row in song_master.iterrows():
        sql = f"""INSERT INTO Song VALUES (
{format_value(row["track_name"])},
{format_value(row["artist_names"])}
);\n"""
        f.write(sql)

    f.write("\n")

    # -----------------------------------------
    # Insert TOP_SONG_2022
    # -----------------------------------------
    for _, row in data22.iterrows():
        if pd.isna(row["track_name"]) or pd.isna(row["artist_names"]):
            continue

        sql = f"""INSERT INTO TOP_SONG_2022 VALUES (
{format_value(row["song_id"])},
{format_value(row["track_name"])},
{format_value(row["artist_names"])},
{format_value(row["uri"])},
{format_value(clean_number(row["peak_rank"]))},
{format_value(clean_number(row["weeks_on_chart"]))},
{format_value(clean_number(row["danceability"]))},
{format_value(clean_number(row["energy"]))},
{format_value(row["music_key"])},
{format_value(clean_number(row["loudness"]))},
{format_value(row["music_mode"])},
{format_value(clean_number(row["speechiness"]))},
{format_value(clean_number(row["acousticness"]))},
{format_value(clean_number(row["tempo"]))}
);\n"""
        f.write(sql)

    f.write("\n")

    # -----------------------------------------
    # Insert TOP_SONG_2023
    # -----------------------------------------
    for _, row in data23.iterrows():
        if pd.isna(row["track_name"]) or pd.isna(row["artist_names"]) or pd.isna(row["streams"]):
            continue

        sql = f"""INSERT INTO TOP_SONG_2023 VALUES (
{format_value(row["song_id"])},
{format_value(row["track_name"])},
{format_value(row["artist_names"])},
{format_value(clean_number(row["streams"]))},
{format_value(clean_number(row["bpm"]))},
{format_value(row["music_key"])},
{format_value(row["music_mode"])},
{format_value(clean_number(row["danceability"]))},
{format_value(clean_number(row["valence"]))},
{format_value(clean_number(row["energy"]))},
{format_value(clean_number(row["acousticness"]))},
{format_value(clean_number(row["instrumentalness"]))},
{format_value(clean_number(row["liveness"]))},
{format_value(clean_number(row["speechiness"]))}
);\n"""
        f.write(sql)

    f.write("\n")

    # -----------------------------------------
    # Insert TOP_SONG_2024
    # -----------------------------------------
    for _, row in data24.iterrows():
        if pd.isna(row["track_name"]) or pd.isna(row["artist_names"]) or pd.isna(row["spotify_streams"]):
            continue

        sql = f"""INSERT INTO TOP_SONG_2024 VALUES (
{format_value(row["song_id"])},
{format_value(row["track_name"])},
{format_value(row["artist_names"])},
{format_value(clean_number(row["spotify_streams"]))},
{format_value(row["isrc"])},
{format_value(clean_number(row["all_time_rank"]))},
{format_value(clean_number(row["track_score"]))}
);\n"""
        f.write(sql)

    f.write("\nCOMMIT;\n")

print("SQL file successfully generated: load_phase3_reduced.sql")


