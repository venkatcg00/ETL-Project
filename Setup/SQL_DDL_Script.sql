-- TABLE 1: CSD_SOURCES
CREATE TABLE IF NOT EXISTS CSD_SOURCES (
    SOURCE_ID              INTEGER         NOT NULL        PRIMARY KEY AUTOINCREMENT,
    SOURCE_NAME            TEXT            NOT NULL,
    SOURCE_FILE_TYPE       TEXT            NOT NULL,
    DATA_LOAD_STRATEGY     TEXT            NOT NULL,
    LAST_LOADED_RECORD_ID  INTEGER,
    DESCRIPTION            TEXT,
    ACTIVE_FLAG            INTEGER         NOT NULL,
    START_DATE             TEXT            NOT NULL,
    END_DATE               TEXT            NOT NULL,
    UNIQUE                 (SOURCE_NAME)
);

-- TABLE 2: CSD_SUPPORT_AREAS
CREATE TABLE IF NOT EXISTS CSD_SUPPORT_AREAS (
    SUPPORT_AREA_ID        INTEGER         NOT NULL        PRIMARY KEY AUTOINCREMENT,
    SUPPORT_AREA_NAME      TEXT            NOT NULL,
    SOURCE_ID              INTEGER         NOT NULL,
    DESCRIPTION            TEXT,
    ACTIVE_FLAG            INTEGER         NOT NULL,
    START_DATE             TEXT            NOT NULL,
    END_DATE               TEXT            NOT NULL,
    FOREIGN KEY            (SOURCE_ID) REFERENCES CSD_SOURCES(SOURCE_ID)
);

-- TABLE 3: CSD_AGENTS
CREATE TABLE IF NOT EXISTS CSD_AGENTS (
    AGENT_ID               INTEGER         NOT NULL        PRIMARY KEY AUTOINCREMENT,
    FIRST_NAME             TEXT            NOT NULL,
    MIDDLE_NAME            TEXT,
    LAST_NAME              TEXT            NOT NULL,
    PSEUDO_CODE            TEXT            NOT NULL,
    SOURCE_ID              INTEGER         NOT NULL,
    ACTIVE_FLAG            INTEGER         NOT NULL,
    START_DATE             TEXT            NOT NULL,
    END_DATE               TEXT            NOT NULL,
    UNIQUE                 (PSEUDO_CODE),
    FOREIGN KEY            (SOURCE_ID) REFERENCES CSD_SOURCES(SOURCE_ID)
);

-- TABLE 4: CSD_CUSTOMER_TYPES
CREATE TABLE IF NOT EXISTS CSD_CUSTOMER_TYPES (
    CUSTOMER_TYPE_ID       INTEGER         NOT NULL        PRIMARY KEY AUTOINCREMENT,
    CUSTOMER_TYPE_NAME     TEXT            NOT NULL,
    SOURCE_ID              INTEGER         NOT NULL,
    DESCRIPTION            TEXT,
    ACTIVE_FLAG            INTEGER         NOT NULL,
    START_DATE             TEXT            NOT NULL,
    END_DATE               TEXT            NOT NULL,
    FOREIGN KEY            (SOURCE_ID) REFERENCES CSD_SOURCES(SOURCE_ID)
);

-- TABLE 5: CSD_TABLE_TYPES
CREATE TABLE IF NOT EXISTS CSD_TABLE_TYPES (
    TABLE_TYPE_ID          INTEGER         NOT NULL        PRIMARY KEY AUTOINCREMENT,
    TABLE_TYPE_NAME        TEXT            NOT NULL,
    DESCRIPTION            TEXT,
    ACTIVE_FLAG            INTEGER         NOT NULL,
    START_DATE             TEXT            NOT NULL,
    END_DATE               TEXT            NOT NULL,
    UNIQUE                 (TABLE_TYPE_NAME)
);

-- TABLE 6: CSD_TABLE_LOAD_TYPES
CREATE TABLE IF NOT EXISTS CSD_TABLE_LOAD_TYPES (
    TABLE_LOAD_TYPE_ID     INTEGER         NOT NULL        PRIMARY KEY AUTOINCREMENT,
    TABLE_LOAD_TYPE_NAME   TEXT            NOT NULL,
    DESCRIPTION            TEXT,
    ACTIVE_FLAG            INTEGER         NOT NULL,
    START_DATE             TEXT            NOT NULL,
    END_DATE               TEXT            NOT NULL,
    UNIQUE                 (TABLE_LOAD_TYPE_NAME)
);

-- TABLE 7: CSD_DATA_DICTIONARY
CREATE TABLE IF NOT EXISTS CSD_DATA_DICTIONARY (
    DATA_DICTIONARY_ID     INTEGER         NOT NULL        PRIMARY KEY AUTOINCREMENT,
    SOURCE_ID              INTEGER         NOT NULL,
    FIELD_NAME             TEXT            NOT NULL,
    DATA_TYPE              TEXT            NOT NULL,
    VALUES_ALLOWED         TEXT            NOT NULL,
    DESCRIPTION            TEXT,
    EXAMPLE                TEXT,
    ACTIVE_FLAG            INTEGER         NOT NULL,
    START_DATE             TEXT            NOT NULL,
    END_DATE               TEXT            NOT NULL,
    FOREIGN KEY            (SOURCE_ID) REFERENCES CSD_SOURCES(SOURCE_ID)
);

-- TABLE 8: CSD_DATA_LOADS
CREATE TABLE IF NOT EXISTS CSD_DATA_LOADS (
    DATA_LOAD_ID           INTEGER         NOT NULL        PRIMARY KEY AUTOINCREMENT,
    DAG_ID                 TEXT,
    LOAD_STATUS            TEXT            NOT NULL,
    LOAD_START_DATE        TEXT            NOT NULL,
    LOAD_END_DATE          TEXT,
    TOTAL_UPSERT_COUNT     INTEGER,
    VALID_COUNT            INTEGER,
    INVALID_COUNT          INTEGER,
    DATA_VALID_PERCENTAGE  INTEGER,
    LOAD_DURATION          TEXT,
    UNIQUE                 (DAG_ID)
);

-- TABLE 9: CSD_TABLE_NAMES
CREATE TABLE IF NOT EXISTS CSD_TABLE_NAMES (
    TABLE_ID               INTEGER         NOT NULL        PRIMARY KEY AUTOINCREMENT,
    TABLE_NAME             TEXT            NOT NULL,
    TABLE_TYPE_ID          INTEGER         NOT NULL,
    DESCRIPTION            TEXT,
    TABLE_LOAD_TYPE_ID     INTEGER         NOT NULL,
    ACTIVE_FLAG            INTEGER         NOT NULL,
    START_DATE             TEXT            NOT NULL,
    END_DATE               TEXT            NOT NULL,
    UNIQUE                 (TABLE_NAME),
    FOREIGN KEY            (TABLE_TYPE_ID) REFERENCES CSD_TABLE_TYPES(TABLE_TYPE_ID),
    FOREIGN KEY            (TABLE_LOAD_TYPE_ID) REFERENCES CSD_TABLE_LOAD_TYPES(TABLE_LOAD_TYPE_ID)
);

-- TABLE 10: CSD_TABLE_COLUMNS
CREATE TABLE IF NOT EXISTS CSD_TABLE_COLUMNS (
    TABLE_COLUMN_ID        INTEGER         NOT NULL        PRIMARY KEY AUTOINCREMENT,
    TABLE_ID               INTEGER         NOT NULL,
    COLUMN_NAME            TEXT            NOT NULL,
    DATA_TYPE              TEXT            NOT NULL,
    FIELD_LENGTH           INTEGER,
    DESCRIPTION            TEXT,
    ACTIVE_FLAG            INTEGER         NOT NULL,
    START_DATE             TEXT            NOT NULL,
    END_DATE               TEXT            NOT NULL,
    FOREIGN KEY            (TABLE_ID) REFERENCES CSD_TABLE_NAMES(TABLE_ID)
);

-- TABLE 11: CSD_DATA_MART
CREATE TABLE IF NOT EXISTS CSD_DATA_MART (
    CSD_ID                 INTEGER         NOT NULL        PRIMARY KEY AUTOINCREMENT,
    SOURCE_ID              INTEGER         NOT NULL,
    SOURCE_SYSTEM_IDENTIFIER TEXT          NOT NULL,
    AGENT_ID               INTEGER,
    INTERACTION_DATE       TEXT,
    SUPPORT_AREA_ID        INTEGER,
    INTERACTION_STATUS     TEXT,
    INTERACTION_TYPE       TEXT,
    CUSTOMER_TYPE_ID       INTEGER,
    HANDLE_TIME            INTEGER,
    WORK_TIME              INTEGER,
    FIRST_CONTACT_RESOLUTION INTEGER,
    QUERY_STATUS           TEXT,
    SOLUTION_TYPE           TEXT,
    CUSTOMER_RATING        INTEGER,
    SOURCE_HASH_KEY        TEXT            NOT NULL,
    DATA_LOAD_ID           INTEGER         NOT NULL,
    IS_VALID_DATA          INTEGER         NOT NULL,
    ACTIVE_FLAG            INTEGER         NOT NULL,
    START_DATE             TEXT            NOT NULL,
    END_DATE               TEXT            NOT NULL,
    FOREIGN KEY            (SOURCE_ID) REFERENCES CSD_SOURCES(SOURCE_ID),
    FOREIGN KEY            (AGENT_ID) REFERENCES CSD_AGENTS(AGENT_ID),
    FOREIGN KEY            (SUPPORT_AREA_ID) REFERENCES CSD_SUPPORT_AREAS(SUPPORT_AREA_ID),
    FOREIGN KEY            (CUSTOMER_TYPE_ID) REFERENCES CSD_CUSTOMER_TYPES(CUSTOMER_TYPE_ID),
    FOREIGN KEY            (DATA_LOAD_ID) REFERENCES CSD_DATA_LOADS(DATA_LOAD_ID)
);


-- TABLE 12: STREAMING_DATA_ARCHIVE
CREATE TABLE IF NOT EXISTS STREAMING_DATA_ARCHIVE (
    ARCHIVE_ID                  INTEGER             NOT NULL        PRIMARY KEY AUTOINCREMENT,
    STREAM_RECORD_ID            INTEGER             NOT NULL,
    STREAMING_DATA              TEXT                NOT NULL
);