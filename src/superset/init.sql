-- Superset görselleştirme (Spark bu tablolara da yazar).
CREATE TABLE IF NOT EXISTS tomtom_predictions (
    batch_id BIGINT,
    sample_id BIGINT,
    location_name TEXT,
    prediction TEXT,
    capture_time TEXT,
    lat DOUBLE PRECISION,
    lon DOUBLE PRECISION,
    day INTEGER,
    current_speed DOUBLE PRECISION,
    free_flow_speed DOUBLE PRECISION,
    current_travel_time DOUBLE PRECISION,
    free_flow_travel_time DOUBLE PRECISION,
    confidence DOUBLE PRECISION,
    hour INTEGER,
    minute INTEGER,
    day_of_week INTEGER,
    is_weekend INTEGER
);

CREATE TABLE IF NOT EXISTS tomtom_preprocessed (
    batch_id BIGINT,
    sample_id BIGINT,
    location_name TEXT,
    capture_time TEXT,
    lat DOUBLE PRECISION,
    lon DOUBLE PRECISION,
    day INTEGER,
    current_speed DOUBLE PRECISION,
    free_flow_speed DOUBLE PRECISION,
    current_travel_time DOUBLE PRECISION,
    free_flow_travel_time DOUBLE PRECISION,
    confidence DOUBLE PRECISION,
    hour INTEGER,
    minute INTEGER,
    day_of_week INTEGER,
    is_weekend INTEGER
);
