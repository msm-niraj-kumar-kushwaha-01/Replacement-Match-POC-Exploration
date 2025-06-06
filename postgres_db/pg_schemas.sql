-- Drop the table if it already exists
-- DROP TABLE IF EXISTS partnumber_input_table;
CREATE TABLE partnumber_input_table (
    S_N BIGINT GENERATED ALWAYS AS IDENTITY,
    partnumber TEXT NOT NULL,
    category TEXT NOT NULL,
    brand TEXT NOT NULL,
    Static_Part TEXT NOT NULL,
    Regex_partnumber TEXT NOT NULL,
    Ranges_partnumber JSONB NOT NULL,
    partnumber_Type_input TEXT NOT NULL,
    Env_value TEXT NOT NULL,
    Specs JSONB NOT NULL,
    Mapper JSONB NOT NULL,
    PRIMARY KEY (partnumber, brand, category)
);

-- Create a B-tree index on Static_Part
CREATE INDEX idx_static_part ON partnumber_input_table (Static_Part);

CREATE INDEX idx_partnumber_Type_input ON partnumber_input_table (partnumber_Type_input);


-- DROP TABLE IF EXISTS brand_type_mappings;

CREATE TABLE type_input_table (
    type_input TEXT NOT NULL,
    type_output TEXT NOT NULL,
    Specs_to_be_Matched TEXT NOT NULL,
    PRIMARY KEY (type_input, type_output)
);

CREATE INDEX idx_type_input ON type_input_table (type_input);




CREATE OR REPLACE FUNCTION notify_part_events() RETURNS trigger AS $$
DECLARE
    payload JSON;
BEGIN
    IF TG_OP = 'INSERT' THEN
        payload := json_build_object(
            'action', TG_OP,
            'partnumber', NEW.partnumber,
            'brand',      NEW.brand,
            'category',   NEW.category
        );
    ELSIF TG_OP = 'UPDATE' THEN
        payload := json_build_object(
            'action', TG_OP,
            'partnumber', NEW.partnumber,
            'brand',      NEW.brand,
            'category',   NEW.category
        );
    ELSIF TG_OP = 'DELETE' THEN
        payload := json_build_object(
            'action', TG_OP,
            'partnumber', OLD.partnumber,
            'brand',      OLD.brand,
            'category',   OLD.category
        );
    ELSE
        RETURN NULL;
    END IF;

    -- Send the payload on channel 'part_events'
    PERFORM pg_notify('part_events', payload::text);
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;



-- 2) Drop any existing triggers (optional, to avoid duplicates)
DROP TRIGGER IF EXISTS trg_part_insert   ON partnumber_input_table;
DROP TRIGGER IF EXISTS trg_part_update   ON partnumber_input_table;
DROP TRIGGER IF EXISTS trg_part_delete   ON partnumber_input_table;

-- 3) Create triggers for INSERT, UPDATE, DELETE
CREATE TRIGGER trg_part_insert
AFTER INSERT ON partnumber_input_table
FOR EACH ROW
EXECUTE PROCEDURE notify_part_events();

CREATE TRIGGER trg_part_update
AFTER UPDATE ON partnumber_input_table
FOR EACH ROW
EXECUTE PROCEDURE notify_part_events();

CREATE TRIGGER trg_part_delete
AFTER DELETE ON partnumber_input_table
FOR EACH ROW
EXECUTE PROCEDURE notify_part_events();



CREATE OR REPLACE FUNCTION notify_type_input_events() RETURNS trigger AS $$
DECLARE
    payload JSON;
BEGIN
    IF TG_OP = 'INSERT' THEN
        payload := json_build_object(
            'action', TG_OP,
            'type_input', NEW.type_input,
            'type_output', NEW.type_output,
            'Specs_to_be_Matched', NEW.Specs_to_be_Matched
        );
    ELSIF TG_OP = 'UPDATE' THEN
        payload := json_build_object(
            'action', TG_OP,
            'type_input', NEW.type_input,
            'type_output', NEW.type_output,
            'Specs_to_be_Matched', NEW.Specs_to_be_Matched
        );
    ELSIF TG_OP = 'DELETE' THEN
        payload := json_build_object(
            'action', TG_OP,
            'type_input', OLD.type_input,
            'type_output', OLD.type_ou  tput,
            'Specs_to_be_Matched', OLD.Specs_to_be_Matched
        );
    ELSE
        RETURN NULL;
    END IF;

    PERFORM pg_notify('type_input_events', payload::text);
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;


DROP TRIGGER IF EXISTS trg_type_input_insert ON type_input_table;
DROP TRIGGER IF EXISTS trg_type_input_update ON type_input_table;
DROP TRIGGER IF EXISTS trg_type_input_delete ON type_input_table;

CREATE TRIGGER trg_type_input_insert
AFTER INSERT ON type_input_table
FOR EACH ROW
EXECUTE PROCEDURE notify_type_input_events();

CREATE TRIGGER trg_type_input_update
AFTER UPDATE ON type_input_table
FOR EACH ROW
EXECUTE PROCEDURE notify_type_input_events();

CREATE TRIGGER trg_type_input_delete
AFTER DELETE ON type_input_table
FOR EACH ROW
EXECUTE PROCEDURE notify_type_input_events();



CREATE TABLE partnumber_mapping_table (
    partnumber_input TEXT NOT NULL,
    brand_input TEXT NOT NULL,
    partnumber_output TEXT NOT NULL,
    brand_output TEXT NOT NULL,
    partnumber_output_specs TEXT NOT NULL,
    PRIMARY KEY (partnumber_input, brand_input, partnumber_output, brand_output)
);

-- Create a balanced tree (B-tree) index on partnumber_input
CREATE INDEX idx_partnumber_input ON partnumber_mapping_table USING btree (partnumber_input);



CREATE OR REPLACE FUNCTION get_partnumbers_by_type_input(p_type_input TEXT)
RETURNS TABLE (
    partnumber TEXT,
    specs JSONB
)
AS $$
BEGIN
    RETURN QUERY
    SELECT
        p.partnumber,
        p.Specs
    FROM
        partnumber_input_table p
    WHERE
        p.partnumber_Type_input = p_type_input;
END;
$$ LANGUAGE plpgsql;
