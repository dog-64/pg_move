CREATE OR REPLACE PROCEDURE p_assert(query text)
LANGUAGE plpgsql AS
$$
DECLARE
    result BOOLEAN;
BEGIN
    EXECUTE query INTO result;
    IF NOT result THEN
        RAISE EXCEPTION 'Assertion failed: %', query;
    ELSE
        RAISE NOTICE 'OK: %', query;
    END IF;
END;
$$;
