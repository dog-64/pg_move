CREATE OR REPLACE PROCEDURE p_assert(query_result BOOLEAN)
LANGUAGE plpgsql AS
$$
BEGIN
    IF NOT query_result THEN
        RAISE EXCEPTION 'Assertion failed';
    ELSE
        RAISE NOTICE 'OK';
    END IF;
END;
$$;
