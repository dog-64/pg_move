CREATE OR REPLACE PROCEDURE p_assert(sql_query text)
LANGUAGE plpgsql AS
$$
DECLARE
    query_result BOOLEAN;
BEGIN
    EXECUTE sql_query INTO query_result;

    IF NOT query_result THEN
        RAISE EXCEPTION 'ASSERTION FAILED: %', sql_query;
    ELSE
        RAISE NOTICE 'OK';
    END IF;
END;
$$;
