-- основное копирование, после установки триггера

CREATE OR REPLACE FUNCTION f_copy_between_tables_by_accounts(source_table text, target_table text, VARIADIC account_ids bigint[])
    RETURNS void
    LANGUAGE plpgsql AS
$$
DECLARE
    column_list text;
    query       text;
BEGIN
    -- Получаем список колонок для копирования
    SELECT STRING_AGG(QUOTE_IDENT(column_name), ', ')
    INTO column_list
    FROM information_schema.columns
    WHERE table_name = source_table;

    -- Формируем и выполняем SQL-запрос для копирования данных
    query := FORMAT(
        $i$
            INSERT INTO %1$I (%2$s) 
            SELECT %2$s FROM %3$I WHERE account_id = ANY($1) 
            ON CONFLICT DO NOTHING
        $i$, 
        target_table,   -- 1
        column_list,    -- 2
        source_table    -- 3
        );
    EXECUTE query USING account_ids;
END;
$$;
