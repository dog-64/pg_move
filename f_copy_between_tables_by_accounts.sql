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
            'INSERT INTO %I (%s) SELECT %s FROM %I WHERE account_id = ANY($1) ON CONFLICT DO NOTHING',
            target_table,
            column_list,
            column_list,
            source_table
        );
    EXECUTE query USING account_ids;
END;
$$;
