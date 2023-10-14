CREATE OR REPLACE FUNCTION f_sync_tables_by_account_id()
    RETURNS TRIGGER AS
$$
DECLARE
    source_table       text;
    target_table       text;
    column_list        text;
    query              text;
    account_ids        bigint[];
    current_account_id bigint;
BEGIN
    source_table := TG_ARGV[0];
    target_table := TG_ARGV[1];
    -- TODO: возможно тут нужно уметь передавать диапазоны
    account_ids := TG_ARGV[2:];

    IF TG_OP = 'DELETE' THEN
        current_account_id := OLD.account_id;
    ELSE
        current_account_id := NEW.account_id;
    END IF;

    IF NOT current_account_id = ANY (account_ids) THEN
        RETURN NULL;
    END IF;

    -- Получаем список колонок
    SELECT STRING_AGG(QUOTE_IDENT(column_name), ', ')
    INTO column_list
    FROM information_schema.columns
    WHERE table_name = source_table;

    -- Формируем SQL-запрос в зависимости от операции
    IF TG_OP = 'INSERT' THEN
        query := FORMAT('INSERT INTO %s (%s) SELECT %s FROM %s WHERE id = %s', target_table, column_list, column_list,
                        source_table, NEW.id);
    ELSIF TG_OP = 'UPDATE' THEN
        query := FORMAT('UPDATE %s SET (%s) = (SELECT %s FROM %s WHERE id = %s) WHERE id = %s', target_table,
                        column_list, column_list, source_table, NEW.id, OLD.id);
    ELSIF TG_OP = 'DELETE' THEN
        query := FORMAT('DELETE FROM %s WHERE id = %s', target_table, OLD.id);
    END IF;

    EXECUTE query;

    RETURN NULL;
END;
$$ LANGUAGE plpgsql;
