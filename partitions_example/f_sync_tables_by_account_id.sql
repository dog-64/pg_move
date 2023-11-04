CREATE OR REPLACE FUNCTION f_sync_tables_by_account_id() RETURNS trigger
    LANGUAGE plpgsql
AS
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
        query := FORMAT(
            $i$
            INSERT INTO $1%I ($2%s) 
            SELECT $2%s FROM $3%I WHERE id = $4%L 
            ON CONFLICT (id) 
            DO UPDATE SET ($2%s) = (SELECT $2%s FROM $1%I) 
                WHERE $1%I.id = $4%L
            $i$, 
            target_table, --1 
            column_list, -- 2 
            source_table, -- 3 
            NEW.id -- 4
            );
    ELSIF TG_OP = 'UPDATE' THEN
        query := FORMAT(
            $i$
            UPDATE $1%s SET ($2%s) = (SELECT $2%s FROM $3%s WHERE id = $4%s)
                WHERE id = $5%s 
            ON CONFLICT (id) 
            DO UPDATE SET ($2%s) = (SELECT $2%s FROM excluded) 
                WHERE $1%s.id = excluded.id',
            $i$, 
            target_table, --1
            column_list, -- 2
            source_table, -- 3 
            NEW.id, -- 4
            OLD.id -- 5
            );
    ELSIF TG_OP = 'DELETE' THEN
        query := FORMAT('DELETE FROM %s WHERE id = %s', target_table, OLD.id);
    END IF;

    EXECUTE query;

    RETURN NULL;
END;
$$;

ALTER FUNCTION f_sync_tables_by_account_id() OWNER TO postgres;
