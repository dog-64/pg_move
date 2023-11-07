CREATE OR REPLACE FUNCTION f_copy_tables(source_table text, target_table text) RETURNS void
    LANGUAGE plpgsql
AS
$$
DECLARE
    column_list        text; -- Список колонок
    query              text; -- Текст запроса
    set_clause         text; -- Условие SET для DO UPDATE
BEGIN
    -- Получаем список колонок
    SELECT STRING_AGG(QUOTE_IDENT(column_name), ', ')
    INTO column_list
    FROM information_schema.columns
    WHERE table_name = source_table AND table_schema = 'public'; -- Предполагается, что таблица находится в схеме public

    -- Формируем условие SET для DO UPDATE, используя синтаксис с подзапросом
    SELECT STRING_AGG(FORMAT('%1$I = excluded.%1$I', column_name), ', ')
    INTO set_clause
    FROM information_schema.columns
    WHERE table_name = target_table AND table_schema = 'public'; -- Предполагается, что таблица находится в схеме public

    -- Формируем SQL-запрос для копирования данных
    query := FORMAT(
        $i$
            INSERT INTO %1$I (%2$s) 
            SELECT %2$s FROM %3$I
            ON CONFLICT (id, account_id) -- Предполагается, что уникальный ключ состоит из столбцов id и account_id
            DO UPDATE SET %4$s
        $i$,
        target_table, -- %1$I: Таблица назначения
        column_list,  -- %2$s: Список колонок
        source_table, -- %3$I: Исходная таблица
        set_clause    -- %4$s: Условие SET для DO UPDATE
    );

    RAISE NOTICE 'Executing query: %', query;
    EXECUTE query;

    RETURN;
END;
$$;

ALTER FUNCTION f_copy_tables(text, text) OWNER TO postgres;

SELECT f_copy_tables('orders_1_log', 'orders');
