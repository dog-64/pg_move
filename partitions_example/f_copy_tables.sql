CREATE OR REPLACE FUNCTION f_copy_tables(source_table text, target_table text) RETURNS void
    LANGUAGE plpgsql
AS
$$
DECLARE
    column_list text;
    query       text;
BEGIN
    SELECT STRING_AGG(QUOTE_IDENT(column_name), ', ')
    INTO column_list
    FROM information_schema.columns
    WHERE table_name = source_table
      AND table_schema = 'public';

    query := FORMAT(
            $i$
                INSERT INTO %1$I (%2$s) 
                SELECT %2$s FROM %3$I
                ON CONFLICT (id) 
                DO UPDATE SET %4$s
            $i$,
            target_table, -- %1$I: Таблица назначения
            column_list, -- %2$s: Список колонок
            source_table -- %3$I: Исходная таблица
        -- %4$s: Должен быть сформирован список присваиваний для DO UPDATE SET
        );

    -- Формируем список присваиваний для SET в DO UPDATE
    SELECT STRING_AGG(FORMAT('%1$I = EXCLUDED.%1$I', column_name), ', ')
    INTO column_list
    FROM information_schema.columns
    WHERE table_name = source_table
      AND table_schema = 'public';
    -- Добавлено указание схемы

    -- Обновляем исходный запрос с учетом списка присваиваний для SET
    query := FORMAT(
            $i$
                INSERT INTO %1$I (%2$s) 
                SELECT %2$s FROM %3$I
                ON CONFLICT (id) 
                DO UPDATE SET %4$s
            $i$,
            target_table, -- %1$I: Таблица назначения
            column_list, -- %2$s: Список колонок
            source_table, -- %3$I: Исходная таблица
            column_list -- %4$s: Список присваиваний для DO UPDATE SET
        );

    RAISE NOTICE 'query "%"', query;
    EXECUTE query;

    RETURN;
END;
$$;

ALTER FUNCTION f_copy_tables(text, text) OWNER TO postgres;

SELECT f_copy_tables('orders_1_log', 'orders');
