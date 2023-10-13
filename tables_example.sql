-- вариант переноса изменений в таблице p1 в p2 
DROP TABLE IF EXISTS p1;
CREATE TABLE p1
(
    id          bigint DEFAULT NEXTVAL('orders_id_seq'::regclass),
    account_id  bigint,
    client_id   bigint NOT NULL,
    items_price numeric(10, 2),
    CONSTRAINT p1_pkey PRIMARY KEY (id, account_id)
);

-- Indices -------------------------------------------------------

CREATE UNIQUE INDEX p1_pkey ON p1 (id int8_ops, account_id int8_ops);

-- DDL generated by Postico 2.0.4
-- Not all database features are supported. Do not use for backup.

-- Table Definition ----------------------------------------------

DROP TABLE IF EXISTS p2;
CREATE TABLE p2
(
    id          bigint DEFAULT NEXTVAL('orders_id_seq'::regclass),
    account_id  bigint,
    client_id   bigint NOT NULL,
    items_price numeric(10, 2),
    CONSTRAINT p2_pkey PRIMARY KEY (id, account_id)
);

-- PSQL
-- Включаем таймер для замера времени выполнения
\timing on

-- Отключаем журналирование транзакций для увеличения скорости вставки
BEGIN;
SET LOCAL SYNCHRONOUS_COMMIT TO 'off';

-- Заполняем таблицу
INSERT INTO p1 (account_id, client_id, items_price)
SELECT FLOOR(RANDOM() * 10 + 1)::bigint,                   -- случайное значение для account_id между 1 и 10
       FLOOR(RANDOM() * 10000)::bigint,                    -- случайное значение для client_id
       ROUND((RANDOM() * 100)::numeric, 2)::numeric(10, 2) -- случайное значение для items_price
FROM GENERATE_SERIES(1, 10000000);

-- Возвращаем настройки к исходным значениям и завершаем транзакцию
COMMIT;


CREATE UNIQUE INDEX p2_pkey ON p2 (id int8_ops, account_id int8_ops);

-- TODO: вынести имена таблиц в параметры

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
    source_table := TG_ARGV[0]; -- таблица-откуда
    target_table := TG_ARGV[1]; -- таблица-куда
    account_ids := TG_ARGV[2:];
    -- список id копируемых account_id, как массив

    -- Получаем account_id текущей строки в зависимости от операции
    IF TG_OP = 'DELETE' THEN
        current_account_id := OLD.account_id;
    ELSE
        current_account_id := NEW.account_id;
    END IF;

    -- Если текущая строка не соответствует ни одному из значений account_id, выход из функции
    IF NOT current_account_id = ANY (account_ids) THEN
        IF TG_OP = 'DELETE' THEN
            RETURN OLD;
        ELSE
            RETURN NEW;
        END IF;
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

    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    ELSE
        RETURN NEW;
    END IF;

END;
$$ LANGUAGE plpgsql;

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
    source_table := TG_ARGV[0]; -- таблица-откуда
    target_table := TG_ARGV[1]; -- таблица-куда
    account_ids := TG_ARGV[2:];
    -- список id копируемых account_id, как массив

    -- Получаем account_id текущей строки в зависимости от операции
    IF TG_OP = 'DELETE' THEN
        current_account_id := OLD.account_id;
    ELSE
        current_account_id := NEW.account_id;
    END IF;

    -- Если текущая строка не соответствует ни одному из значений account_id, выход из функции
    IF NOT current_account_id = ANY (account_ids) THEN
        IF TG_OP = 'DELETE' THEN
            RETURN OLD;
        ELSE
            RETURN NEW;
        END IF;
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

    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    ELSE
        RETURN NEW;
    END IF;

END;
$$ LANGUAGE plpgsql;

-- Создание триггера с параметрами
CREATE TRIGGER sync_tables_by_account_id
    AFTER INSERT OR UPDATE OR DELETE
    ON p1
    FOR EACH ROW
EXECUTE FUNCTION f_sync_tables_by_account_id('p1', 'p2', 1, 2, 3, 4);

-- Проверка - после выполнения в p2 ДОЛЖНА появится запись 
INSERT INTO p1(account_id, client_id, items_price)
VALUES (1, 7, 8);

-- Проверка - после выполнения в p2 НЕ ДОЛЖНА появится запись 
INSERT INTO p1(account_id, client_id, items_price)
VALUES (6, 7, 8);

-- основное копирование, после установки триггера
-- основное копирование, после установки триггера
CREATE OR REPLACE FUNCTION copy_between_tables_by_accounts(source_table text, target_table text, VARIADIC account_ids bigint[])
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

-- Пример использования функции:
SELECT copy_between_tables_by_accounts('p1', 'p2', 1, 2, 3, 4);


-- PSQL
-- Отключаем журналирование транзакций для увеличения скорости вставки
-- если нужно чистить - то не в транзакции
-- TRUNCATE p2;
BEGIN;
-- на проде - Создание триггера с параметрами
-- CREATE TRIGGER copy_changes

SELECT copy_between_tables('p1', 'p2', 1);

-- на проде - удаление порциями, и можно не сразу
-- 	DELETE FROM p1 WHERE id IN (1);
COMMIT;
