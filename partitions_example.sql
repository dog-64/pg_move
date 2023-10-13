-- вариант переноса изменений в таблице p1 в p2 
ROLLBACK ;
DROP TABLE IF EXISTS orders_1;
DROP TABLE IF EXISTS orders_3;
TRUNCATE TABLE orders_3;

DROP TABLE IF EXISTS orders;
DROP SEQUENCE orders_id_seq;

CREATE SEQUENCE orders_id_seq;

-- Создание главной таблицы
CREATE TABLE orders
(
    id          bigint DEFAULT NEXTVAL('orders_id_seq'::regclass),
    account_id  bigint,
    client_id   bigint NOT NULL,
    items_price numeric(10, 2),
    PRIMARY KEY (account_id, id),
    CONSTRAINT account_id_check CHECK (account_id BETWEEN 1 AND 5000000)
) PARTITION BY RANGE (account_id);

-- Создание разделов
CREATE TABLE orders_default PARTITION OF orders
    DEFAULT;
CREATE TABLE orders_1 PARTITION OF orders
    FOR VALUES FROM (1) TO (3); -- 3 не включается
CREATE INDEX idx_orders_1_account_id ON orders_1(account_id);

CREATE TABLE orders_3
(
    id          bigint DEFAULT NEXTVAL('orders_id_seq'::regclass),
    account_id  bigint,
    client_id   bigint NOT NULL,
    items_price numeric(10, 2),
--     PRIMARY KEY (account_id, id)
    PRIMARY KEY (id)
);

-- PSQL
BEGIN;
-- Отключаем журналирование транзакций для увеличения скорости вставки
SET LOCAL SYNCHRONOUS_COMMIT TO 'off';

-- Заполняем таблицу
INSERT INTO orders (account_id, client_id, items_price)
SELECT FLOOR(RANDOM() * 2 + 1)::bigint,                   -- случайное значение для account_id между 1 и 10
       FLOOR(RANDOM() * 10000)::bigint,                    -- случайное значение для client_id
       ROUND((RANDOM() * 100)::numeric, 2)::numeric(10, 2) -- случайное значение для items_price
FROM GENERATE_SERIES(1, 10000000);

-- Возвращаем настройки к исходным значениям и завершаем транзакцию
COMMIT;

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

DROP TRIGGER sync_tables_by_account_id ON orders;
DROP TRIGGER sync_tables_by_account_id ON orders_1;

-- Создание триггера с параметрами
CREATE OR REPLACE TRIGGER sync_tables_by_account_id
    AFTER INSERT OR UPDATE OR DELETE
    ON orders_1
    FOR EACH ROW
EXECUTE FUNCTION f_sync_tables_by_account_id('orders_1', 'orders_3', 2);

-- Проверка - после выполнения в p2 ДОЛЖНА появится запись 
INSERT INTO orders(account_id, client_id, items_price)
VALUES (2, 7, 8);

INSERT INTO orders_3 (id, account_id, client_id, items_price)
SELECT id, account_id, client_id, items_price
FROM orders_3
WHERE id = 10000008;

-- Проверка - после выполнения в p2 НЕ ДОЛЖНА появится запись 
INSERT INTO orders(account_id, client_id, items_price)
VALUES (1, 7, 8);

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
-- SELECT copy_between_tables_by_accounts('p1', 'p2', 1, 2, 3, 4);


-- PSQL
-- Отключаем журналирование транзакций для увеличения скорости вставки
-- если нужно чистить - то не в транзакции
-- TRUNCATE p2;
BEGIN;
SELECT copy_between_tables('orders_1', 'orders_3', 2);
DELETE FROM orders_1 WHERE account_id = 2;
COMMIT;
END;

-- тесты
DO $$ 
DECLARE 
    v_exists BOOLEAN;
BEGIN
    SELECT EXISTS(SELECT 1 FROM orders_1 WHERE account_id = 2) INTO v_exists;

    IF NOT v_exists THEN
        -- Вставьте здесь ваш код, который должен выполниться, если записей с account_id = 2 нет
        RAISE NOTICE 'Нет записей с account_id = 2';
    ELSE
        -- Вставьте здесь ваш код, который должен выполниться, если записи с account_id = 2 существуют
        RAISE NOTICE 'Записи с account_id = 2 существуют';
    END IF;
END;
$$;
