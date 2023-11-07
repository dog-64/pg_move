-- второй вариант переноса разделения секции order_1 на две другие - 2 и 3  
ROLLBACK;
DROP TABLE IF EXISTS orders_1;
DROP TABLE IF EXISTS orders_2;
DROP TABLE IF EXISTS orders_3;
-- DROP TRIGGER sync_tables_by_account_id ON orders;
DROP TRIGGER IF EXISTS sync_tables_by_account_id ON orders_1;
DROP TRIGGER IF EXISTS sync_tables_by_account_id ON orders_2;


DROP TABLE IF EXISTS orders;
DROP SEQUENCE IF EXISTS orders_id_seq CASCADE ;

CREATE SEQUENCE orders_id_seq;

CREATE TABLE orders
(
    id          bigint DEFAULT NEXTVAL('orders_id_seq'::regclass),
    account_id  bigint,
    client_id   bigint NOT NULL,
    items_price numeric(10, 4),
    PRIMARY KEY (account_id, id),
    CONSTRAINT account_id_check CHECK (account_id BETWEEN 1 AND 5000000)
) PARTITION BY RANGE (account_id);

-- Создание разделов
CREATE TABLE orders_default PARTITION OF orders
    DEFAULT;
CREATE TABLE orders_1 PARTITION OF orders
    FOR VALUES FROM (1) TO (3); -- 3 не включается
CREATE INDEX idx_orders_1_account_id ON orders_1 (account_id);

-- структура дб такой же как у orders, кроме primary key
CREATE TABLE orders_2
(
    id          bigint DEFAULT NEXTVAL('orders_id_seq'::regclass) NOT NULL,
    account_id  bigint                                            NOT NULL,
    client_id   bigint                                            NOT NULL,
    items_price numeric(10, 4),
--     PRIMARY KEY (account_id, id)
    PRIMARY KEY (id)
);

CREATE TABLE orders_3
(
    id          bigint DEFAULT NEXTVAL('orders_id_seq'::regclass) NOT NULL,
    account_id  bigint                                            NOT NULL,
    client_id   bigint                                            NOT NULL,
    items_price numeric(10, 4),
--     PRIMARY KEY (account_id, id)
    PRIMARY KEY (id)
);

-- PSQL
BEGIN;
    -- Отключаем журналирование транзакций для увеличения скорости вставки
    SET LOCAL SYNCHRONOUS_COMMIT TO 'off';
    
    INSERT INTO orders (account_id, client_id, items_price)
    SELECT FLOOR(RANDOM() * 2 + 1)::bigint,
           FLOOR(RANDOM() * 10_000)::bigint,
           ROUND((RANDOM() * 100)::numeric, 2)::numeric(10, 2)
    
    FROM GENERATE_SERIES(1, 100_000_000);
    COMMIT;
END;

-- Создание триггера с параметрами
-- TODO: вынести в свою схему account_move.

-- создаем таблицу лога изменений исходной таблицы
DROP TABLE IF EXISTS orders_1_log;
CREATE TABLE orders_1_log (LIKE orders INCLUDING ALL);

CREATE OR REPLACE TRIGGER sync_tables
    AFTER INSERT OR UPDATE OR DELETE
    ON orders_1
    FOR EACH ROW
EXECUTE FUNCTION f_sync_tables('orders_1', 'orders_1_log');

-- Проверка - после выполнения в orders_1_log ДОЛЖНА появится запись 
INSERT INTO orders(account_id, client_id, items_price)
VALUES (1, 2, 3);
CALL p_assert('SELECT EXISTS(SELECT 1 FROM orders_1_log WHERE account_id = 1 AND client_id = 2 AND items_price = 3)');

DELETE FROM orders WHERE account_id = 1 AND client_id = 2;
CALL p_assert('SELECT NOT EXISTS(SELECT 1 FROM orders_1_log WHERE account_id = 1 AND client_id = 2)');

-- эти вставки должны будут появиться в orders_1_log
INSERT INTO orders (account_id, client_id, items_price)
SELECT FLOOR(RANDOM() * 2 + 1)::bigint,
       FLOOR(RANDOM() * 10_000)::bigint,
       5.1234
FROM GENERATE_SERIES(1, 1_000);


SELECT 'START -------------------------------', now();
BEGIN;
    TRUNCATE TABLE  orders_2;
    TRUNCATE TABLE  orders_3;

    SELECT 'COPY -------------------------------', now();
    SELECT f_copy_between_tables_by_accounts('orders_1', 'orders_2', 1);
    SELECT f_copy_between_tables_by_accounts('orders_1', 'orders_3', 2);

    ALTER TABLE orders
        DETACH PARTITION orders_1;
    -- DELETE очень долго - проще пересоздать раздел, это лучше и потому что первоначальный раздел сохраняется неизменным и его можно использовать, если проблемы 
    ALTER TABLE orders_2
        ADD CONSTRAINT account_id_check CHECK (account_id BETWEEN 1 AND 5_000_000);
    ALTER TABLE orders_2
        DROP CONSTRAINT orders_2_pkey;
    DROP INDEX IF EXISTS orders_2_pkey;
    ALTER TABLE orders
        ATTACH PARTITION orders_2 FOR VALUES FROM (1) TO (2);
    
    ALTER TABLE orders_3
        ADD CONSTRAINT account_id_check CHECK (account_id BETWEEN 1 AND 5_000_000);
    ALTER TABLE orders_3
        DROP CONSTRAINT orders_3_pkey;
    DROP INDEX IF EXISTS orders_3_pkey;
    ALTER TABLE orders
        ATTACH PARTITION orders_3 FOR VALUES FROM (2) TO (3);
    -- DROP TABLE orders_1

    SELECT f_copy_tables('orders_1_log', 'orders');
    -- DROP TABLE orders_1_log;

    COMMIT;
END;
SELECT 'FINISH -------------------------------', now();
DROP TRIGGER sync_tables ON orders_1;

-- тесты
CALL p_assert('SELECT EXISTS(SELECT 1 FROM orders WHERE account_id = 1)');
CALL p_assert('SELECT EXISTS(SELECT 1 FROM orders WHERE account_id = 2)');
CALL p_assert('SELECT (SELECT count(*) FROM orders_2) = (SELECT count(*) FROM orders_1 WHERE account_id = 1)');

CALL p_assert('SELECT EXISTS(SELECT 1 FROM orders_2 WHERE account_id = 1)');
CALL p_assert('SELECT NOT EXISTS(SELECT 1 FROM orders_2 WHERE account_id = 2)');
CALL p_assert('SELECT (SELECT count(*) FROM orders_3) = (SELECT count(*) FROM orders_1 WHERE account_id = 2)');

CALL p_assert('SELECT EXISTS(SELECT 1 FROM orders_3 WHERE account_id = 2)');
CALL p_assert('SELECT NOT EXISTS(SELECT 1 FROM orders_3 WHERE account_id = 1)');

CALL p_assert('SELECT 1_000 <= (SELECT count(*) FROM orders WHERE items_price = 5.1234)');
