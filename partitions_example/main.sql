-- второй вариант переноса разделения секции order_1 на две другие - 2 и 3  
ROLLBACK;
DROP TABLE IF EXISTS orders_1;
DROP TABLE IF EXISTS orders_2;
DROP TABLE IF EXISTS orders_3;
-- DROP TRIGGER sync_tables_by_account_id ON orders;
DROP TRIGGER IF EXISTS sync_tables_by_account_id ON orders_1;
DROP TRIGGER IF EXISTS sync_tables_by_account_id ON orders_2;


DROP TABLE IF EXISTS orders;
DROP SEQUENCE IF EXISTS orders_id_seq;

CREATE SEQUENCE orders_id_seq;

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
CREATE INDEX idx_orders_1_account_id ON orders_1 (account_id);

-- структура дб такой же как у orders, кроме primary key
CREATE TABLE orders_2
(
    id          bigint DEFAULT NEXTVAL('orders_id_seq'::regclass) NOT NULL,
    account_id  bigint                                            NOT NULL,
    client_id   bigint                                            NOT NULL,
    items_price numeric(10, 2),
--     PRIMARY KEY (account_id, id)
    PRIMARY KEY (id)
);

CREATE TABLE orders_3
(
    id          bigint DEFAULT NEXTVAL('orders_id_seq'::regclass) NOT NULL,
    account_id  bigint                                            NOT NULL,
    client_id   bigint                                            NOT NULL,
    items_price numeric(10, 2),
--     PRIMARY KEY (account_id, id)
    PRIMARY KEY (id)
);

-- PSQL
BEGIN;
-- Отключаем журналирование транзакций для увеличения скорости вставки
SET LOCAL SYNCHRONOUS_COMMIT TO 'off';

INSERT INTO orders (account_id, client_id, items_price)
SELECT FLOOR(RANDOM() * 2 + 1)::bigint,                    -- случайное значение для account_id между 1 и 10
       FLOOR(RANDOM() * 10000)::bigint,                    -- случайное значение для client_id
       ROUND((RANDOM() * 100)::numeric, 2)::numeric(10, 2) -- случайное значение для items_price
FROM GENERATE_SERIES(1, 100000000);
COMMIT;

-- Создание триггера с параметрами
-- TODO: вынести в свою схему account_move.

CREATE OR REPLACE TRIGGER sync_tables_by_account_id_1
    AFTER INSERT OR UPDATE OR DELETE
    ON orders_1
    FOR EACH ROW
EXECUTE FUNCTION f_sync_tables_by_account_id('orders_1', 'orders_2', 1);

CREATE OR REPLACE TRIGGER sync_tables_by_account_id_2
    AFTER INSERT OR UPDATE OR DELETE
    ON orders_1
    FOR EACH ROW
EXECUTE FUNCTION f_sync_tables_by_account_id('orders_1', 'orders_3', 2);

-- Проверка - после выполнения в orders_2 ДОЛЖНА появится запись 
INSERT INTO orders(account_id, client_id, items_price)
VALUES (1, 2, 3);
CALL p_assert('SELECT EXISTS(SELECT 1 FROM orders_2 WHERE account_id = 1)');

-- Проверка - после выполнения в orders_3 ДОЛЖНА появится запись 
INSERT INTO orders(account_id, client_id, items_price)
VALUES (2, 3, 4.1);
CALL p_assert('SELECT EXISTS(SELECT 1 FROM orders_3 WHERE account_id = 2)');

BEGIN;
DELETE FROM orders_2 WHERE TRUE;
DELETE FROM orders_3 WHERE TRUE;
SELECT f_copy_between_tables_by_accounts('orders_1', 'orders_2', 1);
SELECT f_copy_between_tables_by_accounts('orders_1', 'orders_3', 2);

ALTER TABLE orders
    DETACH PARTITION orders_1;
-- DELETE очень долго - проще пересоздать раздел, это лучше и потому что первоначальный раздел сохраняется неизменным и его можно использовать, если проблемы 
ALTER TABLE orders_2
    ADD CONSTRAINT account_id_check CHECK (account_id BETWEEN 1 AND 5000000);
ALTER TABLE orders_2
    DROP CONSTRAINT orders_2_pkey;
DROP INDEX IF EXISTS orders_2_pkey;
ALTER TABLE orders
    ATTACH PARTITION orders_2 FOR VALUES FROM (1) TO (2);

ALTER TABLE orders_3
    ADD CONSTRAINT account_id_check CHECK (account_id BETWEEN 1 AND 5000000);
ALTER TABLE orders_3
    DROP CONSTRAINT orders_3_pkey;
DROP INDEX IF EXISTS orders_3_pkey;
ALTER TABLE orders
    ATTACH PARTITION orders_3 FOR VALUES FROM (2) TO (3);
-- DROP TABLE orders_1
COMMIT;
END;
DROP TRIGGER sync_tables_by_account_id_1 ON orders_1;
DROP TRIGGER sync_tables_by_account_id_2 ON orders_1;

-- тесты
CALL p_assert('SELECT EXISTS(SELECT 1 FROM orders WHERE account_id = 1)');
CALL p_assert('SELECT EXISTS(SELECT 1 FROM orders WHERE account_id = 2)');
CALL p_assert('SELECT (SELECT count(*) FROM orders_2) = (SELECT count(*) FROM orders_1 WHERE account_id = 1)');

CALL p_assert('SELECT EXISTS(SELECT 1 FROM orders_2 WHERE account_id = 1)');
CALL p_assert('SELECT NOT EXISTS(SELECT 1 FROM orders_2 WHERE account_id = 2)');
CALL p_assert('SELECT (SELECT count(*) FROM orders_3) = (SELECT count(*) FROM orders_1 WHERE account_id = 2)');

CALL p_assert('SELECT EXISTS(SELECT 1 FROM orders_3 WHERE account_id = 2)');
CALL p_assert('SELECT NOT EXISTS(SELECT 1 FROM orders_3 WHERE account_id = 1)');
