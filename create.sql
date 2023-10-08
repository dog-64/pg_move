-- https://chat.openai.com/c/ad9b7d46-a884-4a17-ab8a-31ba1605ec5f

DROP TABLE IF EXISTS orders;

-- Создание родительской таблицы
CREATE TABLE orders
(
    id          BIGSERIAL,
    account_id  BIGINT NOT NULL,
    client_id   BIGINT NOT NULL,
    items_price DECIMAL(10, 2),
    PRIMARY KEY (id, account_id)
) PARTITION BY RANGE (account_id);

-- Создание дочерних таблиц
CREATE TABLE orders_account_1 PARTITION OF orders FOR VALUES FROM (1) TO (1001);
CREATE TABLE orders_account_2 PARTITION OF orders FOR VALUES FROM (1001) TO (2001);

-- Заполнение таблицы orders
DO
$$
    DECLARE
        counter BIGINT;
    BEGIN
        FOR counter IN 1..2000
            LOOP
                INSERT INTO orders (account_id, client_id, items_price)
                SELECT counter,
                       GENERATE_SERIES(1, 1000),
                       ROUND(CAST(RANDOM() * 100 AS numeric), 2);
            END LOOP;
    END
$$;

CREATE OR REPLACE FUNCTION dynamic_copy()
    RETURNS TRIGGER AS
$$
DECLARE
    source_table_name   TEXT;
    target_table_name   TEXT;
    allowed_account_ids BIGINT[];
    record_to_insert    RECORD;
    i                   INT;
BEGIN
    source_table_name := TG_ARGV[0];
    target_table_name := TG_ARGV[1];

    -- Собираем все аргументы после второго в массив allowed_account_ids
    allowed_account_ids := ARRAY []::BIGINT[];
    FOR i IN 2..ARRAY_UPPER(TG_ARGV, 1)
        LOOP
            allowed_account_ids := ARRAY_APPEND(allowed_account_ids, TG_ARGV[i]::BIGINT);
        END LOOP;

    IF TG_OP = 'DELETE' THEN
        record_to_insert := OLD;
    ELSE
        record_to_insert := NEW;
    END IF;

    RAISE NOTICE 'Record.id: %', record_to_insert.id;

    IF record_to_insert.account_id = ANY (allowed_account_ids) THEN
        EXECUTE 'INSERT INTO ' || target_table_name ||
                ' (id, account_id, client_id, items_price) VALUES($1, $2, $3, $4)'
            USING record_to_insert.id, record_to_insert.account_id, record_to_insert.client_id, record_to_insert.items_price;
    END IF;

    RETURN NEW;
END ;
$$ LANGUAGE plpgsql;

DROP TRIGGER dynamic_copy_trigger ON p1;

CREATE OR REPLACE TRIGGER dynamic_copy_trigger
    AFTER INSERT OR DELETE OR UPDATE
    ON p1
    FOR EACH ROW
EXECUTE FUNCTION dynamic_copy('p1', 'p3', 3);

INSERT INTO p1(id, "account_id", "client_id", "items_price")
VALUES (-4, 3, 72892, 372.38);
