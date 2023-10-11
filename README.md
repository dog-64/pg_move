# pg_move

Перемещение записей между таблицами одинаковой структуры.  
Например, при перебалансировке секций.

- [PgRepack](https://github.com/reorg/pg_repack/blob/master/lib/pg_repack.sql.in)

## Для таблиц

смотри `tables_example.sql`

```postgresql
-- PSQL
-- CREATE OR REPLACE FUNCTION f_sync_tables_by_account_id()
-- CREATE OR REPLACE FUNCTION copy_between_tables_by_accounts...
-- Создание триггера с параметрами
BEGIN;
    CREATE TRIGGER sync_tables_by_account_id
        AFTER INSERT OR UPDATE OR DELETE
        ON p1
        FOR EACH ROW
    EXECUTE FUNCTION f_sync_tables_by_account_id('p1', 'p2', 1);
    
    SELECT copy_between_tables_by_accounts('p1', 'p2', 1);
    DELETE
    FROM p1
    WHERE account_id IN (1, 2, 3, 4);
    DROP TRIGGER sync_tables_by_account_id ON p1;
END;
```
