# pg_move

Перемещение записей между секциями таблиц Postgres.  
Например, при перебалансировке секций.
Ключ партицирования - account_id

Размер секций до 50ГБ с числом акаунтов до 2000.

- [PgRepack](https://github.com/reorg/pg_repack/blob/master/lib/pg_repack.sql.in)
- [partition create](https://supabase.com/blog/postgres-dynamic-table-partitioning)

## Предположения

- все таблицы имеют первичный ключь по account_id + id
- существует индекс по account_id + id
- оба этих поля int (bigint)

## Todo

- вынести все в схему pg_move

+ проверить на разделе 40M записей
+ сделать для триггера ON CONFLICT UPDATE
+ тесты

- проверить на разделе 50GB
- проверить выполнение при идущих изменениях
- заменить в триггере INSERT INTO ... SELECT на INSERT INTO ... VALUES(NEW.*)
- добавление таблицы секции

## Для таблиц

смотри `tables_example.sql`

## Для секций

смотри `partitions_example.sql`

## Замеры

### Условия

- MacOs 12.6.8 (21G725)
- Postgres 14
- MacBook Pro (14-inch, 2021)
- Apple M1 Max

## 40М в секции

|                             | новое | уже было |     |
|-----------------------------|-------|----------|-----|
| copy_between_tables(o1, o2) | 52    | 59       |     |
| copy_between_tables(o1, o3) | 52    | 53       |     |
| ADD CONSTRAINT o2           | 1     | 1        |     |
| ATTACH o2                   | 9     | 10       |     |  
| ADD CONSTRAINT o2           | 1     | 1        |     |
| ATTACH PARTITION o3         | 9     | 3        |     |
| **ИТОГО**                   | 120   |          |     |

## 100M записей - 5.7GB

|                                                          | 1, s | 2, s | 3, s |
|----------------------------------------------------------|------|------|------|
| 100M записей без параллельной вставки, max_wal_size 1GB  | 315  | 363  | 376  |
| 100м записей и вставка 100К, max_wal_size 1GB            | 332  | 300  | 360  |
| 100M записей без параллельной вставки, max_wal_size 32GB | 321  | 313  | 315  |
|                                                          | 312  |      |      |

- заполнение 100M записей - 293s
- вставка 100К записей, в другой сессии, параллельно переносу секции - 15-25s

## Проблемы

В Postgres 14/16 при выполнении

```postgresql
-- Проверка - после выполнения в orders_2 ДОЛЖНА появится запись 
INSERT INTO orders(account_id, client_id, items_price)
VALUES (1, 2, 3);
```

в функции `f_sync_tables_by_account_id` jib,rf

```log
[2023-11-04 12:43:50] [42P01] ERROR: relation "excluded" does not exist
[2023-11-04 12:43:50] Where: PL/pgSQL function f_sync_tables_by_account_id() line 46 at EXECUTE
```

Причина - нужно было

```postgresql
ALTER FUNCTION f_sync_tables_by_account_id() OWNER TO postgres;
```
