# pg_move

Перемещение записей между таблицами одинаковой структуры.  
Например, при перебалансировке секций.

Ключ партицирования - account_id  

- [PgRepack](https://github.com/reorg/pg_repack/blob/master/lib/pg_repack.sql.in)

## Todo

- вынести все в схему pg_move

+ проверить на разделе 40M записей
+ сделать для триггера ON CONFLICT UPDATE

- тесты
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
