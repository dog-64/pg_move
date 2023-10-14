# pg_move

Перемещение записей между таблицами одинаковой структуры.  
Например, при перебалансировке секций.

- [PgRepack](https://github.com/reorg/pg_repack/blob/master/lib/pg_repack.sql.in)

## Todo

- вынести все в схему pg_move
- проверить на разделе 50GB
- проверить выполнение при идущих изменениях
- заменить в триггере INSERT INTO ... SELECT на INSERT INTO ... VALUES(NEW.*)

## Для таблиц

смотри `tables_example.sql`

## Для секций

смотри `partitions_example.sql`


