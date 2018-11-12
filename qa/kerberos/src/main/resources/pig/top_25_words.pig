A = LOAD '/output/pride' USING PigStorage(',') AS (word: chararray, count: int);
B = ORDER A BY count DESC;
C = LIMIT B 25;
DUMP C;
