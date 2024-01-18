# PRAGMA SAS

## Pruba de ingeniería de datos

JULIAN ALEXANDER MALAVER MORENO

## Environment

### Coding

I utilized Anaconda as environment manager and pycharm as Python IDE.

* Anaconda: 
    * Navigator: 2.5.2
    * Distribution: 23.10.0

* Python: 3.10.13
    * pandas: 2.1.4
    * pymysql: 1.1.0

### DB

I used a docker image of the mysql db on the debian distro.

* MySQL: mysql:8.0.34-debian
* MySQL Workbench: 8.0.34

## Preparation

3 different schemas were created on the MySQL Database:

* raw: staging area. 
    * purchase: table to store microbatch data, it's truncated after every iteration.
* data: curated area.
    * purchase: contains all the transactions data.
* log: used for store metrics.
    * purchase_history: contains aggregated metrics for every iteration(microbatch).
    * new_purchases (view): get aggregated metrics for micro-batch.
    * prev_purchases (view): get last value of metrics from purchase_history.

### Tables / Views definition

```sql
CREATE TABLE `raw`.`purchase` (
  `timestamp` DATE DEFAULT NULL,
  `price` DECIMAL(20,4) DEFAULT NULL,
  `user_id` INT DEFAULT NULL
);

CREATE TABLE `data`.`purchase` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `timestamp` DATE DEFAULT NULL,
  `price` DECIMAL(20,4) DEFAULT NULL,
  `user_id` INT DEFAULT NULL,
  PRIMARY KEY (`id`)
);

CREATE TABLE `log`.`purchase_history` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `count` BIGINT NULL,
  `avg_price` DECIMAL(20,4) NULL,
  `min_price` DECIMAL(20,4) NULL,
  `max_price` DECIMAL(20,4) NULL,
  PRIMARY KEY (`id`)
);

CREATE OR REPLACE VIEW `log`.`new_purchases` AS
SELECT count(*) as count, 
  avg(price) as avg_price, 
  min(price) as min_price, 
  max(price) as max_price
from `raw`.`purchase`;

CREATE OR REPLACE VIEW `log`.`prev_purchases` AS
SELECT * 
FROM `log`.`purchase_history`
ORDER BY id DESC
LIMIT 1;
```

Decimals are used to better handle prices.

Auto increment was added to `log.purchase_history` and `data.purchase` for
faster data retrieval.

## PROCESS

These are the calculation used at every iteration to obtain the new metrics.

```
count = new_count + prev_count
avg_price = (new_count * new_avg_price + prev_count * prev_avg_price) / count
min_price = min(new_min_price, prev_min_price)
max_price = max(new_max_price, prev_max_price)
```

The results are stored in the `log.purchase_history` table for being used in the next iteration / microbatch.

## EXECUTION

The entry point of the execution is the python file `python.py`.
It iterates over all the files that match the regexp `\d{4}-\d{1,2}.csv` in the files folder of the project.

In order to execute the program for the validation file `validation.csv` the next line of code must be modified:

```
if re.search(r'\d{4}-\d{1,2}.csv', file):

# replace by

if not re.search(r'\d{4}-\d{1,2}.csv', file):
```
