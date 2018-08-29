# Portfolio
Your task is to write SQL definitions based on the instructions given below. The SQL definitions must be compatible with the latest versions of `PostgreSQL` or `MySQL`. Your answers must be written into `index.sql`. Your answers will be assessed based on the following criteria:
1. Accuracy
2. Readability
3. Code Performance

## Instructions
1. Create the `reits` table. The `reits` table must have the following attributes:

    |Name|Description|
    |---|---|
    |`id`|unique ID|
    |`code`|stock code|
    |`name`|REIT name|

2. Create the `activities` table. The `activities` table must have the following attributes:

    |Name|Description|
    |---|---|
    |`id`|unique ID|
    |`reit`|reference to a row in the `reits` table|
    |`type`|activity type, value must be one of the following values: `BUY`, `SELL`|
    |`quantity`|number of units bought or sold|
    |`tradePrice`|price per unit at which the stocks where bought or sold|
    |`tradeDate`|date when the trade was executed|

3. Create the `prices` table. The `prices` table must have the following attributes:

    |Name|Description|
    |---|---|
    |`reit`|reference to a row in the `reits` table|
    |`price`|price per unit at date|
    |`date`|date|

4. Define the `calculateHoldings` procedure. The `calculateHoldings` procedure must be based on the calculations given in `calculations.xlsx`. The `calculateHoldings` procedure must accept a `date` parameter which will determine which `activities` to be included in the calculations. The `activities` that are included must have `tradeDate`s that are less than or equal to the `date` parameter. Finally, the procedure must return a result set with the following attributes:

    |Name|Description|
    |---|---|
    |`reit`|reference to a row in the `reits` table|
    |`quantity`|total number of units in holding|
    |`averageBuyPrice`|average buy price weighted by quantity|
    |`sellProfit`|total profit for all units sold|

5. Define the `calculateMetrics` procedure. The `calculateMetrics` procedure must be based on the calculations given in `calculations.xlsx`. Similar to the `calculateHoldings` procedure, `calculateMetrics` must accept a `date` parameter. The procedure must return a single-row result set with the following attributes:

    |Name|Description|
    |---|---|
    |`totalValue`|total value of the portfolio|
    |`totalProfit`|total portfolio profit|

6. Load `reits.csv` into the `reits` table.

7. Load `prices.csv` into the `prices` table.

8. Load `activities.csv` into the `activities` table.
