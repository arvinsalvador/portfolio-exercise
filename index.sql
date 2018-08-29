-- 1. Create the `reits` table.

    -- -----------------------------------------------------
    -- Table `reits`
    -- -----------------------------------------------------
    CREATE TABLE IF NOT EXISTS `reits` (
      `id` INT NOT NULL AUTO_INCREMENT,
      `code` CHAR(8) NOT NULL,
      `name` VARCHAR(150) NOT NULL,
      PRIMARY KEY (`id`),
      UNIQUE INDEX `id_UNIQUE` (`id` ASC),
      INDEX `reits_index` (`code` ASC))
    ENGINE = InnoDB;

-- 2. Create the `activities` table.

    -- -----------------------------------------------------
    -- Table `activities`
    -- -----------------------------------------------------
    CREATE TABLE IF NOT EXISTS `activities` (
      `id` INT NOT NULL AUTO_INCREMENT,
      `reit` CHAR(8) NOT NULL,
      `type` CHAR(5) NOT NULL,
      `quantity` INT NOT NULL DEFAULT 0,
      `tradePrice` FLOAT NOT NULL,
      `tradeDate` DATE NOT NULL,
      PRIMARY KEY (`id`, `reit`),
      UNIQUE INDEX `id_UNIQUE` (`id` ASC),
      INDEX `fk_activities_reits_idx` (`reit` ASC),
      CONSTRAINT `fk_activities_reits`
        FOREIGN KEY (`reit`)
        REFERENCES `reits` (`code`)
        ON DELETE NO ACTION
        ON UPDATE NO ACTION)
    ENGINE = InnoDB;
    
-- 3. Create the `prices` table.

    -- -----------------------------------------------------
    -- Table `prices`
    -- -----------------------------------------------------
    CREATE TABLE IF NOT EXISTS `prices` (
      `reit` CHAR(8) NOT NULL,
      `price` FLOAT NOT NULL,
      `date` DATE NOT NULL,
      INDEX `fk_prices_reits_idx` (`reit` ASC),
      PRIMARY KEY (`reit`),
      CONSTRAINT `fk_prices_reits`
        FOREIGN KEY (`reit`)
        REFERENCES `reits` (`code`)
        ON DELETE NO ACTION
        ON UPDATE NO ACTION)
    ENGINE = InnoDB;
    DELIMITER ;

    DELIMITER ;;
    CREATE PROCEDURE `activities_avgbuyprice`(IN reitCode varchar(10), OUT avgBuyPrice float)
    BEGIN
    
        DECLARE cursor_reit varchar(10);
        DECLARE cursor_type varchar(10);
        DECLARE cursor_qty int;
        DECLARE cursor_prc float;
        DECLARE cum_qty int;
        DECLARE cum_qtyP int;
        DECLARE avgBuyPrc float;
        DECLARE done INT DEFAULT FALSE;
        DECLARE cursor_i CURSOR FOR SELECT REIT, type, quantity, tradePrice FROM activities WHERE REIT = reitCode;
        DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;
        
        SET avgBuyPrc = 0;
        SET cum_qty = 0;
        SET cum_qtyP = 0;
        
        OPEN cursor_i;
            read_loop: LOOP
                FETCH cursor_i INTO cursor_reit, cursor_type, cursor_qty, cursor_prc;
                IF done THEN
                    LEAVE read_loop;
                END IF;
                IF cursor_type = "BUY" THEN
                    SET cum_qty = cum_qty + cursor_qty;
                    IF avgBuyPrc = 0 THEN
                        SET avgBuyPrc = cursor_prc;
                    ELSE
                        SET avgBuyPrc = ((cum_qtyP * avgBuyPrc) + (cursor_prc * cursor_qty)) / cum_qty;
                    END IF;
                ELSE
                    SET cum_qty = cum_qty - cursor_qty;
                END IF;
                SET cum_qtyP = cum_qty;
            END LOOP;
            SET avgBuyPrice = avgBuyPrc;
        CLOSE cursor_i;
    END;;
    DELIMITER ;

    DELIMITER ;;
    CREATE PROCEDURE `activities_cumqty`(IN reitCode varchar(10), OUT cumQty int)
    BEGIN
    
        DECLARE cursor_reit varchar(10);
        DECLARE cursor_type varchar(10);
        DECLARE cursor_qty int;
        DECLARE cum_qty int;
        DECLARE done INT DEFAULT FALSE;
        DECLARE cursor_i CURSOR FOR SELECT REIT, type, quantity FROM activities WHERE REIT = reitCode;
        DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;
        
        SET cum_qty = 0;
        
        OPEN cursor_i;
            read_loop: LOOP
                FETCH cursor_i INTO cursor_reit, cursor_type, cursor_qty;
                IF done THEN
                    LEAVE read_loop;
                END IF;
                IF cursor_type = "BUY" THEN
                    SET cum_qty = cum_qty + cursor_qty;
                ELSE
                    SET cum_qty = cum_qty - cursor_qty;
                END IF;
                
            END LOOP;
            SET cumQty = cum_qty;
        CLOSE cursor_i;
    END;;
    DELIMITER ;

    DELIMITER ;;
    CREATE PROCEDURE `activities_sellprofit`(IN reitCode varchar(10), OUT sellProfit float)
    BEGIN
    
        DECLARE cursor_reit varchar(10);
        DECLARE cursor_type varchar(10);
        DECLARE cursor_qty int;
        DECLARE cursor_prc float;
        DECLARE cum_qty int;
        DECLARE cum_qtyP int;
        DECLARE avgBuyPrc float;
        DECLARE sProfit float;
        DECLARE avgsProfit float;
        DECLARE done INT DEFAULT FALSE;
        DECLARE cursor_i CURSOR FOR SELECT REIT, type, quantity, tradePrice FROM activities WHERE REIT = reitCode;
        DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;
        
        SET avgBuyPrc = 0;
        SET sProfit = 0;
        SET cum_qty = 0;
        SET cum_qtyP = 0;
        SET avgsProfit = 0;
        
        OPEN cursor_i;
            read_loop: LOOP
                FETCH cursor_i INTO cursor_reit, cursor_type, cursor_qty, cursor_prc;
                IF done THEN
                    LEAVE read_loop;
                END IF;
                SET sProfit = 0;
                IF cursor_type = "BUY" THEN
                    SET cum_qty = cum_qty + cursor_qty;
                    IF avgBuyPrc = 0 THEN
                        SET avgBuyPrc = cursor_prc;
                    ELSE
                        SET avgBuyPrc = ((cum_qtyP * avgBuyPrc) + (cursor_prc * cursor_qty)) / cum_qty;
                    END IF;
                ELSE
                    SET cum_qty = cum_qty - cursor_qty;
                    SET sProfit = (cursor_prc - avgBuyPrc) * cursor_qty;
                END IF;
                SET cum_qtyP = cum_qty;
                SET avgsProfit = avgsProfit + sProfit;
            END LOOP;
            SET sellProfit = avgsProfit;
        CLOSE cursor_i;
    END;;
    DELIMITER ;

-- 4. Define the `calculateHoldings` procedure.
    DELIMITER ;; 
    CREATE PROCEDURE `calculateHoldings`(IN inpDate date)
    BEGIN
        
        DECLARE cursor_reit varchar(10);
        DECLARE cursor_price float;
        DECLARE hQty int;
        DECLARE hAvgBuyPrice float;
        DECLARE hSellProfit float;
        DECLARE hPrice float;
        DECLARE hTProfits float;
        DECLARE done INT DEFAULT FALSE;
        DECLARE cursor_i CURSOR FOR SELECT R.code FROM prices as P LEFT JOIN reits  as R ON P.reit = R.code WHERE P.date = inpDate;
        DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;
        
        CREATE TABLE IF NOT EXISTS `tmp_holdings` (
            `reit` CHAR(8) NOT NULL,
            `quantity` INT NOT NULL,
            `buyPrice` FLOAT NOT NULL,
            `sellProfit` FLOAT NOT NULL,
            `profits` INT NOT NULL);
        
        OPEN cursor_i;
            read_loop: LOOP
                FETCH cursor_i INTO cursor_reit;
                IF done THEN
                    LEAVE read_loop;
                END IF;
                CALL activities_cumqty(cursor_reit, hQty);
                CALL activities_avgbuyprice(cursor_reit, hAvgBuyPrice);
                CALL activities_sellprofit(cursor_reit, hSellProfit);
                CALL getPriceValue(cursor_reit, inpDate, hPrice);
                
                SET hTProfits = hSellProfit + (hPrice - hAvgBuyPrice) * hQty;
                INSERT INTO tmp_holdings VALUES(cursor_reit, hQty, hAvgBuyPrice, hSellProfit, hTProfits);
            END LOOP;
            SELECT * FROM tmp_holdings;
        CLOSE cursor_i;
        DROP TABLE tmp_holdings;
    END;;
    DELIMITER ;
    
-- 5. Define the `calculateMetrics` procedure.
    DELIMITER ;;
    CREATE PROCEDURE `calculateMetrics`(IN inpDate date)
    BEGIN
        DECLARE cursor_reit varchar(10);
        DECLARE hQty int;
        DECLARE hAvgBuyPrice float;
        DECLARE hSellProfit float;
        DECLARE hPrice float;
        DECLARE hTProfits float;
        DECLARE tValue int;
        DECLARE tProfit int;
        DECLARE done INT DEFAULT FALSE;
        DECLARE cursor_i CURSOR FOR SELECT R.code FROM prices as P LEFT JOIN reits  as R ON P.reit = R.code WHERE P.date = inpDate;
        DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;
        
        SET tValue = 0;
        SET tProfit = 0;
        
        OPEN cursor_i;
            read_loop: LOOP
                FETCH cursor_i INTO cursor_reit;
                IF done THEN
                    LEAVE read_loop;
                END IF;
                CALL activities_cumqty(cursor_reit, hQty);
                CALL activities_avgbuyprice(cursor_reit, hAvgBuyPrice);
                CALL activities_sellprofit(cursor_reit, hSellProfit);
                CALL getPriceValue(cursor_reit, inpDate, hPrice);
                
                SET hTProfits = hSellProfit + (hPrice - hAvgBuyPrice) * hQty;
                SET tValue = tValue + (hQty * hPrice);
                SET tProfit = tProfit + hTProfits;
            END LOOP;
            SELECT tValue, tProfit;
        CLOSE cursor_i;
    END;;
    DELIMITER ;

    DELIMITER ;;
    CREATE PROCEDURE `getPriceValue`(IN reitCode varchar(10), IN prcDate date, OUT prcVal float)
    BEGIN
        DECLARE cursor_price float;
        DECLARE done INT DEFAULT FALSE;
        DECLARE cursor_i CURSOR FOR SELECT price FROM prices WHERE reit = reitCode AND date = prcDate;
        DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;
        
        OPEN cursor_i;
            read_loop: LOOP
                FETCH cursor_i INTO cursor_price;
                IF done THEN
                    LEAVE read_loop;
                END IF;
            END LOOP;
            SET prcVal = cursor_price;
        CLOSE cursor_i;
    END;;
    DELIMITER ;

-- 6. Load `reits.csv` into the `reits` table.

    LOAD DATA INFILE 'reits.csv' 
    INTO TABLE reits 
    FIELDS TERMINATED BY ',' 
    ENCLOSED BY '"'
    LINES TERMINATED BY '\n'
    IGNORE 1 ROWS
        (code, name);

-- 7. Load `prices.csv` into the `prices` table.

-- 8. Load `activities.csv` into the `activities` table.

    LOAD DATA INFILE 'activities.csv' 
    INTO TABLE activities 
    FIELDS TERMINATED BY ',' 
    ENCLOSED BY '"'
    LINES TERMINATED BY '\n'
    IGNORE 1 ROWS
        (tradeDate, REIT, type, quantity, tradePrice);