DELIMITER //
DROP PROCEDURE IF EXISTS `delete_rows_6_months_account_bal`()
BEGIN  //
CREATE PROCEDURE delete_rows_6_months()
BEGIN 
    DELETE FROM ACCOUNT_BALANCES 
    WHERE TIMESTAMP < DATE_SUB(NOW(), INTERVAL 6 MONTH);
END //
DELIMITER ;



CREATE EVENT delete_rows_6_months_account_bal
ON SCHEDULE
    EVERY 1 MONTH
    STARTS '2023-05-01 00:00:00'
DO
    CALL delete_rows_6_months_account_bal();


------------------------------------------
DELIMITER //
DROP PROCEDURE IF EXISTS `delete_rows_6_month_market_summary` //
CREATE PROCEDURE delete_rows_6_month_market_summary()
BEGIN 
    DELETE FROM MARKET_SUMMARY MS  
    WHERE TIMESTAMP < DATE_SUB(NOW(), INTERVAL 6 MONTH);
END //
DELIMITER ;



CREATE EVENT delete_rows_6_month_market_summary
ON SCHEDULE
    EVERY 1 MONTH
    STARTS '2023-05-01 00:00:00'
DO
    CALL delete_rows_6_month_market_summary();

--------------------------------------------
DELIMITER //
DROP PROCEDURE IF EXISTS `delete_rows_7_days_trade_pair` //
CREATE PROCEDURE delete_rows_7_days_trade_pair()
BEGIN 
    DELETE FROM TRADING_PAIRS 
    WHERE TIMESTAMP < DATE_SUB(NOW(), INTERVAL 7 DAY);
END //
DELIMITER ;



CREATE EVENT delete_rows_7_days_trade_pair
ON SCHEDULE
    EVERY 1 MONTH
    STARTS '2023-05-01 00:00:00'
DO
    CALL delete_rows_7_days_trade_pair();
    
