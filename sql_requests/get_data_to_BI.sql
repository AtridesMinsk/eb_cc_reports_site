SELECT 
	CONVERT(user.ID, CHAR) AS ID,
    CONVERT_TZ(DateRegistered,'+00:00','+03:00') AS DateRegistered,
	CONVERT(BirthDate, DATE) AS BirthDate,
	CONVERT_TZ(bi.first_time_deposits.Date,'+00:00','+03:00') AS FTD_Date, 
	bi.first_time_deposits.DepositAmount/100 AS FTD_BYN, 
    StatusID, 
    providers.Name AS Provider, 
    bi.channels.Name AS Chanel
FROM user
LEFT JOIN bi.first_time_deposits ON userid=user.ID
LEFT JOIN providers ON bi.first_time_deposits.providerid=providers.ProviderID
LEFT JOIN bi.channels ON bi.first_time_deposits.Channel=bi.channels.ID
WHERE CONVERT_TZ(bi.first_time_deposits.Date,'+00:00','+03:00') != 0 AND bi.first_time_deposits.DepositAmount/100 != 0 AND StatusID = 1
ORDER BY user.ID DESC