SELECT ID, UserName, EMail, TelephoneNumber, StatusID, DateRegistered, FraudIndex, ActiveNotifications
FROM Core.user
WHERE StatusID = 1 AND BirthDate = '1970-01-01 00:00:00' AND DateRegistered >= DATE_SUB(CURRENT_DATE, INTERVAL 7 DAY)
ORDER BY ID DESC