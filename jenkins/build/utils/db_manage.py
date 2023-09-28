# delete table every 3 month
#DELETE YourTable WHERE YourDate<GETDATE()-7

#backup table every month
#sqlcmd -U sa -P sa@admin -S .\SQLEXPRESS -Q "EXEC sp_BackupDatabases @backupLocation='D:\SQLBackups\', @databaseName='counter', @backupType='F'"


#import subprocess
#print "start"
#subprocess.call("./sleep.sh", shell=True)
#print "end"