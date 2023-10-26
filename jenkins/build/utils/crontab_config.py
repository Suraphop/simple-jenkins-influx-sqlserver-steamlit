def crontab_delete():
    with open("crontab",'r+') as f:
        f.truncate(0)

def crontab_every_minute():
    with open("crontab", 'w') as f:
        f.write("# START CRON JOB\n")
        f.write("PATH=/usr/local/bin\n")
        f.write("* * * * * python3 /app/main.py\n")
        f.write("# END CRON JOB")

def crontab_every_hr():
    with open("crontab", 'w') as f:
        f.write("# START CRON JOB\n")
        f.write("PATH=/usr/local/bin\n")
        f.write("0 * * * * python3 /app/main.py\n")
        f.write("# END CRON JOB")

def crontab_read():
    f = open("crontab", "r+")
    return f.read()
