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
        f.write("* 0 * * * python3 /app/main.py\n")
        f.write("* 1 * * * python3 /app/main.py\n")
        f.write("* 2 * * * python3 /app/main.py\n")
        f.write("* 3 * * * python3 /app/main.py\n")
        f.write("* 4 * * * python3 /app/main.py\n")
        f.write("* 5 * * * python3 /app/main.py\n")
        f.write("* 6 * * * python3 /app/main.py\n")
        f.write("* 7 * * * python3 /app/main.py\n")
        f.write("* 8 * * * python3 /app/main.py\n")
        f.write("* 9 * * * python3 /app/main.py\n")
        f.write("* 10 * * * python3 /app/main.py\n")
        f.write("* 11 * * * python3 /app/main.py\n")
        f.write("* 12 * * * python3 /app/main.py\n")
        f.write("* 13 * * * python3 /app/main.py\n")
        f.write("* 14 * * * python3 /app/main.py\n")
        f.write("* 15 * * * python3 /app/main.py\n")
        f.write("* 16 * * * python3 /app/main.py\n")
        f.write("* 17 * * * python3 /app/main.py\n")
        f.write("* 18 * * * python3 /app/main.py\n")
        f.write("* 19 * * * python3 /app/main.py\n")
        f.write("* 20 * * * python3 /app/main.py\n")
        f.write("* 21 * * * python3 /app/main.py\n")
        f.write("* 22 * * * python3 /app/main.py\n")
        f.write("* 23 * * * python3 /app/main.py\n")
        f.write("# END CRON JOB")

def crontab_read():
    f = open("crontab", "r+")
    return f.read()
