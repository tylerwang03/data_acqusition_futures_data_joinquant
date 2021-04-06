import schedule
import time


def do_something():
    print("let's all in")


schedule.every(10).seconds.do(do_something)
while True:
    schedule.run_pending()
    time.sleep(1)
