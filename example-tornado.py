import datetime
from tornado import gen, ioloop
from pyhs2.connections import TornadoConnection

# fill in your server settings...
HIVE_SERVER_SETTINGS = dict(
    host="hiveserver",
    port=10000,
    authMechanism="PLAIN",
    user="hiveuser",
    password="hivepass"
)

DATABASE = "default"

# selecting specific columns is slow with hive, whereas selecting all columns is fast
LONG_QUERY = "select col1, col2 from some_table"
SHORT_QUERY = "select * from some_table"


@gen.engine
def long_task(callback):
    print "starting long task"
    conn = TornadoConnection(**HIVE_SERVER_SETTINGS)
    yield gen.Task(conn.connect, database=DATABASE, configuration=None)
    cur = conn.cursor()
    yield gen.Task(cur.execute, LONG_QUERY)
    rows = yield gen.Task(cur.fetch)
    for row in rows:
        print row
    yield gen.Task(cur.close)
    yield gen.Task(conn.close)
    print "finished long task"
    callback()


@gen.engine
def short_task(callback):
    print "starting short task"
    conn = TornadoConnection(**HIVE_SERVER_SETTINGS)
    yield gen.Task(conn.connect, database=DATABASE, configuration=None)
    cur = conn.cursor()
    yield gen.Task(cur.execute, SHORT_QUERY)
    rows = yield gen.Task(cur.fetch)
    for row in rows:
        print row
    yield gen.Task(cur.close)
    yield gen.Task(conn.close)
    print "finished short task"
    if callback:
        callback()


def heartbeat():
    print "Non-blocking, yeah! You should see this message every second or so!"


def main():
    io_loop = ioloop.IOLoop.instance()

    def long_func():
        # once the long running task is done, it will stop the loop
        long_task(callback=io_loop.stop)

    def short_func():
        short_task(callback=None)

    # add the long running function first
    io_loop.add_callback(long_func)

    # add in a short running function that will run while the long running function is working
    io_loop.add_timeout(datetime.timedelta(seconds=3), short_func)

    # add in a periodic callback as well...simply prints out a message to make sure the ioloop isn't blocked
    p = ioloop.PeriodicCallback(heartbeat, 1000)
    p.start()

    # start the ioloop
    io_loop.start()


if __name__ == "__main__":
    main()