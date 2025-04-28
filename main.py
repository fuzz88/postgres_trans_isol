from time import sleep
from psycopg2 import extensions, connect
from psycopg2.extras import LoggingConnection
import logging
from threading import Thread, Event

logging.basicConfig(level=logging.DEBUG)

def main():
    conn = connect(dbname="postgres",
                   user="postgres",
                   host="localhost",
                   password="postgres"
                   )

    create_table(conn)
    print("-----------------dirty_read with READ_COMMITTED")
    dirty_read(extensions.ISOLATION_LEVEL_READ_COMMITTED)
    drop_table(conn)
    create_table(conn)
    print("-----------------unrepeatable_read with READ_COMMITTED")
    unrepeatable_read(extensions.ISOLATION_LEVEL_READ_COMMITTED)
    drop_table(conn)
    create_table(conn)
    print("-----------------unrepeatable_read with REPEATABLE_READ")
    unrepeatable_read(extensions.ISOLATION_LEVEL_REPEATABLE_READ)
    drop_table(conn)
    create_table(conn)
    print("-----------------phantom_read with READ_COMMITTED")
    phantom_read(extensions.ISOLATION_LEVEL_READ_COMMITTED)
    drop_table(conn)
    create_table(conn)
    print("-----------------phantom_read with REPEATABLE_READ")
    phantom_read(extensions.ISOLATION_LEVEL_REPEATABLE_READ)
    drop_table(conn)

    conn.close()


def dirty_read(isolation_level):
    sync_event = Event()

    def thread1_func():
        # Create a new connection for thread1
        conn1 = connect(
            connection_factory=LoggingConnection,
            dbname="postgres",
            user="postgres",
            host="localhost",
            password="postgres"
        )
        logger = logging.getLogger("thread1")
        conn1.initialize(logger)
        conn1.autocommit = False
        conn1.set_isolation_level(isolation_level)
        cursor = conn1.cursor()
        cursor.execute("BEGIN;")
        cursor.execute("UPDATE cars SET max_speed = 250 WHERE model = 'Toyota';")
        sync_event.set()  # Signal thread2 to proceed
        sleep(1)
        cursor.execute("COMMIT;")
        cursor.close()
        conn1.close()

    def thread2_func():
        # Create a new connection for thread2
        conn2 = connect(
            connection_factory=LoggingConnection,
            dbname="postgres",
            user="postgres",
            host="localhost",
            password="postgres"
        )
        logger = logging.getLogger("thread2")
        conn2.initialize(logger)
        conn2.autocommit = False
        conn2.set_isolation_level(isolation_level)
        sync_event.wait()  # Wait for thread1 to signal
        cursor = conn2.cursor()
        cursor.execute("BEGIN;")
        cursor.execute("SELECT model, max_speed FROM cars;")
        rows = cursor.fetchall()
        print("Thread 2: Read cars table:")
        for row in rows:
            print(f"    {row}")
        print("done")
        cursor.close()
        conn2.close()

    thread1 = Thread(target=thread1_func)
    thread2 = Thread(target=thread2_func)

    thread1.start()
    thread2.start()

    thread1.join()
    thread2.join()


def unrepeatable_read(isolation_level):
    sync_event = Event()

    def thread1_func():
        # Create a new connection for thread1
        conn1 = connect(
            connection_factory=LoggingConnection,
            dbname="postgres",
            user="postgres",
            host="localhost",
            password="postgres"
        )
        logger = logging.getLogger("thread1")
        conn1.initialize(logger)
        conn1.autocommit = False
        conn1.set_isolation_level(isolation_level)
        cursor = conn1.cursor()
        cursor.execute("BEGIN;")
        cursor.execute("SELECT * FROM cars WHERE model='Renault';")
        rows = cursor.fetchall()
        print("Thread 1: Initial read:")
        for row in rows:
            print(f"    {row}")
        sync_event.set()  # Signal thread2 to proceed
        sleep(1)  # Wait for thread2 to commit changes
        cursor.execute("SELECT * FROM cars WHERE model='Renault';")
        rows = cursor.fetchall()
        print("Thread 1: Second read:")
        for row in rows:
            print(f"    {row}")
        cursor.execute("COMMIT;")
        cursor.close()
        conn1.close()

    def thread2_func():
        # Create a new connection for thread2
        conn2 = connect(
            connection_factory=LoggingConnection,
            dbname="postgres",
            user="postgres",
            host="localhost",
            password="postgres"
        )
        logger = logging.getLogger("thread2")
        conn2.initialize(logger)
        conn2.autocommit = False
        conn2.set_isolation_level(isolation_level)
        sync_event.wait()  # Wait for thread1 to signal
        cursor = conn2.cursor()
        cursor.execute("BEGIN;")
        cursor.execute("UPDATE cars SET max_speed = max_speed + 30 WHERE model='Renault';")
        cursor.execute("COMMIT;")
        cursor.close()
        conn2.close()

    thread1 = Thread(target=thread1_func)
    thread2 = Thread(target=thread2_func)

    thread1.start()
    thread2.start()

    thread1.join()
    thread2.join()


def phantom_read(isolation_level):
    sync_event = Event()

    def thread1_func():
        # Create a new connection for thread1
        conn1 = connect(
            connection_factory=LoggingConnection,
            dbname="postgres",
            user="postgres",
            host="localhost",
            password="postgres"
        )
        logger = logging.getLogger("thread1")
        conn1.initialize(logger)
        conn1.autocommit = False
        conn1.set_isolation_level(isolation_level)
        cursor = conn1.cursor()
        cursor.execute("BEGIN;")
        cursor.execute("SELECT * FROM cars;")
        rows = cursor.fetchall()
        print("Thread 1: Initial read:")
        for row in rows:
            print(f"    {row}")
        sync_event.set()  # Signal thread2 to proceed
        sleep(1)  # Wait for thread2 to commit changes
        cursor.execute("SELECT * FROM cars;")
        rows = cursor.fetchall()
        print("Thread 1: Second read:")
        for row in rows:
            print(f"    {row}")
        cursor.execute("COMMIT;")
        cursor.close()
        conn1.close()

    def thread2_func():
        # Create a new connection for thread2
        conn2 = connect(
            connection_factory=LoggingConnection,
            dbname="postgres",
            user="postgres",
            host="localhost",
            password="postgres"
        )
        logger = logging.getLogger("thread2")
        conn2.initialize(logger)
        conn2.autocommit = False
        conn2.set_isolation_level(isolation_level)
        sync_event.wait()  # Wait for thread1 to signal
        cursor = conn2.cursor()
        cursor.execute("BEGIN;")
        cursor.execute("DELETE FROM cars WHERE model = 'Renault';")
        cursor.execute("COMMIT;")
        cursor.close()
        conn2.close()

    thread1 = Thread(target=thread1_func)
    thread2 = Thread(target=thread2_func)

    thread1.start()
    thread2.start()

    thread1.join()
    thread2.join()


def drop_table(conn):
    with conn.cursor() as cursor:
        cursor.execute("DROP TABLE IF EXISTS cars;")
        conn.commit()


def create_table(conn):
    with conn.cursor() as cursor:
        cursor.execute("DROP TABLE IF EXISTS cars;")
        cursor.execute("""CREATE TABLE IF NOT EXISTS cars (
id BIGSERIAL PRIMARY KEY,
model varchar(20) UNIQUE,
max_speed INTEGER);""")
        cursor.execute("""INSERT INTO cars (model, max_speed) VALUES 
('Toyota', 150),
('Lada', 130),
('BMW', 200),
('Renault', 170),
('Mercedes', 230);""")
        conn.commit()


if __name__ == "__main__":
    main()