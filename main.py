from time import sleep
from psycopg2 import extensions, connect
from psycopg2.extras import LoggingConnection
import logging
from threading import Thread, Event

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()

def main():
    conn = connect(connection_factory=LoggingConnection,
    dbname = "postgres",
    user = "postgres",
    host = "localhost",
    password = "postgres"
    )

    conn.initialize(logger)
    conn.autocommit = False

    print("\npsycopg2.extensions:")
    for attr in dir(extensions):
        if "ISOLATION" in attr:
            print(f"    {attr}")
    print()

    conn.set_isolation_level(extensions.ISOLATION_LEVEL_READ_UNCOMMITTED)

    create_table(conn)
    dirty_read(conn)
    unrepeatable_read(conn)
    drop_table(conn)

    conn.close()


def dirty_read(conn):
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
        conn1.initialize(logger)
        conn1.autocommit = False
        conn1.set_isolation_level(extensions.ISOLATION_LEVEL_READ_UNCOMMITTED)
        cursor = conn1.cursor()
        cursor.execute("BEGIN;")
        cursor.execute("UPDATE cars SET max_speed = 250 WHERE model = 'Toyota';")
        print("Thread 1: Updated Toyota's max_speed to 250.")
        # cursor.execute("COMMIT;")
        sleep(1)
        sync_event.set()  # Signal thread2 to proceed
        sleep(3)
        cursor.execute("COMMIT;")
        print("Thread 1: Transaction committed.")
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
        conn2.initialize(logger)
        conn2.autocommit = False
        conn2.set_isolation_level(extensions.ISOLATION_LEVEL_READ_UNCOMMITTED)
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
 

def unrepeatable_read(conn):
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
        conn1.initialize(logger)
        conn1.autocommit = False
        conn1.set_isolation_level(extensions.ISOLATION_LEVEL_REPEATABLE_READ)
        cursor = conn1.cursor()
        cursor.execute("BEGIN;")
        cursor.execute("SELECT * FROM cars WHERE model='Renault';")
        rows = cursor.fetchall()
        print("Thread 1: Initial read:")
        for row in rows:
            print(f"    {row}")
        sync_event.set()  # Signal thread2 to proceed
        sleep(3)  # Wait for thread2 to commit changes
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
        conn2.initialize(logger)
        conn2.autocommit = False
        conn2.set_isolation_level(extensions.ISOLATION_LEVEL_REPEATABLE_READ)
        sync_event.wait()  # Wait for thread1 to signal
        cursor = conn2.cursor()
        cursor.execute("BEGIN;")
        cursor.execute("UPDATE cars SET max_speed = max_speed + 30 WHERE model='Renault';")
        print("Thread 2: Updated Renault's max_speed by 30.")
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