from psycopg2 import extensions, connect

def main():
    conn = connect(
    dbname = "postgres",
    user = "postgres",
    host = "localhost",
    password = "postgres"
    )

    print("\npsycopg2.extensions:")
    for attr in dir(extensions):
        if "ISOLATION" in attr:
            print(f"    {attr}")
    print()

    conn.set_isolation_level(extensions.ISOLATION_LEVEL_READ_UNCOMMITTED)

if __name__ == "__main__":
    main()