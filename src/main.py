from config import init_conn
from etl import setup_db, source_to_landing, load_to_marts

init_conn()

def main():
    setup_db()
    source_to_landing()
    load_to_marts()
    
if __name__ == "__main__":
    main()