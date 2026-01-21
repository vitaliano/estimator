#cria um novo DB noehub deletando o anterior
#cria e popula as tabelas login_camera e peopleflowtotals 
#a partir do csv genado no script anterior

import os
import sqlite3
from datetime import datetime
import csv


def setup_camera_data_db():

    DB_NAME = "nodehub.db"

    # -----------------------------------------
    # Remove o banco se já existir
    # -----------------------------------------
    if os.path.exists(DB_NAME):
        os.remove(DB_NAME)
        print("Database deleted:", DB_NAME)

    # -----------------------------------------
    # Cria conexão com o novo banco
    # -----------------------------------------
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    # -----------------------------------------
    # Cria tabela peopleflowtotals
    # -----------------------------------------
    cursor.execute("""
    CREATE TABLE peopleflowtotals (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        created_at DATETIME,
        camera_id INTEGER,
        total_inside INTEGER,
        total_outside INTEGER,
        valid INTEGER
    );
    """)

    # -----------------------------------------
    # Cria tabela login_camera
    # -----------------------------------------

    cursor.execute("""
    CREATE TABLE login_camera (
            id INTEGER, 
            client TEXT NOT NULL DEFAULT net3rcorp, 
            location TEXT NOT NULL DEFAULT teste, 
            entrance TEXT, 
            door TEXT DEFAULT central,
            pong_ts DATETIME, 
            pong_ts_last_fail DATETIME, 
            counting_hour_sunday INTEGER NOT NULL DEFAULT (9), 
            counting_hour_sunday_qtd INTEGER NOT NULL DEFAULT (22), 
            counting_hour_monday INTEGER NOT NULL DEFAULT (9), 
            counting_hour_monday_qtd INTEGER NOT NULL DEFAULT (22), 
            counting_hour_tuesday INTEGER NOT NULL DEFAULT (9), 
            counting_hour_tuesday_qtd INTEGER NOT NULL DEFAULT (22), 
            counting_hour_wednesday INTEGER NOT NULL DEFAULT (9), 
            counting_hour_wednesday_qtd INTEGER NOT NULL DEFAULT (22), 
            counting_hour_thursday INTEGER NOT NULL DEFAULT (9), 
            counting_hour_thursday_qtd INTEGER NOT NULL DEFAULT (22), 
            counting_hour_fryday INTEGER NOT NULL DEFAULT (9), 
            counting_hour_fryday_qtd INTEGER NOT NULL DEFAULT (22), 
            counting_hour_saturday INTEGER NOT NULL DEFAULT (9), 
            counting_hour_saturday_qtd INTEGER NOT NULL DEFAULT (22), 
            counting_hour_holiday INTEGER NOT NULL DEFAULT (9), 
            counting_hour_holiday_qtd INTEGER NOT NULL DEFAULT (22)
            )         
    """)

    # -----------------------------------------
    # Cria tabela holidays
    # -----------------------------------------

    cursor.execute("""
    CREATE TABLE holidays (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date DATETIME NOT NULL ,
        type TEXT NOT NULL DEFAULT closed,  -- 'sunday' ou 'closed'   
        description TEXT 
    )
    """)


    conn.commit()
    conn.close()

    print("Database created successfully with required tables.")


    # -----------------------------------------
    # popula a tabela peopleflowtotals
    # -----------------------------------------


    CSV_FILE = "camera_data.csv"

    # Conecta ao banco
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    # Abre o CSV
    with open(CSV_FILE, "r") as f:
        reader = csv.reader(f)
        
        
        header = next(reader)

        # Descobre índices das colunas
        idx_camera = header.index("camera_id")
        idx_created_at = header.index("created_at")
        idx_total_inside = header.index("total_inside")
        idx_total_outside = header.index("total_outside")
        idx_valid = header.index("valid")
        
        next(reader)  # pula o header

        for row in reader:
            camera_id = int(row[idx_camera])
            created_at = row[idx_created_at]  # já vem como string no formato correto
            total_inside = int(row[idx_total_inside])
            total_outside = int(row[idx_total_outside])
            valid = int(row[idx_valid])

            cursor.execute("""
                INSERT INTO peopleflowtotals    
                (camera_id, created_at, total_inside, total_outside, valid)
                VALUES (?, ?, ?, ?, ?)
            """, (camera_id, created_at, total_inside, total_outside, valid))

    cursor.execute("UPDATE peopleflowtotals SET created_at = strftime('%Y-%m-%d %H:00:00', created_at)")
    cursor.execute("UPDATE  peopleflowtotals  SET created_at = datetime(created_at, '+14 days')")
    
    # Salva e fecha
    conn.commit()
    conn.close()

    print("Import completed: data inserted into peopleflowtotals.")


    # -----------------------------------------
    # popula a tabela login_camera
    # -----------------------------------------

    CSV_FILE = "camera_data.csv"

    # Conecta ao banco
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    # Dicionário: camera_id → (location, last_created_at)
    camera_info = {}

    with open(CSV_FILE, "r") as f:
        reader = csv.reader(f)
        header = next(reader)

        # Descobre índices das colunas
        idx_camera = header.index("camera_id")
        idx_location = header.index("location")
        idx_created_at= header.index("created_at")

        for row in reader:
            camera_id = int(row[idx_camera])
            location = row[idx_location]
            created_at_str = row[idx_created_at]

            # Converte string para datetime
            created_at = datetime.fromisoformat(created_at_str)

            # Se ainda não existe, adiciona
            if camera_id not in camera_info:
                camera_info[camera_id] = (location, created_at)
            else:
                # Atualiza se o timestamp for mais recente
                _, last_ts = camera_info[camera_id]
                if created_at > last_ts:
                    camera_info[camera_id] = (location, created_at)

    # Insere no banco
    for camera_id, (location, last_ts) in camera_info.items():
        cursor.execute("""
            INSERT INTO login_camera (id, entrance, pong_ts, pong_ts_last_fail)
            VALUES (?, ?, ?, NULL)
        """, (camera_id, location, last_ts.isoformat()))

    conn.commit()
    conn.close()
    print("Import completed: data inserted into login_camera.")
    
 

    # -----------------------------------------
    # popula a tabela holidays
    # -----------------------------------------


    # Conecta ao banco
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    cursor.execute("""
            INSERT INTO holidays ( date, type, description)
            VALUES (?, ?, ?)
        """, ('2025-12-25', 'closed', 'natal'))
    cursor.execute("""
            INSERT INTO holidays ( date, type, description)
            VALUES (?, ?, ?)
        """, ('2025-01-01', 'closed', 'ano novo'))
    
    conn.commit()
    conn.close()
    print("Import completed: data inserted into holidays.")


if __name__ == "__main__":
    setup_camera_data_db()
    print("End of db setup script")