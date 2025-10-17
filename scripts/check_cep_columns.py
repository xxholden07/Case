# -*- coding: utf-8 -*-
import sqlite3

db_path = r"C:\Users\matheus.rodrigues\Downloads\Case\output\vendas_analytics.db"
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

print("\n=== COLUNAS DA TABELA visao_consolidada ===")
cursor.execute("PRAGMA table_info(visao_consolidada)")
for row in cursor.fetchall():
    print(f"  - {row[1]} ({row[2]})")

print("\n=== AMOSTRA DE DADOS (3 primeiros registros) ===")
cursor.execute("SELECT cliente_id, cidade, estado FROM visao_consolidada LIMIT 3")
for row in cursor.fetchall():
    print(f"  Cliente: {row[0]} | Cidade: {row[1]} | Estado: {row[2]}")

conn.close()
