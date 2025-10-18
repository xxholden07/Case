"""
Exporta resultados do Spark para SQLite
"""
import sqlite3
import pandas as pd
import os

def criar_banco_sqlite():
    """Cria banco SQLite e carrega dados dos CSVs"""
    
    print("\n" + "="*70)
    print("EXPORTANDO DADOS PARA SQLITE")
    print("="*70)
    
    # Caminho do banco
    db_path = r"C:\Users\matheus.rodrigues\Downloads\Case\output\vendas_analytics.db"
    csv_dir = r"C:\Users\matheus.rodrigues\Downloads\Case\output\csv"
    
    # Conectar ao SQLite
    conn = sqlite3.connect(db_path)
    print(f"\nâ†’ Conectado ao banco: {db_path}")
    
    # Lista de tabelas para importar
    tabelas = [
        'visao_consolidada',
        'metas_por_cliente',
        'dim_cliente',
        'dim_produto',
        'fato_vendas'
    ]
    
    for tabela in tabelas:
        csv_file = os.path.join(csv_dir, f"{tabela}.csv")
        
        if os.path.exists(csv_file):
            print(f"\nâ†’ Importando {tabela}...")
            df = pd.read_csv(csv_file)
            df.to_sql(tabela, conn, if_exists='replace', index=False)
            print(f"  âœ“ {len(df)} registros salvos na tabela '{tabela}'")
        else:
            print(f"  âš  Arquivo nÃ£o encontrado: {csv_file}")
    
    # Criar Ã­ndices para melhor performance
    print("\nâ†’ Criando Ã­ndices...")
    cursor = conn.cursor()
    
    indices = [
        "CREATE INDEX IF NOT EXISTS idx_cliente ON visao_consolidada(cliente_id)",
        "CREATE INDEX IF NOT EXISTS idx_metas_cliente ON metas_por_cliente(cliente_id)",
        "CREATE INDEX IF NOT EXISTS idx_metas_marca ON metas_por_cliente(marca)",
        "CREATE INDEX IF NOT EXISTS idx_fato_cliente ON fato_vendas(sk_cliente)",
        "CREATE INDEX IF NOT EXISTS idx_fato_produto ON fato_vendas(sk_produto)",
    ]
    
    for idx in indices:
        try:
            cursor.execute(idx)
        except Exception as e:
            print(f"  âš  Ãndice jÃ¡ existe ou erro: {e}")
    
    conn.commit()
    conn.close()
    
    print("\nâœ“ Banco SQLite criado com sucesso!")
    print(f"  LocalizaÃ§Ã£o: {db_path}")
    
    return db_path

if __name__ == "__main__":
    criar_banco_sqlite()

