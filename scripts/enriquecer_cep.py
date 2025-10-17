# -*- coding: utf-8 -*-
"""
Enriquecimento de CEP via API ViaCEP
Executa APÓS o ETL para adicionar cidade e estado
"""
import sqlite3
import requests
import time
from pathlib import Path

def consultar_cep(cep):
    """Consulta API ViaCEP"""
    try:
        cep_limpo = ''.join(filter(str.isdigit, str(cep)))
        if len(cep_limpo) != 8:
            return None, None
        
        url = f"https://viacep.com.br/ws/{cep_limpo}/json/"
        response = requests.get(url, timeout=5)
        
        if response.status_code == 200:
            dados = response.json()
            if 'erro' not in dados:
                return dados.get('localidade'), dados.get('uf')
        
        return None, None
    except Exception as e:
        print(f"  ⚠ Erro CEP {cep}: {e}")
        return None, None

def enriquecer_banco():
    """Enriquece banco SQLite com dados de CEP"""
    
    db_path = r"C:\Users\matheus.rodrigues\Downloads\Case\output\vendas_analytics.db"
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    print("\n" + "="*70)
    print("ENRIQUECIMENTO DE CEP VIA API ViaCEP")
    print("="*70 + "\n")
    
    # 1. Verificar se colunas existem
    cursor.execute("PRAGMA table_info(dim_cliente)")
    colunas = [row[1] for row in cursor.fetchall()]
    
    if 'cidade' not in colunas:
        cursor.execute("ALTER TABLE dim_cliente ADD COLUMN cidade TEXT")
        print("✓ Coluna 'cidade' adicionada")
    
    if 'estado' not in colunas:
        cursor.execute("ALTER TABLE dim_cliente ADD COLUMN estado TEXT")
        print("✓ Coluna 'estado' adicionada")
    
    conn.commit()
    
    # 2. Buscar CEPs únicos
    cursor.execute("SELECT DISTINCT cep FROM dim_cliente WHERE cep IS NOT NULL")
    ceps = cursor.fetchall()
    total = len(ceps)
    
    print(f"\n→ {total} CEPs únicos encontrados")
    print("\n→ Consultando API ViaCEP...\n")
    
    # 3. Consultar cada CEP
    for i, (cep,) in enumerate(ceps, 1):
        cidade, estado = consultar_cep(cep)
        
        if cidade and estado:
            cursor.execute("""
                UPDATE dim_cliente 
                SET cidade = ?, estado = ? 
                WHERE cep = ?
            """, (cidade, estado, cep))
            print(f"  [{i}/{total}] CEP {cep} → {cidade}/{estado}")
        else:
            cursor.execute("""
                UPDATE dim_cliente 
                SET cidade = 'Não encontrado', estado = 'Não encontrado' 
                WHERE cep = ?
            """, (cep,))
            print(f"  [{i}/{total}] CEP {cep} → Não encontrado")
        
        if i % 5 == 0:
            conn.commit()
            time.sleep(0.5)  # Rate limiting
    
    conn.commit()
    
    # 4. Atualizar tabelas relacionadas via JOIN
    print("\n→ Atualizando visao_consolidada...")
    cursor.execute("""
        UPDATE visao_consolidada
        SET cidade = (
            SELECT cidade FROM dim_cliente 
            WHERE dim_cliente.cliente_id = visao_consolidada.cliente_id
        ),
        estado = (
            SELECT estado FROM dim_cliente 
            WHERE dim_cliente.cliente_id = visao_consolidada.cliente_id
        )
    """)
    
    print("→ Atualizando metas_por_cliente...")
    cursor.execute("""
        UPDATE metas_por_cliente
        SET cidade = (
            SELECT cidade FROM dim_cliente 
            WHERE dim_cliente.cliente_id = metas_por_cliente.cliente_id
        ),
        estado = (
            SELECT estado FROM dim_cliente 
            WHERE dim_cliente.cliente_id = metas_por_cliente.cliente_id
        )
    """)
    
    conn.commit()
    
    # 5. Verificar resultado
    cursor.execute("""
        SELECT COUNT(*) FROM dim_cliente 
        WHERE cidade IS NOT NULL AND cidade != 'Não encontrado'
    """)
    enriquecidos = cursor.fetchone()[0]
    
    print(f"\n✓ {enriquecidos} clientes enriquecidos com sucesso!")
    print(f"✓ Banco atualizado: {db_path}\n")
    
    conn.close()

if __name__ == "__main__":
    enriquecer_banco()
