"""
Script de Teste Rápido
Verifica se todas as dependências estão instaladas corretamente
"""

def testar_instalacao():
    """Testa instalação de todas as dependências"""
    
    print("="*70)
    print("TESTE DE INSTALAÇÃO DE DEPENDÊNCIAS")
    print("="*70)
    
    erros = []
    
    # Teste 1: PySpark
    print("\n→ Testando PySpark...")
    try:
        from pyspark.sql import SparkSession
        from pyspark.sql.functions import col, sum, avg
        print("  ✓ PySpark instalado corretamente")
    except ImportError as e:
        erros.append(f"PySpark: {str(e)}")
        print(f"  ✗ Erro ao importar PySpark: {str(e)}")
    
    # Teste 2: Pandas
    print("\n→ Testando Pandas...")
    try:
        import pandas as pd
        print(f"  ✓ Pandas {pd.__version__} instalado corretamente")
    except ImportError as e:
        erros.append(f"Pandas: {str(e)}")
        print(f"  ✗ Erro ao importar Pandas: {str(e)}")
    
    # Teste 3: Openpyxl (para ler Excel)
    print("\n→ Testando Openpyxl...")
    try:
        import openpyxl
        print(f"  ✓ Openpyxl {openpyxl.__version__} instalado corretamente")
    except ImportError as e:
        erros.append(f"Openpyxl: {str(e)}")
        print(f"  ✗ Erro ao importar Openpyxl: {str(e)}")
    
    # Teste 4: Requests (para API)
    print("\n→ Testando Requests...")
    try:
        import requests
        print(f"  ✓ Requests {requests.__version__} instalado corretamente")
    except ImportError as e:
        erros.append(f"Requests: {str(e)}")
        print(f"  ✗ Erro ao importar Requests: {str(e)}")
    
    # Teste 5: Java (necessário para Spark)
    print("\n→ Testando Java...")
    try:
        import subprocess
        result = subprocess.run(['java', '-version'], 
                              capture_output=True, 
                              text=True,
                              timeout=5)
        if result.returncode == 0:
            java_version = result.stderr.split('\n')[0]
            print(f"  ✓ Java instalado: {java_version}")
        else:
            erros.append("Java não encontrado")
            print("  ✗ Java não encontrado ou não configurado")
    except FileNotFoundError:
        erros.append("Java não encontrado no PATH")
        print("  ✗ Java não encontrado no PATH")
    except Exception as e:
        erros.append(f"Java: {str(e)}")
        print(f"  ✗ Erro ao verificar Java: {str(e)}")
    
    # Teste 6: Arquivos de entrada
    print("\n→ Verificando arquivos de entrada...")
    import os
    base_dir = r"C:\Users\matheus.rodrigues\Downloads\Case"
    
    arquivos_necessarios = [
        "sales.xlsx",
        "metas por marca.csv"
    ]
    
    for arquivo in arquivos_necessarios:
        caminho = os.path.join(base_dir, arquivo)
        if os.path.exists(caminho):
            tamanho = os.path.getsize(caminho) / 1024  # KB
            print(f"  ✓ {arquivo} encontrado ({tamanho:.2f} KB)")
        else:
            erros.append(f"Arquivo não encontrado: {arquivo}")
            print(f"  ✗ {arquivo} NÃO encontrado")
    
    # Teste 7: Criar sessão Spark (teste básico)
    print("\n→ Testando criação de sessão Spark...")
    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.builder \
            .appName("Teste") \
            .config("spark.driver.memory", "1g") \
            .getOrCreate()
        
        spark.sparkContext.setLogLevel("ERROR")
        
        # Teste simples
        df = spark.createDataFrame([(1, "teste")], ["id", "nome"])
        count = df.count()
        
        if count == 1:
            print(f"  ✓ Spark Session criada e testada com sucesso")
        
        spark.stop()
    except Exception as e:
        erros.append(f"Spark Session: {str(e)}")
        print(f"  ✗ Erro ao criar Spark Session: {str(e)}")
    
    # Teste 8: API ViaCEP
    print("\n→ Testando API ViaCEP...")
    try:
        import requests
        response = requests.get("https://viacep.com.br/ws/01310100/json/", timeout=5)
        if response.status_code == 200:
            dados = response.json()
            if 'localidade' in dados:
                print(f"  ✓ API ViaCEP acessível (teste: {dados['localidade']}-{dados['uf']})")
            else:
                print("  ⚠ API respondeu mas formato inesperado")
        else:
            print(f"  ⚠ API retornou status {response.status_code}")
    except Exception as e:
        print(f"  ⚠ Erro ao testar API (não crítico): {str(e)}")
    
    # Resumo
    print("\n" + "="*70)
    if len(erros) == 0:
        print("✓ TODAS AS VERIFICAÇÕES PASSARAM!")
        print("="*70)
        print("\nVocê está pronto para executar o script principal:")
        print("  python spark_analysis.py")
        return True
    else:
        print("✗ ALGUNS PROBLEMAS FORAM ENCONTRADOS")
        print("="*70)
        print("\nErros encontrados:")
        for i, erro in enumerate(erros, 1):
            print(f"  {i}. {erro}")
        
        print("\n💡 Soluções recomendadas:")
        if any("PySpark" in e for e in erros):
            print("  • Instale PySpark: pip install pyspark")
        if any("Pandas" in e for e in erros):
            print("  • Instale Pandas: pip install pandas")
        if any("Openpyxl" in e for e in erros):
            print("  • Instale Openpyxl: pip install openpyxl")
        if any("Requests" in e for e in erros):
            print("  • Instale Requests: pip install requests")
        if any("Java" in e for e in erros):
            print("  • Instale Java 8 ou 11 e configure JAVA_HOME")
        if any("Arquivo" in e for e in erros):
            print("  • Verifique se os arquivos estão no diretório correto")
        
        print("\nOu instale todas as dependências de uma vez:")
        print("  pip install -r requirements.txt")
        
        return False


if __name__ == "__main__":
    import sys
    sucesso = testar_instalacao()
    sys.exit(0 if sucesso else 1)
