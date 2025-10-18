# -*- coding: utf-8 -*-
"""
Sistema de Logging para ETL
Logs estruturados com níveis INFO, WARNING, ERROR
"""
import logging
import sys
from datetime import datetime
from pathlib import Path

class ETLLogger:
    """Logger customizado para ETL com rotação de arquivos"""
    
    def __init__(self, log_dir="logs", log_level=logging.INFO):
        """
        Inicializa logger
        
        Args:
            log_dir: Diretório para salvar logs
            log_level: Nível de log (INFO, WARNING, ERROR)
        """
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(exist_ok=True)
        
        # Nome do arquivo de log com timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = self.log_dir / f"etl_{timestamp}.log"
        
        # Configurar logger
        self.logger = logging.getLogger("ETL")
        self.logger.setLevel(log_level)
        
        # Remover handlers existentes
        self.logger.handlers.clear()
        
        # Handler para arquivo
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setLevel(log_level)
        
        # Handler para console
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(log_level)
        
        # Formato do log
        formatter = logging.Formatter(
            '%(asctime)s | %(levelname)-8s | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)
        
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)
        
        self.log_file = log_file
        self.start_time = datetime.now()
        
        self.info(f"Log iniciado: {log_file}")
        self.info(f"Nível de log: {logging.getLevelName(log_level)}")
        self.info("="*70)
    
    def info(self, message):
        """Log de informação"""
        self.logger.info(message)
    
    def warning(self, message):
        """Log de aviso"""
        self.logger.warning(message)
    
    def error(self, message, exc_info=False):
        """Log de erro"""
        self.logger.error(message, exc_info=exc_info)
    
    def step(self, step_name):
        """Log de início de etapa"""
        self.info("")
        self.info("="*70)
        self.info(f"ETAPA: {step_name}")
        self.info("="*70)
    
    def data_quality(self, df_name, record_count, null_counts=None):
        """Log de qualidade de dados"""
        self.info(f"→ {df_name}: {record_count:,} registros")
        
        if null_counts:
            for col, count in null_counts.items():
                if count > 0:
                    pct = (count / record_count) * 100
                    self.warning(f"  - {col}: {count:,} nulos ({pct:.2f}%)")
    
    def query_result(self, query_name, result_count):
        """Log de resultado de query"""
        if result_count == 0:
            self.warning(f"⚠ QUERY VAZIA: {query_name} retornou 0 registros!")
        else:
            self.info(f"✓ {query_name}: {result_count:,} registros")
    
    def validation(self, check_name, passed, details=""):
        """Log de validação"""
        if passed:
            self.info(f"✓ Validação OK: {check_name} {details}")
        else:
            self.error(f"✗ Validação FALHOU: {check_name} {details}")
    
    def finalize(self):
        """Finaliza log com resumo"""
        elapsed = datetime.now() - self.start_time
        self.info("")
        self.info("="*70)
        self.info("ETL FINALIZADO")
        self.info(f"Tempo total: {elapsed}")
        self.info(f"Log salvo em: {self.log_file}")
        self.info("="*70)
    
    def exception(self, error, context=""):
        """Log de exceção com stack trace"""
        self.error(f"EXCEÇÃO {context}: {str(error)}", exc_info=True)


# Singleton para uso global
_logger_instance = None

def get_logger():
    """Retorna instância global do logger"""
    global _logger_instance
    if _logger_instance is None:
        _logger_instance = ETLLogger()
    return _logger_instance
