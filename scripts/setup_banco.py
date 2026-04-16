#!/usr/bin/env python3
"""
setup_banco.py — Executa o schema SQL no Supabase via conexão direta.

Como usar:
  1. Acesse: supabase.com → seu projeto → Settings → Database
  2. Copie a "Connection string" (URI mode) — ela contém a senha do banco
  3. Execute: python setup_banco.py --db-url "postgresql://postgres:SENHA@db.bagsommckxgkgrpmewip.supabase.co:5432/postgres"
     OU defina a variável: DB_PASSWORD=sua_senha e rode sem argumentos

Alternativa (SQL Editor manual):
  Abra supabase.com → SQL Editor → New query
  Cole o conteúdo de scripts/schema_supabase.sql e execute.
"""

import sys, os, argparse, pathlib
import psycopg2

SUPABASE_URL    = 'https://bagsommckxgkgrpmewip.supabase.co'
SUPABASE_HOST   = 'db.bagsommckxgkgrpmewip.supabase.co'
SUPABASE_POOLER = 'aws-0-sa-east-1.pooler.supabase.com'  # ajustar se necessário
SUPABASE_REF    = 'bagsommckxgkgrpmewip'

SCHEMA_FILE = pathlib.Path(__file__).parent / 'schema_supabase.sql'

def conectar(db_url: str):
    try:
        conn = psycopg2.connect(db_url, connect_timeout=20, sslmode='require')
        conn.autocommit = True
        print(f'✅ Conectado ao banco Supabase')
        return conn
    except Exception as e:
        print(f'❌ Falha na conexão: {e}')
        sys.exit(1)

def executar_schema(conn):
    sql = SCHEMA_FILE.read_text(encoding='utf-8')

    # Split em blocos por delimitador de comentário de seção ═══
    blocos = []
    bloco_atual = []
    for linha in sql.splitlines():
        if linha.startswith('--') and '═══' in linha and bloco_atual:
            bloco_sql = '\n'.join(bloco_atual).strip()
            if bloco_sql:
                blocos.append(bloco_sql)
            bloco_atual = [linha]
        else:
            bloco_atual.append(linha)
    if bloco_atual:
        blocos.append('\n'.join(bloco_atual).strip())

    cur = conn.cursor()
    erros = 0
    for i, bloco in enumerate(blocos, 1):
        if not bloco or bloco.startswith('--'):
            continue
        try:
            cur.execute(bloco)
            print(f'  ✅ Bloco {i} executado')
        except Exception as e:
            print(f'  ⚠️  Bloco {i}: {str(e)[:100]}')
            erros += 1

    # Verificar tabelas criadas
    cur.execute("""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = 'public'
        ORDER BY table_name
    """)
    tabelas = [row[0] for row in cur.fetchall()]
    print(f'\n📋 Tabelas no banco: {tabelas}')

    esperadas = {'escolas', 'escolas_cadastro_manual', 'artigos', 'web_stories', 'vagas'}
    criadas = set(tabelas) & esperadas
    ausentes = esperadas - set(tabelas)
    print(f'✅ Criadas: {sorted(criadas)}')
    if ausentes:
        print(f'❌ Ausentes: {sorted(ausentes)}')
    else:
        print('🎉 Todas as tabelas necessárias estão presentes!')

    return erros == 0

def main():
    parser = argparse.ArgumentParser(description='Setup banco Brasil Escolas')
    parser.add_argument('--db-url', help='URL de conexão PostgreSQL completa')
    parser.add_argument('--password', help='Apenas a senha do banco (sem precisar da URL completa)')
    args = parser.parse_args()

    password = args.password or os.environ.get('DB_PASSWORD') or os.environ.get('SUPABASE_DB_PASSWORD')

    if args.db_url:
        db_url = args.db_url
    elif password:
        db_url = f'postgresql://postgres:{password}@{SUPABASE_HOST}:5432/postgres'
    else:
        print('❌ Forneça --db-url ou --password (ou defina DB_PASSWORD no ambiente)')
        print('   Encontre a senha em: supabase.com → projeto → Settings → Database')
        sys.exit(1)

    print(f'🔌 Conectando a {SUPABASE_HOST}...')
    conn = conectar(db_url)
    sucesso = executar_schema(conn)
    conn.close()
    sys.exit(0 if sucesso else 1)

if __name__ == '__main__':
    main()
