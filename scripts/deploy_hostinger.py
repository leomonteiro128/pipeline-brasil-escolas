#!/usr/bin/env python3
"""
deploy_hostinger.py — Deploy completo do site Brasil Escolas para o Hostinger.

Uso:
  python deploy_hostinger.py --password SENHA_SSH
  python deploy_hostinger.py  # lê SSH_PASSWORD do .env ou ambiente

Requer: pip install paramiko
"""

import os, sys, pathlib, argparse, getpass
import paramiko
from paramiko import SSHClient, AutoAddPolicy
from paramiko.sftp_client import SFTPClient

# ─── Configuração ───
SSH_HOST = 'srv1783.hstgr.io'
SSH_PORT = 65002
SSH_USER = 'u450071435'
REMOTE_BASE = '/home/u450071435/public_html'

LOCAL_SITE = pathlib.Path(r'C:\Users\leo-m\OneDrive\Área de Trabalho\BRASIL ESCOLAS v2')

EXCLUDE_PATTERNS = {'.env', '__pycache__', '.git', '.DS_Store', 'node_modules', '*.pyc', 'Thumbs.db'}

def should_exclude(name: str) -> bool:
    if name in EXCLUDE_PATTERNS:
        return True
    if name.endswith('.pyc'):
        return True
    return False

def sftp_mkdir_p(sftp: SFTPClient, remote_path: str):
    """Create remote directory and parents if they don't exist."""
    parts = remote_path.split('/')
    current = ''
    for part in parts:
        if not part:
            current = '/'
            continue
        current = f'{current}/{part}' if current != '/' else f'/{part}'
        try:
            sftp.stat(current)
        except FileNotFoundError:
            try:
                sftp.mkdir(current)
            except Exception:
                pass

def upload_directory(sftp: SFTPClient, local_dir: pathlib.Path, remote_dir: str, verbose=True):
    """Recursively upload a local directory to a remote path."""
    uploaded = 0
    for item in local_dir.iterdir():
        if should_exclude(item.name):
            continue
        remote_path = f'{remote_dir}/{item.name}'
        if item.is_dir():
            sftp_mkdir_p(sftp, remote_path)
            count = upload_directory(sftp, item, remote_path, verbose)
            uploaded += count
        elif item.is_file():
            sftp.put(str(item), remote_path)
            if verbose:
                rel = str(item.relative_to(LOCAL_SITE))
                print(f'  ↑ {rel}')
            uploaded += 1
    return uploaded

def setup_hostinger(ssh: SSHClient):
    """Run initial server setup — create directory structure."""
    commands = [
        f'mkdir -p {REMOTE_BASE}/assets/css',
        f'mkdir -p {REMOTE_BASE}/assets/js',
        f'mkdir -p {REMOTE_BASE}/assets/fonts',
        f'mkdir -p {REMOTE_BASE}/assets/img',
        f'mkdir -p {REMOTE_BASE}/assets/icons',
        f'mkdir -p {REMOTE_BASE}/escolas',
        f'mkdir -p {REMOTE_BASE}/artigos',
        f'mkdir -p {REMOTE_BASE}/web-stories',
        f'mkdir -p {REMOTE_BASE}/inclusao/tea',
        f'mkdir -p {REMOTE_BASE}/inclusao/tdah',
        f'mkdir -p {REMOTE_BASE}/inclusao/dislexia',
        f'mkdir -p {REMOTE_BASE}/inclusao/acessibilidade',
        f'mkdir -p {REMOTE_BASE}/cadastrar-escola',
        f'mkdir -p {REMOTE_BASE}/vagas-emprego-estagio',
        f'mkdir -p {REMOTE_BASE}/sobre',
        f'mkdir -p {REMOTE_BASE}/contato',
        f'mkdir -p {REMOTE_BASE}/admin-panel',
    ]
    print('🔧 Criando estrutura de diretórios...')
    for cmd in commands:
        stdin, stdout, stderr = ssh.exec_command(cmd)
        stdout.channel.recv_exit_status()
    print('✅ Estrutura criada')

def main():
    parser = argparse.ArgumentParser(description='Deploy Brasil Escolas para Hostinger')
    parser.add_argument('--password', '-p', help='Senha SSH do Hostinger')
    parser.add_argument('--setup-only', action='store_true', help='Apenas criar estrutura de diretórios')
    parser.add_argument('--quiet', action='store_true', help='Menos output')
    args = parser.parse_args()

    password = args.password or os.environ.get('SSH_PASSWORD') or os.environ.get('HOSTINGER_PASSWORD')
    if not password:
        # Try loading from .env
        env_file = LOCAL_SITE / '.env'
        if env_file.exists():
            for line in env_file.read_text().splitlines():
                if line.startswith('SSH_PASSWORD='):
                    password = line.split('=', 1)[1].strip()
                    break
    if not password:
        password = getpass.getpass(f'Senha SSH para {SSH_USER}@{SSH_HOST}: ')

    print(f'🔌 Conectando a {SSH_HOST}:{SSH_PORT}...')
    ssh = SSHClient()
    ssh.set_missing_host_key_policy(AutoAddPolicy())
    try:
        ssh.connect(SSH_HOST, port=SSH_PORT, username=SSH_USER, password=password, timeout=20)
        print('✅ SSH conectado')
    except Exception as e:
        print(f'❌ Falha na conexão SSH: {e}')
        sys.exit(1)

    setup_hostinger(ssh)

    if not args.setup_only:
        print(f'\n📤 Iniciando upload de {LOCAL_SITE}...')
        sftp = ssh.open_sftp()
        total = upload_directory(sftp, LOCAL_SITE, REMOTE_BASE, verbose=not args.quiet)
        sftp.close()
        print(f'\n✅ Deploy concluído: {total} arquivos enviados para {REMOTE_BASE}')

        # Verify deployment
        stdin, stdout, stderr = ssh.exec_command(f'ls {REMOTE_BASE}/')
        files = stdout.read().decode().strip()
        print(f'\n📁 Conteúdo de public_html:\n{files}')

        # Quick HTTP test
        import urllib.request
        try:
            resp = urllib.request.urlopen('https://brasilescolas.com.br/', timeout=10)
            print(f'\n🌐 Site respondendo: HTTP {resp.status}')
        except Exception as e:
            print(f'\n⚠️  HTTP test: {e}')

    ssh.close()
    print('\n🎉 Concluído!')

if __name__ == '__main__':
    main()
