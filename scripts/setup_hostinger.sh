#!/bin/bash
# ============================================================
# FASE 0.2 — Estrutura de pastas no Hostinger via SSH
# Executar UMA VEZ antes do primeiro deploy.
# ssh -p 65002 u450071435@srv1783.hstgr.io < setup_hostinger.sh
# ============================================================

set -e
echo "=== Setup Hostinger Brasil Escolas ==="

# Backup da pasta atual
BACKUP="public_html_backup_$(date +%Y%m%d_%H%M%S)"
if [ -d ~/public_html ]; then
  echo "Fazendo backup: $BACKUP"
  mv ~/public_html ~/$BACKUP
  echo "Backup criado: ~/$BACKUP"
fi

# Criar estrutura nova
mkdir -p ~/public_html/assets/css
mkdir -p ~/public_html/assets/js
mkdir -p ~/public_html/assets/img
mkdir -p ~/public_html/assets/fonts
mkdir -p ~/public_html/assets/icons
mkdir -p ~/public_html/escolas
mkdir -p ~/public_html/artigos
mkdir -p ~/public_html/web-stories
mkdir -p ~/public_html/cadastrar-escola
mkdir -p ~/public_html/vagas-emprego-estagio
mkdir -p ~/public_html/sobre
mkdir -p ~/public_html/contato
mkdir -p ~/public_html/admin-panel
mkdir -p ~/public_html/jogos
mkdir -p ~/public_html/inclusao/tea
mkdir -p ~/public_html/inclusao/tdah
mkdir -p ~/public_html/inclusao/dislexia
mkdir -p ~/public_html/inclusao/acessibilidade

echo "=== Estrutura criada com sucesso ==="
echo ""
echo "Passos seguintes:"
echo "1. Fazer upload de assets/img/logo.png e favicon.png via SFTP"
echo "2. Fazer upload de todos os arquivos locais via deploy do pipeline"
echo "3. Criar projeto Supabase e executar o SQL schema"
echo ""
ls -la ~/public_html/
