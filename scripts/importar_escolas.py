#!/usr/bin/env python3
"""
importar_escolas.py — Importa escolas do INEP para o Supabase (Brasil Escolas pipeline)

Fontes tentadas em ordem:
  1. API CKAN do INEP (dados.gov.br)
  2. QEdu (qedu.org.br)
  3. Fallback: data/escolas_inep_static.json (se existir)

NUNCA usa dados fictícios. Campos sem fonte real ficam como string vazia "".
"""

import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import Optional

import requests

# Importa utilitários do próprio pipeline
SCRIPTS_DIR = Path(__file__).parent
sys.path.insert(0, str(SCRIPTS_DIR))

from utils import (
    slugify,
    validar_uf,
    inserir_supabase,
    buscar_supabase,
    UFS_VALIDAS,
)

# ──────────────────────────────────────────────
# CONFIGURAÇÕES
# ──────────────────────────────────────────────

SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY", os.environ.get("SUPABASE_ANON_KEY", ""))

DATA_DIR = SCRIPTS_DIR.parent / "data"
PROGRESSO_FILE = DATA_DIR / "estado_progresso.json"
STATIC_FILE    = DATA_DIR / "escolas_inep_static.json"

# Ordem alfabética dos estados para rotação automática
ESTADOS_ORDEM = sorted(UFS_VALIDAS)

# Mapeamentos INEP
DEPENDENCIA_MAP = {1: "federal", 2: "estadual", 3: "municipal", 4: "privada"}
LOCALIZACAO_MAP = {1: "urbana", 2: "rural"}

# Resource IDs conhecidos do CKAN do INEP (escolas — censo escolar)
CKAN_RESOURCE_IDS = [
    "2b459b7e-6c4d-44f4-a64a-a5dd2f3d8b8f",  # Censo Escolar 2023 — escolas
    "7d63e6bc-fd85-4cff-a1fc-c0c7deece097",  # Censo Escolar 2022 — escolas
]

CKAN_BASE = "https://dados.gov.br/api/3/action/datastore_search"
QEDU_BASE = "https://api.qedu.org.br/v1/escolas"

REQUEST_TIMEOUT = 30
REQUEST_HEADERS = {
    "User-Agent": "BrasilEscolas-Pipeline/1.0 (github.com/leomonteiro128/pipeline-brasil-escolas)"
}


# ──────────────────────────────────────────────
# GERENCIAMENTO DE PROGRESSO
# ──────────────────────────────────────────────

def carregar_progresso() -> dict:
    """Carrega o arquivo de progresso de estados. Cria estrutura vazia se não existir."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    if PROGRESSO_FILE.exists():
        try:
            with open(PROGRESSO_FILE, encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            print(f"[progresso] Aviso: erro ao ler {PROGRESSO_FILE}: {e}. Recriando.")
    return {"ultimo_estado": None, "estados_concluidos": [], "historico": []}


def salvar_progresso(progresso: dict) -> None:
    """Salva o arquivo de progresso."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with open(PROGRESSO_FILE, "w", encoding="utf-8") as f:
        json.dump(progresso, f, ensure_ascii=False, indent=2)


def proximo_estado(progresso: dict, estado_forcado: Optional[str] = None) -> str:
    """
    Determina o próximo estado a processar.
    Se --estado foi informado, usa esse.
    Caso contrário, avança em ordem alfabética a partir do último processado.
    """
    if estado_forcado:
        uf = estado_forcado.upper().strip()
        if not validar_uf(uf):
            print(f"[estado] UF inválida: '{uf}'. UFs válidas: {', '.join(sorted(UFS_VALIDAS))}")
            sys.exit(1)
        return uf

    concluidos = set(progresso.get("estados_concluidos", []))
    ultimo = progresso.get("ultimo_estado")

    # Filtra estados ainda não concluídos
    pendentes = [uf for uf in ESTADOS_ORDEM if uf not in concluidos]

    if not pendentes:
        print("[progresso] Todos os estados já foram processados. Reiniciando ciclo.")
        progresso["estados_concluidos"] = []
        pendentes = list(ESTADOS_ORDEM)

    if ultimo and ultimo in ESTADOS_ORDEM:
        idx = ESTADOS_ORDEM.index(ultimo)
        # Pega próximo na lista que ainda não foi concluído
        for i in range(1, len(ESTADOS_ORDEM) + 1):
            candidato = ESTADOS_ORDEM[(idx + i) % len(ESTADOS_ORDEM)]
            if candidato in pendentes:
                return candidato

    return pendentes[0]


# ──────────────────────────────────────────────
# FONTES DE DADOS
# ──────────────────────────────────────────────

def buscar_ckan(estado: str, quantidade: int) -> list:
    """
    Fonte 1: API CKAN do INEP via dados.gov.br.
    Tenta múltiplos resource_ids até obter dados.
    """
    for resource_id in CKAN_RESOURCE_IDS:
        try:
            params = {
                "resource_id": resource_id,
                "filters": json.dumps({"SG_UF": estado}),
                "limit": quantidade,
            }
            print(f"[CKAN] Tentando resource_id={resource_id} para UF={estado}...")
            resp = requests.get(
                CKAN_BASE,
                params=params,
                headers=REQUEST_HEADERS,
                timeout=REQUEST_TIMEOUT,
            )
            resp.raise_for_status()
            dados = resp.json()

            if dados.get("success") and dados.get("result", {}).get("records"):
                registros = dados["result"]["records"]
                print(f"[CKAN] {len(registros)} registros obtidos para {estado}.")
                return registros
        except requests.exceptions.HTTPError as e:
            print(f"[CKAN] HTTP error com resource {resource_id}: {e}")
        except requests.exceptions.RequestException as e:
            print(f"[CKAN] Erro de rede: {e}")
        except (KeyError, ValueError) as e:
            print(f"[CKAN] Resposta inesperada: {e}")
    return []


def buscar_qedu(estado: str, quantidade: int) -> list:
    """
    Fonte 2: QEdu API (requer QEDU_TOKEN no ambiente).
    Retorna lista de registros no formato INEP normalizado.
    """
    token = os.environ.get("QEDU_TOKEN", "")
    if not token:
        print("[QEdu] QEDU_TOKEN não configurado. Pulando fonte QEdu.")
        return []

    try:
        params = {
            "uf": estado,
            "per_page": min(quantidade, 100),
            "page": 1,
        }
        headers = {**REQUEST_HEADERS, "Authorization": f"Bearer {token}"}
        resp = requests.get(QEDU_BASE, params=params, headers=headers, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        dados = resp.json()

        escolas_raw = dados.get("data", dados if isinstance(dados, list) else [])
        if not escolas_raw:
            return []

        # Normaliza para formato INEP
        normalizados = []
        for e in escolas_raw:
            normalizados.append({
                "CO_ENTIDADE":   str(e.get("inep_id", e.get("id", ""))),
                "NO_ENTIDADE":   e.get("name", e.get("nome", "")),
                "SG_UF":         estado,
                "NO_MUNICIPIO":  e.get("city", e.get("municipio", "")),
                "TP_DEPENDENCIA": _mapear_dependencia_qedu(e.get("dependence", e.get("dependencia", ""))),
                "TP_LOCALIZACAO": 1 if e.get("location", "").lower() == "urbana" else 2,
                "NO_BAIRRO":     e.get("neighborhood", e.get("bairro", "")),
                "DS_ENDERECO":   e.get("address", e.get("endereco", "")),
                "NU_CEP":        str(e.get("zip_code", e.get("cep", ""))),
                "NU_TELEFONE":   str(e.get("phone", e.get("telefone", ""))),
            })
        print(f"[QEdu] {len(normalizados)} registros obtidos para {estado}.")
        return normalizados

    except requests.exceptions.RequestException as e:
        print(f"[QEdu] Erro de rede: {e}")
        return []
    except (KeyError, ValueError) as e:
        print(f"[QEdu] Resposta inesperada: {e}")
        return []


def _mapear_dependencia_qedu(dep_str: str) -> int:
    """Converte string de dependência do QEdu para código INEP."""
    mapa = {"federal": 1, "estadual": 2, "municipal": 3, "privada": 4, "particular": 4}
    return mapa.get(str(dep_str).lower(), 3)


def buscar_static(estado: str, quantidade: int) -> list:
    """
    Fonte 3 (Fallback): lê data/escolas_inep_static.json se existir.
    Filtra pelo estado solicitado.
    """
    if not STATIC_FILE.exists():
        print(f"[Static] Arquivo {STATIC_FILE} não encontrado.")
        return []

    try:
        with open(STATIC_FILE, encoding="utf-8") as f:
            todos = json.load(f)

        if isinstance(todos, dict) and "escolas" in todos:
            todos = todos["escolas"]
        if not isinstance(todos, list):
            print("[Static] Formato inválido (esperado lista ou dict com chave 'escolas').")
            return []

        filtrados = [e for e in todos if str(e.get("SG_UF", "")).upper() == estado]
        resultado = filtrados[:quantidade]
        print(f"[Static] {len(resultado)} registros para {estado} no arquivo estático.")
        return resultado

    except (json.JSONDecodeError, OSError) as e:
        print(f"[Static] Erro ao ler arquivo: {e}")
        return []


def buscar_escolas_inep(estado: str, quantidade: int) -> list:
    """
    Tenta obter dados reais do INEP em ordem de prioridade:
    CKAN → QEdu → Static.
    Retorna lista de registros ou [] se nenhuma fonte funcionar.
    """
    print(f"\n[buscar] Iniciando busca para estado={estado}, quantidade={quantidade}")

    registros = buscar_ckan(estado, quantidade)
    if registros:
        return registros

    print("[buscar] CKAN sem dados. Tentando QEdu...")
    registros = buscar_qedu(estado, quantidade)
    if registros:
        return registros

    print("[buscar] QEdu sem dados. Tentando arquivo estático...")
    registros = buscar_static(estado, quantidade)
    if registros:
        return registros

    print(f"[buscar] Nenhuma fonte retornou dados para {estado}.")
    return []


# ──────────────────────────────────────────────
# VALIDAÇÃO E MAPEAMENTO DE REGISTRO
# ──────────────────────────────────────────────

def validar_registro(raw: dict) -> bool:
    """
    Valida campos obrigatórios de um registro INEP.
    - CO_ENTIDADE: deve ter 8 dígitos
    - SG_UF: deve ser UF válida
    """
    codigo = str(raw.get("CO_ENTIDADE", "")).strip()
    uf = str(raw.get("SG_UF", "")).strip().upper()

    if not codigo.isdigit() or len(codigo) != 8:
        return False
    if not validar_uf(uf):
        return False
    return True


def mapear_escola(raw: dict) -> dict:
    """
    Transforma um registro bruto do INEP no formato da tabela 'escolas' do Supabase.
    Campos sem fonte real ficam como string vazia "".
    """
    codigo    = str(raw.get("CO_ENTIDADE", "")).strip()
    nome      = str(raw.get("NO_ENTIDADE", "")).strip()
    uf        = str(raw.get("SG_UF", "")).strip().upper()
    municipio = str(raw.get("NO_MUNICIPIO", "")).strip()
    bairro    = str(raw.get("NO_BAIRRO", "") or "").strip()
    endereco  = str(raw.get("DS_ENDERECO", "") or "").strip()
    cep_raw   = str(raw.get("NU_CEP", "") or "").strip()
    telefone  = str(raw.get("NU_TELEFONE", "") or "").strip()

    dep_code  = raw.get("TP_DEPENDENCIA")
    loc_code  = raw.get("TP_LOCALIZACAO")

    try:
        dep_int = int(dep_code) if dep_code is not None else 3
    except (ValueError, TypeError):
        dep_int = 3

    try:
        loc_int = int(loc_code) if loc_code is not None else 1
    except (ValueError, TypeError):
        loc_int = 1

    dependencia  = DEPENDENCIA_MAP.get(dep_int, "municipal")
    localizacao  = LOCALIZACAO_MAP.get(loc_int, "urbana")

    from utils import formatar_cep, formatar_telefone
    cep_fmt = formatar_cep(cep_raw)
    tel_fmt = formatar_telefone(telefone)

    slug_base = slugify(f"{nome}-{municipio}-{uf}")

    return {
        "nome":         nome,
        "slug":         slug_base,  # unicidade garantida em garantir_slug_unico()
        "codigo_inep":  codigo,
        "uf":           uf,
        "municipio":    municipio,
        "bairro":       bairro,
        "endereco":     endereco,
        "cep":          cep_fmt,
        "telefone":     tel_fmt,
        "email":        "",
        "site":         "",
        "dependencia":  dependencia,
        "localizacao":  localizacao,
        "niveis":        [],
        "infraestrutura": [],
        "status":       "ativo",
        "fonte":        "inep_automatico",
    }


# ──────────────────────────────────────────────
# SLUG ÚNICO
# ──────────────────────────────────────────────

def garantir_slug_unico(slug_base: str, slugs_inseridos: set) -> str:
    """
    Garante unicidade do slug dentro da sessão atual de importação.
    Apenda -2, -3, ... em caso de colisão.
    """
    slug = slug_base
    contador = 2
    while slug in slugs_inseridos:
        slug = f"{slug_base}-{contador}"
        contador += 1
    return slug


# ──────────────────────────────────────────────
# VERIFICAÇÃO DE DUPLICATA NO SUPABASE
# ──────────────────────────────────────────────

def escola_existe(codigo_inep: str) -> bool:
    """Verifica se a escola já existe no Supabase pelo código INEP."""
    if not SUPABASE_URL or not SUPABASE_KEY:
        return False
    endpoint = f"{SUPABASE_URL.rstrip('/')}/rest/v1/escolas"
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
    }
    try:
        resp = requests.get(
            endpoint,
            headers=headers,
            params={"codigo_inep": f"eq.{codigo_inep}", "select": "id"},
            timeout=15,
        )
        resp.raise_for_status()
        return len(resp.json()) > 0
    except requests.exceptions.RequestException:
        return False


# ──────────────────────────────────────────────
# CHAMAR GERADOR DE PÁGINA
# ──────────────────────────────────────────────

def gerar_pagina(escola_id: str, slug: str) -> None:
    """Chama gerar_pagina_escola.py para a escola recém-inserida."""
    gerador = SCRIPTS_DIR / "gerar_pagina_escola.py"
    if not gerador.exists():
        return
    import subprocess
    try:
        result = subprocess.run(
            [sys.executable, str(gerador), "--slug", slug, "--id", str(escola_id)],
            capture_output=True, text=True, timeout=120
        )
        if result.returncode != 0:
            print(f"  [gerar_pagina] Aviso (slug={slug}): {result.stderr[:200]}")
    except subprocess.TimeoutExpired:
        print(f"  [gerar_pagina] Timeout para slug={slug}")
    except Exception as e:
        print(f"  [gerar_pagina] Erro: {e}")


# ──────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────

def main() -> int:
    parser = argparse.ArgumentParser(
        description="Importa escolas do INEP para o Supabase (Brasil Escolas pipeline)."
    )
    parser.add_argument(
        "--estado", "-e",
        type=str,
        default=None,
        help="UF específica a importar (ex: SP). Se omitido, avança automaticamente na ordem alfabética.",
    )
    parser.add_argument(
        "--quantidade", "-q",
        type=int,
        default=10,
        help="Quantidade máxima de escolas a importar (padrão: 10).",
    )
    args = parser.parse_args()

    # Validação básica de ambiente
    if not SUPABASE_URL:
        print("[ERRO] Variável de ambiente SUPABASE_URL não definida.")
        return 1
    if not SUPABASE_KEY:
        print("[ERRO] Variável de ambiente SUPABASE_SERVICE_KEY (ou SUPABASE_ANON_KEY) não definida.")
        return 1

    # Carrega progresso e determina estado
    progresso = carregar_progresso()
    estado = proximo_estado(progresso, args.estado)
    print(f"\n{'='*55}")
    print(f"  Brasil Escolas — Importação INEP")
    print(f"  Estado: {estado} | Quantidade: {args.quantidade}")
    print(f"{'='*55}\n")

    # Busca dados
    registros_raw = buscar_escolas_inep(estado, args.quantidade)
    if not registros_raw:
        print(f"[main] Nenhum registro encontrado para {estado}. Encerrando.")
        # Mesmo sem dados, avança o progresso para não ficar preso no mesmo estado
        progresso["ultimo_estado"] = estado
        if estado not in progresso.get("estados_concluidos", []):
            progresso.setdefault("estados_concluidos", []).append(estado)
        salvar_progresso(progresso)
        return 0

    # Contadores
    criadas = 0
    duplicatas = 0
    erros = 0
    slugs_inseridos: set = set()

    print(f"\n[main] Processando {len(registros_raw)} registros...\n")

    for i, raw in enumerate(registros_raw, 1):
        # Validação
        if not validar_registro(raw):
            print(f"  [{i}] Inválido (CO_ENTIDADE ou SG_UF). Pulando.")
            erros += 1
            continue

        codigo_inep = str(raw["CO_ENTIDADE"]).strip()
        nome = str(raw.get("NO_ENTIDADE", "")).strip() or "(sem nome)"

        print(f"  [{i}] {nome} ({codigo_inep})", end=" ")

        # Deduplicação
        if escola_existe(codigo_inep):
            print("→ duplicata, ignorada.")
            duplicatas += 1
            continue

        # Mapear campos
        escola = mapear_escola(raw)

        # Garantir slug único
        slug_unico = garantir_slug_unico(escola["slug"], slugs_inseridos)
        escola["slug"] = slug_unico
        slugs_inseridos.add(slug_unico)

        # Inserir no Supabase
        resultado = inserir_supabase("escolas", escola, SUPABASE_URL, SUPABASE_KEY)

        if "error" in resultado:
            print(f"→ ERRO: {resultado['error']}")
            erros += 1
            continue

        escola_id = resultado.get("id", "")
        print(f"→ criada (id={escola_id}, slug={slug_unico})")
        criadas += 1

        # Gerar página estática
        if escola_id:
            gerar_pagina(str(escola_id), slug_unico)

        # Pausa mínima para não sobrecarregar as APIs
        time.sleep(0.2)

    # Atualiza progresso
    progresso["ultimo_estado"] = estado
    if estado not in progresso.get("estados_concluidos", []):
        progresso.setdefault("estados_concluidos", []).append(estado)
    progresso.setdefault("historico", []).append({
        "estado": estado,
        "criadas": criadas,
        "duplicatas": duplicatas,
        "erros": erros,
    })
    salvar_progresso(progresso)

    # Relatório final
    print(f"\n{'='*55}")
    print(f"  Relatório final — {estado}")
    print(f"  Criadas:    {criadas}")
    print(f"  Duplicatas: {duplicatas}")
    print(f"  Erros:      {erros}")
    print(f"{'='*55}\n")

    # Exit code: 1 se houve apenas erros e nenhuma escola criada
    if erros > 0 and criadas == 0 and duplicatas == 0:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
