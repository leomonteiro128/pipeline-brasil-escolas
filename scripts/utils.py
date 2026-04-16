"""
utils.py — Funções utilitárias compartilhadas do pipeline Brasil Escolas
"""

import re
import unicodedata
import json
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any, Optional

import requests

# ──────────────────────────────────────────────
# CONSTANTES
# ──────────────────────────────────────────────

UFS_VALIDAS = frozenset([
    "AC", "AL", "AM", "AP", "BA", "CE", "DF", "ES", "GO",
    "MA", "MG", "MS", "MT", "PA", "PB", "PE", "PI", "PR",
    "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP", "TO"
])


# ──────────────────────────────────────────────
# SLUG
# ──────────────────────────────────────────────

def slugify(texto: str) -> str:
    """
    Converte texto em slug URL-amigável.
    Remove acentos, converte para minúsculas, substitui espaços e
    caracteres especiais por hífens.

    Exemplo:
        slugify("Escola Estadual João da Silva — SP")
        → "escola-estadual-joao-da-silva-sp"
    """
    if not texto:
        return ""
    # Normaliza unicode → decompõe caracteres acentuados
    texto = unicodedata.normalize("NFD", texto)
    # Remove diacríticos (acentos)
    texto = "".join(c for c in texto if unicodedata.category(c) != "Mn")
    texto = texto.lower()
    # Substitui caracteres não alfanuméricos por hífen
    texto = re.sub(r"[^a-z0-9]+", "-", texto)
    # Remove hífens duplicados e nas bordas
    texto = re.sub(r"-{2,}", "-", texto).strip("-")
    return texto


# ──────────────────────────────────────────────
# VALIDAÇÕES
# ──────────────────────────────────────────────

def validar_uf(uf: str) -> bool:
    """
    Verifica se a sigla de estado é válida (uma das 27 UFs + DF).

    Args:
        uf: Sigla de estado (ex: "SP", "RJ", "DF")

    Returns:
        True se válida, False caso contrário.
    """
    if not uf or not isinstance(uf, str):
        return False
    return uf.strip().upper() in UFS_VALIDAS


def validar_cnpj(cnpj: str) -> bool:
    """
    Valida CNPJ usando o algoritmo oficial dos dígitos verificadores.

    Args:
        cnpj: CNPJ com ou sem formatação (ex: "11.222.333/0001-81")

    Returns:
        True se válido, False caso contrário.
    """
    if not cnpj:
        return False

    # Remove qualquer caractere não numérico
    cnpj = re.sub(r"\D", "", cnpj)

    if len(cnpj) != 14:
        return False

    # Rejeita sequências homogêneas (ex: 00000000000000)
    if re.match(r"^(\d)\1+$", cnpj):
        return False

    def calcular_digito(cnpj_parcial: str, pesos: list) -> int:
        soma = sum(int(d) * p for d, p in zip(cnpj_parcial, pesos))
        resto = soma % 11
        return 0 if resto < 2 else 11 - resto

    # Primeiro dígito verificador
    pesos1 = [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
    d1 = calcular_digito(cnpj[:12], pesos1)
    if int(cnpj[12]) != d1:
        return False

    # Segundo dígito verificador
    pesos2 = [6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
    d2 = calcular_digito(cnpj[:13], pesos2)
    if int(cnpj[13]) != d2:
        return False

    return True


# ──────────────────────────────────────────────
# FORMATAÇÃO
# ──────────────────────────────────────────────

def formatar_cep(cep: str) -> str:
    """
    Formata um CEP no padrão XXXXX-XXX.

    Args:
        cep: CEP com ou sem formatação (ex: "69900000" ou "69900-000")

    Returns:
        CEP formatado (ex: "69900-000") ou string vazia se inválido.
    """
    if not cep:
        return ""
    digits = re.sub(r"\D", "", str(cep))
    if len(digits) != 8:
        return ""
    return f"{digits[:5]}-{digits[5:]}"


def formatar_telefone(tel: str) -> str:
    """
    Formata telefone no padrão (XX) XXXX-XXXX ou (XX) XXXXX-XXXX.

    Args:
        tel: Número com ou sem formatação

    Returns:
        Telefone formatado ou string vazia se inválido.
    """
    if not tel:
        return ""
    digits = re.sub(r"\D", "", str(tel))
    # Remove código do país 55 se presente
    if digits.startswith("55") and len(digits) in (12, 13):
        digits = digits[2:]
    if len(digits) == 11:
        return f"({digits[:2]}) {digits[2:7]}-{digits[7:]}"
    if len(digits) == 10:
        return f"({digits[:2]}) {digits[2:6]}-{digits[6:]}"
    return ""


# ──────────────────────────────────────────────
# SUPABASE REST API
# ──────────────────────────────────────────────

def _supabase_headers(key: str) -> dict:
    return {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
        "Prefer": "return=representation",
    }


def inserir_supabase(tabela: str, dados: dict, url: str, key: str) -> dict:
    """
    Insere um registro em uma tabela do Supabase via REST API.

    Args:
        tabela: Nome da tabela (ex: "escolas")
        dados:  Dicionário com os campos e valores
        url:    URL base do projeto Supabase (ex: "https://xxx.supabase.co")
        key:    Chave de API (anon ou service_role)

    Returns:
        Dicionário com o registro inserido ou {"error": mensagem} em caso de falha.
    """
    endpoint = f"{url.rstrip('/')}/rest/v1/{tabela}"
    try:
        resp = requests.post(
            endpoint,
            headers=_supabase_headers(key),
            json=dados,
            timeout=20,
        )
        resp.raise_for_status()
        resultado = resp.json()
        if isinstance(resultado, list) and resultado:
            return resultado[0]
        return resultado if isinstance(resultado, dict) else {}
    except requests.exceptions.HTTPError as e:
        return {"error": f"HTTP {resp.status_code}: {resp.text}"}
    except requests.exceptions.RequestException as e:
        return {"error": str(e)}


def atualizar_supabase(tabela: str, id_registro: Any, dados: dict, url: str, key: str) -> dict:
    """
    Atualiza um registro existente no Supabase via REST API (PATCH).

    Args:
        tabela:      Nome da tabela
        id_registro: Valor da chave primária (id)
        dados:       Campos a atualizar
        url:         URL base do Supabase
        key:         Chave de API

    Returns:
        Registro atualizado ou {"error": mensagem}.
    """
    endpoint = f"{url.rstrip('/')}/rest/v1/{tabela}?id=eq.{id_registro}"
    headers = _supabase_headers(key)
    headers["Prefer"] = "return=representation"
    try:
        resp = requests.patch(endpoint, headers=headers, json=dados, timeout=20)
        resp.raise_for_status()
        resultado = resp.json()
        if isinstance(resultado, list) and resultado:
            return resultado[0]
        return resultado if isinstance(resultado, dict) else {}
    except requests.exceptions.HTTPError:
        return {"error": f"HTTP {resp.status_code}: {resp.text}"}
    except requests.exceptions.RequestException as e:
        return {"error": str(e)}


def buscar_supabase(tabela: str, filtros: dict, url: str, key: str) -> list:
    """
    Busca registros no Supabase com filtros de igualdade simples.

    Args:
        tabela:  Nome da tabela
        filtros: Dicionário campo→valor (todos aplicados como eq)
        url:     URL base do Supabase
        key:     Chave de API

    Returns:
        Lista de registros encontrados, ou lista vazia em caso de erro.

    Exemplo:
        buscar_supabase("escolas", {"uf": "SP", "status": "ativo"}, URL, KEY)
    """
    endpoint = f"{url.rstrip('/')}/rest/v1/{tabela}"
    params = {f"{campo}": f"eq.{valor}" for campo, valor in filtros.items()}
    try:
        resp = requests.get(
            endpoint,
            headers=_supabase_headers(key),
            params=params,
            timeout=20,
        )
        resp.raise_for_status()
        resultado = resp.json()
        return resultado if isinstance(resultado, list) else []
    except requests.exceptions.RequestException as e:
        print(f"[buscar_supabase] Erro: {e}")
        return []


# ──────────────────────────────────────────────
# SSH DEPLOY (paramiko)
# ──────────────────────────────────────────────

def deploy_via_ssh(
    local_path: str,
    remote_path: str,
    host: str,
    user: str,
    port: int = 22,
    key_path: Optional[str] = None,
) -> bool:
    """
    Faz upload de um arquivo local para um servidor remoto via SFTP (paramiko).

    Args:
        local_path:  Caminho completo do arquivo local
        remote_path: Caminho de destino no servidor remoto
        host:        Hostname ou IP do servidor
        user:        Usuário SSH
        port:        Porta SSH (padrão: 22)
        key_path:    Caminho para chave privada SSH (None = usa agente/chave padrão)

    Returns:
        True em caso de sucesso, False em caso de falha.
    """
    try:
        import paramiko
    except ImportError:
        print("[deploy_via_ssh] Erro: paramiko não instalado. Execute: pip install paramiko")
        return False

    try:
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        connect_kwargs = dict(hostname=host, port=port, username=user, timeout=30)
        if key_path:
            connect_kwargs["key_filename"] = str(key_path)

        client.connect(**connect_kwargs)
        sftp = client.open_sftp()

        # Garante que o diretório remoto existe
        remote_dir = str(Path(remote_path).parent)
        try:
            sftp.stat(remote_dir)
        except FileNotFoundError:
            # Cria diretório recursivamente
            dirs = []
            d = remote_dir
            while True:
                try:
                    sftp.stat(d)
                    break
                except FileNotFoundError:
                    dirs.append(d)
                    d = str(Path(d).parent)
                    if d == d:
                        break
            for d in reversed(dirs):
                try:
                    sftp.mkdir(d)
                except Exception:
                    pass

        sftp.put(str(local_path), remote_path)
        sftp.close()
        client.close()
        print(f"[deploy_via_ssh] Upload concluído: {local_path} → {host}:{remote_path}")
        return True

    except paramiko.AuthenticationException:
        print(f"[deploy_via_ssh] Erro de autenticação SSH para {user}@{host}")
        return False
    except paramiko.SSHException as e:
        print(f"[deploy_via_ssh] Erro SSH: {e}")
        return False
    except Exception as e:
        print(f"[deploy_via_ssh] Erro inesperado: {e}")
        return False


# ──────────────────────────────────────────────
# SITEMAP
# ──────────────────────────────────────────────

def atualizar_sitemap(url_nova: str, base_dir: str) -> bool:
    """
    Adiciona uma nova URL ao sitemap.xml existente no diretório base.
    Cria o sitemap do zero se não existir.
    Evita duplicatas verificando URLs já presentes.

    Args:
        url_nova: URL completa a adicionar (ex: "https://www.brasilescolas.com/escolas/escola-x/")
        base_dir: Diretório raiz onde sitemap.xml está localizado

    Returns:
        True em caso de sucesso, False em caso de falha.
    """
    sitemap_path = Path(base_dir) / "sitemap.xml"
    namespace = "http://www.sitemaps.org/schemas/sitemap/0.9"
    ET.register_namespace("", namespace)
    tag_urlset = f"{{{namespace}}}urlset"
    tag_url    = f"{{{namespace}}}url"
    tag_loc    = f"{{{namespace}}}loc"
    tag_changefreq = f"{{{namespace}}}changefreq"
    tag_priority   = f"{{{namespace}}}priority"

    try:
        if sitemap_path.exists():
            tree = ET.parse(str(sitemap_path))
            root = tree.getroot()
        else:
            root = ET.Element(tag_urlset)
            tree = ET.ElementTree(root)

        # Verifica se URL já existe
        urls_existentes = {
            loc.text.strip()
            for loc in root.iter(tag_loc)
            if loc.text
        }
        if url_nova in urls_existentes:
            print(f"[atualizar_sitemap] URL já existe no sitemap: {url_nova}")
            return True

        # Cria novo elemento <url>
        url_el = ET.SubElement(root, tag_url)
        loc_el = ET.SubElement(url_el, tag_loc)
        loc_el.text = url_nova

        changefreq_el = ET.SubElement(url_el, tag_changefreq)
        changefreq_el.text = "monthly"

        priority_el = ET.SubElement(url_el, tag_priority)
        priority_el.text = "0.7"

        # Indenta para legibilidade (Python 3.9+)
        try:
            ET.indent(root, space="  ")
        except AttributeError:
            pass  # Python < 3.9

        tree.write(
            str(sitemap_path),
            encoding="utf-8",
            xml_declaration=True,
        )
        print(f"[atualizar_sitemap] URL adicionada: {url_nova}")
        return True

    except ET.ParseError as e:
        print(f"[atualizar_sitemap] Erro ao parsear sitemap.xml: {e}")
        return False
    except OSError as e:
        print(f"[atualizar_sitemap] Erro ao salvar sitemap.xml: {e}")
        return False


# ──────────────────────────────────────────────
# TESTES RÁPIDOS (executar diretamente)
# ──────────────────────────────────────────────

if __name__ == "__main__":
    print("=== Testes utils.py ===\n")

    # slugify
    assert slugify("Escola Estadual João da Silva") == "escola-estadual-joao-da-silva"
    assert slugify("CENTRO EDUCACIONAL — São Paulo/SP") == "centro-educacional-sao-paulo-sp"
    assert slugify("") == ""
    print("✓ slugify")

    # validar_uf
    assert validar_uf("SP") is True
    assert validar_uf("DF") is True
    assert validar_uf("XX") is False
    assert validar_uf("") is False
    print("✓ validar_uf")

    # validar_cnpj
    assert validar_cnpj("11.222.333/0001-81") is True
    assert validar_cnpj("00000000000000") is False
    assert validar_cnpj("12345678000195") is True
    assert validar_cnpj("12345678000100") is False
    print("✓ validar_cnpj")

    # formatar_cep
    assert formatar_cep("69900000") == "69900-000"
    assert formatar_cep("01310100") == "01310-100"
    assert formatar_cep("abc") == ""
    print("✓ formatar_cep")

    # formatar_telefone
    assert formatar_telefone("68 3224-1234") == "(68) 3224-1234"
    assert formatar_telefone("11987654321") == "(11) 98765-4321"
    assert formatar_telefone("") == ""
    print("✓ formatar_telefone")

    print("\n✅ Todos os testes passaram.")
