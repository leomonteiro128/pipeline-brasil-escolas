#!/usr/bin/env python3
"""
Enriquece dados de escolas no Supabase usando Gemini 2.0 Flash + Google Search.

Para cada escola com dados de contato incompletos (sem telefone, site, endereço, etc.),
usa o Gemini com Search Grounding para buscar informações públicas atualizadas.

Variáveis de ambiente necessárias:
  SUPABASE_URL, SUPABASE_SERVICE_KEY, GEMINI_API_KEY
  BATCH_SIZE (opcional, default 20)
  OFFSET     (opcional, default 0 — para continuar de onde parou)
"""
import os, json, re, time, sys
import requests

SUPABASE_URL = os.environ['SUPABASE_URL']
SUPABASE_KEY = os.environ['SUPABASE_SERVICE_KEY']
GEMINI_KEY   = os.environ['GEMINI_API_KEY']
BATCH_SIZE   = int(os.environ.get('BATCH_SIZE', '20'))
OFFSET       = int(os.environ.get('OFFSET', '0'))

HEADERS_SB = {
    'apikey': SUPABASE_KEY,
    'Authorization': f'Bearer {SUPABASE_KEY}',
    'Content-Type': 'application/json',
    'Prefer': 'return=minimal'
}

CAMPOS_INCOMPLETOS = ['telefone', 'site', 'email', 'instagram', 'endereco', 'bairro', 'cep']


def buscar_escolas_incompletas():
    """Retorna escolas ativas com pelo menos telefone OU site nulos."""
    r = requests.get(
        f"{SUPABASE_URL}/rest/v1/escolas",
        headers=HEADERS_SB,
        params={
            'select': 'id,nome,municipio,uf,cnpj,' + ','.join(CAMPOS_INCOMPLETOS),
            'or': '(telefone.is.null,site.is.null)',
            'status': 'eq.ativo',
            'limit': BATCH_SIZE,
            'offset': OFFSET,
            'order': 'id.asc'
        }
    )
    r.raise_for_status()
    return r.json()


def gemini_buscar_contato(escola):
    """Usa Gemini 2.0 Flash com Search Grounding para encontrar dados de contato."""
    nome      = escola['nome']
    municipio = escola['municipio']
    uf        = escola['uf']
    cnpj      = escola.get('cnpj', '')

    prompt = (
        f'Encontre informações de contato atualizadas e verificadas desta escola brasileira:\n'
        f'Nome: {nome}\n'
        f'Município: {municipio} — {uf}\n'
        + (f'CNPJ: {cnpj}\n' if cnpj else '') +
        '\n'
        'Busque em fontes oficiais: site da prefeitura, INEP, QEdu, Google Maps, site da própria escola.\n'
        'Retorne SOMENTE um JSON válido com os campos que você encontrou com certeza.\n'
        'Não invente dados. Omita campos não encontrados.\n\n'
        '{\n'
        '  "telefone": "(XX) XXXX-XXXX ou (XX) 9XXXX-XXXX",\n'
        '  "site": "https://url-oficial.com.br",\n'
        '  "email": "email@escola.edu.br",\n'
        '  "instagram": "nome_sem_arroba",\n'
        '  "endereco": "Rua Nome, 123",\n'
        '  "bairro": "Nome do Bairro",\n'
        '  "cep": "XXXXX-XXX"\n'
        '}'
    )

    url = (
        'https://generativelanguage.googleapis.com/v1beta/'
        f'models/gemini-2.0-flash:generateContent?key={GEMINI_KEY}'
    )
    body = {
        'contents': [{'parts': [{'text': prompt}]}],
        'tools': [{'google_search': {}}],
        'generationConfig': {'temperature': 0.1, 'maxOutputTokens': 512}
    }

    try:
        r = requests.post(url, json=body, timeout=30)
        if r.status_code != 200:
            print(f'    ⚠ Gemini HTTP {r.status_code}: {r.text[:200]}')
            return {}

        candidates = r.json().get('candidates', [])
        if not candidates:
            return {}

        text = candidates[0]['content']['parts'][0]['text']

        # Extrair bloco JSON da resposta
        match = re.search(r'\{[\s\S]*?\}', text)
        if not match:
            return {}

        dados = json.loads(match.group())
        # Limpar valores vazios / placeholder
        limpos = {}
        for k, v in dados.items():
            v = str(v).strip()
            if v and v not in ('null', 'None', 'N/A', '-', ''):
                limpos[k] = v
        return limpos

    except (requests.RequestException, json.JSONDecodeError, KeyError, IndexError) as ex:
        print(f'    ⚠ Erro: {ex}')
        return {}


def atualizar_escola(escola_id, patch):
    """Aplica PATCH no registro da escola no Supabase."""
    r = requests.patch(
        f"{SUPABASE_URL}/rest/v1/escolas?id=eq.{escola_id}",
        headers=HEADERS_SB,
        json=patch
    )
    return r.status_code


def main():
    print(f'🔍 Buscando até {BATCH_SIZE} escolas com dados incompletos (offset={OFFSET})…\n')
    escolas = buscar_escolas_incompletas()

    if not escolas:
        print('✅ Nenhuma escola com dados incompletos encontrada.')
        sys.exit(0)

    print(f'📋 {len(escolas)} escolas para processar\n')
    print('─' * 60)

    atualizadas = 0
    sem_dados   = 0

    for i, e in enumerate(escolas, 1):
        print(f'[{i:02d}/{len(escolas):02d}] {e["nome"]} — {e["municipio"]}/{e["uf"]}')

        dados_gemini = gemini_buscar_contato(e)

        # Só preencher campos que estão VAZIOS no DB
        patch = {}
        for campo in CAMPOS_INCOMPLETOS:
            if campo in dados_gemini and not e.get(campo):
                patch[campo] = dados_gemini[campo]

        if patch:
            status = atualizar_escola(e['id'], patch)
            print(f'    ✅ Preenchido: {list(patch.keys())} → HTTP {status}')
            atualizadas += 1
        else:
            print('    ℹ️  Dados não encontrados ou já preenchidos')
            sem_dados += 1

        # Rate limit: ~7 req/min para não exceder o free tier do Gemini (15/min)
        if i < len(escolas):
            time.sleep(9)

    print()
    print('─' * 60)
    print(f'✅ Concluído: {atualizadas} enriquecidas | {sem_dados} sem dados novos')
    print(f'   Próximo OFFSET para continuar: {OFFSET + len(escolas)}')


if __name__ == '__main__':
    main()
