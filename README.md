# Pipeline Brasil Escolas

Pipeline de automação para importação de dados de escolas para [brasilescolas.com.br](https://brasilescolas.com.br).

## Fluxo

```
01-fetch.yml → fetch_inep.py      → escolas_raw.json
     ↓
02-transform.yml → transform_schools.py → escolas_transformed.json
     ↓
03-import.yml → import_wp.py     → WordPress/Directorist
```

## GitHub Secrets necessários

| Secret | Valor |
|--------|-------|
| `WP_URL` | `https://brasilescolas.com.br` |
| `WP_USER` | `ljferramenta@gmail.com` |
| `WP_APP_PASSWORD` | Senha de aplicativo (WP Admin → Perfil → Senhas de Aplicativo) |

## Execução local

```bash
pip install requests
DATA_DIR=data python scripts/fetch_inep.py
DATA_DIR=data python scripts/transform_schools.py
DATA_DIR=data WP_URL=https://brasilescolas.com.br WP_USER=... WP_APP_PASSWORD=... python scripts/import_wp.py
```

## Agendamento

`01-fetch.yml` executa toda segunda-feira às 03:00 UTC e dispara os workflows seguintes em cadeia.
