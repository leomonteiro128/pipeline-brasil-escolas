#!/usr/bin/env python3
"""Gera dados de amostra quando o artefato do INEP nao esta disponivel."""
import json
import datetime
import os
from pathlib import Path

DATA_DIR = Path(os.environ.get("DATA_DIR", "data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)

ufs = ["SP", "RJ", "MG", "BA", "RS"]
escolas = [
    {
        "NO_ENTIDADE": f"Escola {uf}-{i}",
        "CO_ENTIDADE": f"{i:08d}",
        "SG_UF": uf,
        "NO_MUNICIPIO": f"Cidade {uf}",
        "TP_DEPENDENCIA": 3,
        "TP_LOCALIZACAO": 1,
        "IN_NECESSIDADES_ESPECIAIS": i % 2 == 0,
    }
    for uf in ufs
    for i in range(1, 6)
]

output = {
    "metadata": {
        "source": "sample",
        "fetched_at": datetime.datetime.utcnow().isoformat(),
    },
    "escolas": escolas,
}

out_file = DATA_DIR / "escolas_raw.json"
with open(out_file, "w", encoding="utf-8") as f:
    json.dump(output, f, ensure_ascii=False, indent=2)

print(f"Sample gerado: {len(escolas)} escolas em {out_file}")
