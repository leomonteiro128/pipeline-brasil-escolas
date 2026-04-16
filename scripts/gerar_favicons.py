#!/usr/bin/env python3
"""
Gera todas as variantes de favicon a partir de favicon.png.
Executar uma vez após colocar favicon.png em assets/img/.
"""
from PIL import Image
import json, os, sys

SRC = os.path.join(os.path.dirname(__file__), '..', 'assets', 'img', 'favicon.png')
OUT = os.path.join(os.path.dirname(__file__), '..', 'assets', 'img')

# Para rodar localmente no site:
LOCAL_SRC = 'C:/Users/leo-m/OneDrive/Área de Trabalho/BRASIL ESCOLAS v2/assets/img/favicon.png'
LOCAL_OUT  = 'C:/Users/leo-m/OneDrive/Área de Trabalho/BRASIL ESCOLAS v2/assets/img'

if os.path.exists(LOCAL_SRC):
    SRC = LOCAL_SRC
    OUT = LOCAL_OUT
elif not os.path.exists(SRC):
    print(f"ERRO: favicon.png não encontrado em {SRC}")
    sys.exit(1)

img = Image.open(SRC).convert('RGBA')

sizes = [
    ('favicon-32.png', 32),
    ('favicon-16.png', 16),
    ('apple-touch-icon.png', 180),
    ('android-chrome-192.png', 192),
    ('android-chrome-512.png', 512),
]

for fname, size in sizes:
    resized = img.resize((size, size), Image.LANCZOS)
    path = os.path.join(OUT, fname)
    resized.save(path, 'PNG', optimize=True)
    print(f"OK: {fname} ({size}x{size})")

# Gera site.webmanifest
manifest = {
    "name": "Brasil Escolas",
    "short_name": "Brasil Escolas",
    "icons": [
        {"src": "/assets/img/android-chrome-192.png", "sizes": "192x192", "type": "image/png"},
        {"src": "/assets/img/android-chrome-512.png", "sizes": "512x512", "type": "image/png"}
    ],
    "theme_color": "#003F7F",
    "background_color": "#F0F8FF",
    "display": "standalone"
}

manifest_path = os.path.join(OUT, '..', '..', 'site.webmanifest')
if os.path.exists('C:/Users/leo-m/OneDrive/Área de Trabalho/BRASIL ESCOLAS v2'):
    manifest_path = 'C:/Users/leo-m/OneDrive/Área de Trabalho/BRASIL ESCOLAS v2/site.webmanifest'

with open(manifest_path, 'w') as f:
    json.dump(manifest, f, indent=2)
print("OK: site.webmanifest")
print("Todos os favicons gerados com sucesso!")
