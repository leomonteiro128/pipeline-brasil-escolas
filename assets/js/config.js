/* ============================================================
   Brasil Escolas — Configuração pública
   ATENÇÃO: Nunca expor SUPABASE_SERVICE_KEY aqui.
   Estes valores são injetados pelo pipeline no build.
   ============================================================ */

// Injetado pelo pipeline: scripts/gerar_pagina_escola.py
const SUPABASE_URL  = 'https://bagsommckxgkgrpmewip.supabase.co';
const SUPABASE_ANON = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImJhZ3NvbW1ja3hna2dycG1ld2lwIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzYyODc5NDEsImV4cCI6MjA5MTg2Mzk0MX0._PdQM24NGpftBeIx1bC8gFhwVcbTkElsWyBKTFwfNds';

const CONFIG = {
  SITE_NAME:    'Brasil Escolas',
  SITE_URL:     'https://brasilescolas.com.br',
  ADSENSE_ID:   'ca-pub-6391161522269960',
  AMAZON_TAG:   'brasilescolas-20',
  ESCOLAS_POR_PAGINA: 20,
  ARTIGOS_POR_PAGINA: 12,
  VAGAS_POR_PAGINA:   15,
  BUSCA_DEBOUNCE:     300,
  HISTORICO_MAX:      5,
};

Object.freeze(CONFIG);
