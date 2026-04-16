/* ============================================================
   Brasil Escolas — Busca em tempo real
   ============================================================ */

class BrasilEscolasSearch {
  constructor(inputEl, dropdownEl, opts = {}) {
    this.input    = inputEl;
    this.dropdown = dropdownEl;
    this.onSelect = opts.onSelect || null;
    this.minChars = opts.minChars || 2;
    this._visible = false;
    this._init();
  }

  _init() {
    const handler = debounce(async (e) => {
      const val = e.target.value.trim();
      if (val.length < this.minChars) { this._hide(); return; }
      await this._buscar(val);
    }, CONFIG.BUSCA_DEBOUNCE);

    this.input.addEventListener('input', handler);
    this.input.addEventListener('keydown', e => this._handleKey(e));
    this.input.addEventListener('focus', () => {
      const val = this.input.value.trim();
      if (val.length === 0) this._mostrarHistorico();
      else if (val.length >= this.minChars) this._buscar(val);
    });

    document.addEventListener('click', e => {
      if (!this.input.contains(e.target) && !this.dropdown.contains(e.target)) {
        this._hide();
      }
    });
  }

  async _buscar(texto) {
    this._renderLoading();
    try {
      const resultados = await buscarPorTexto(texto);
      this._renderResultados(resultados, texto);
    } catch {
      this._renderErro();
    }
  }

  _renderLoading() {
    this.dropdown.innerHTML = '<div class="search-dropdown-empty"><span class="spinner"></span></div>';
    this._show();
  }

  _renderResultados(resultados, texto) {
    if (!resultados.length) {
      this.dropdown.innerHTML = `<div class="search-dropdown-empty">Nenhuma escola encontrada para "<strong>${texto}</strong>"</div>`;
      this._show();
      return;
    }
    const items = resultados.map((e, i) => `
      <div class="search-dropdown-item" tabindex="0" role="option"
           data-slug="${e.slug}" data-uf="${e.uf.toLowerCase()}"
           data-index="${i}" aria-label="${e.nome}, ${e.municipio}, ${e.uf}">
        <span class="search-dropdown-item-icon" aria-hidden="true">🏫</span>
        <div>
          <div class="search-dropdown-item-name">${e.nome}</div>
          <div class="search-dropdown-item-sub">${e.municipio} · ${e.uf} · ${this._depLabel(e.dependencia)}</div>
        </div>
      </div>`).join('');

    const hist = obterBuscasRecentes();
    const limpar = hist.length ? `<div style="padding:8px 16px;font-size:11px;color:var(--text-light);display:flex;justify-content:space-between;border-top:1px solid #F7FAFC;"><span>Resultados</span></div>` : '';
    this.dropdown.innerHTML = limpar + items;
    this._show();
    this._bindItems();
  }

  _mostrarHistorico() {
    const hist = obterBuscasRecentes();
    if (!hist.length) return;
    const items = hist.map(h => `
      <div class="search-dropdown-item search-hist-item" tabindex="0" role="option" data-hist="${h}">
        <span class="search-dropdown-item-icon" aria-hidden="true">🕐</span>
        <div>
          <div class="search-dropdown-item-name">${h}</div>
          <div class="search-dropdown-item-sub">Busca recente</div>
        </div>
      </div>`).join('');
    const footer = `<div style="padding:8px 16px;border-top:1px solid #F7FAFC;">
      <button onclick="limparHistoricoBusca();this.closest('.search-dropdown').innerHTML=''"
        style="font-size:11px;color:var(--text-light);background:none;border:none;cursor:pointer;">
        Limpar histórico
      </button></div>`;
    this.dropdown.innerHTML = items + footer;
    this._show();
    this._bindItems();
    this.dropdown.querySelectorAll('.search-hist-item').forEach(el => {
      el.addEventListener('click', () => {
        this.input.value = el.dataset.hist;
        this.input.dispatchEvent(new Event('input'));
      });
    });
  }

  _renderErro() {
    this.dropdown.innerHTML = '<div class="search-dropdown-empty">Erro ao buscar. Tente novamente.</div>';
    this._show();
  }

  _bindItems() {
    this.dropdown.querySelectorAll('.search-dropdown-item[data-slug]').forEach(el => {
      const go = () => {
        const slug = el.dataset.slug;
        const uf   = el.dataset.uf;
        salvarBuscaRecente(this.input.value.trim());
        if (this.onSelect) this.onSelect(slug, uf);
        else location.href = `/escolas/${uf}/${slug}/`;
        this._hide();
      };
      el.addEventListener('click', go);
      el.addEventListener('keydown', e => { if (e.key === 'Enter' || e.key === ' ') go(); });
    });
  }

  _handleKey(e) {
    const items = this.dropdown.querySelectorAll('.search-dropdown-item');
    const current = this.dropdown.querySelector('.search-dropdown-item:focus');
    let idx = -1;
    items.forEach((el, i) => { if (el === current) idx = i; });

    if (e.key === 'ArrowDown') {
      e.preventDefault();
      if (!this._visible) return;
      const next = items[idx + 1] || items[0];
      if (next) next.focus();
    } else if (e.key === 'ArrowUp') {
      e.preventDefault();
      const prev = items[idx - 1] || items[items.length - 1];
      if (prev) prev.focus();
      else this.input.focus();
    } else if (e.key === 'Escape') {
      this._hide();
      this.input.focus();
    } else if (e.key === 'Enter' && !this._visible) {
      this._submitBusca();
    }
  }

  _submitBusca() {
    const val = this.input.value.trim();
    if (!val) return;
    salvarBuscaRecente(val);
    const params = new URLSearchParams({ texto: val });
    location.href = `/escolas/?${params}`;
  }

  _show() { this.dropdown.style.display = 'block'; this._visible = true; }
  _hide() { this.dropdown.style.display = 'none';  this._visible = false; }
  _depLabel(d) {
    return { federal:'Federal', estadual:'Estadual', municipal:'Municipal', privada:'Privada' }[d] || d;
  }
}

/* ── Filtros de URL ── */
function lerFiltrosURL() {
  const p = new URLSearchParams(location.search);
  return {
    uf:          p.get('uf') || '',
    municipio:   p.get('municipio') || '',
    dependencia: p.get('dep') || '',
    niveis:      p.getAll('nivel'),
    tea:         p.get('tea') === 'true',
    tdah:        p.get('tdah') === 'true',
    dislexia:    p.get('dislexia') === 'true',
    acessibilidade: p.get('acess') === 'true',
    texto:       p.get('texto') || '',
    pagina:      parseInt(p.get('p') || '1', 10),
  };
}

function salvarFiltrosURL(filtros) {
  const p = new URLSearchParams();
  if (filtros.uf)          p.set('uf',       filtros.uf);
  if (filtros.municipio)   p.set('municipio', filtros.municipio);
  if (filtros.dependencia) p.set('dep',       filtros.dependencia);
  filtros.niveis?.forEach(n => p.append('nivel', n));
  if (filtros.tea)         p.set('tea',       'true');
  if (filtros.tdah)        p.set('tdah',      'true');
  if (filtros.dislexia)    p.set('dislexia',  'true');
  if (filtros.acessibilidade) p.set('acess',  'true');
  if (filtros.texto)       p.set('texto',     filtros.texto);
  if (filtros.pagina > 1)  p.set('p',         filtros.pagina);
  history.replaceState(null, '', `?${p.toString()}`);
}

/* ── Placeholder animado ── */
function iniciarPlaceholderAnimado(input, textos) {
  if (!input || !textos?.length) return;
  let i = 0;
  function tick() {
    input.setAttribute('placeholder', textos[i % textos.length]);
    i++;
  }
  tick();
  setInterval(tick, 2500);
}
