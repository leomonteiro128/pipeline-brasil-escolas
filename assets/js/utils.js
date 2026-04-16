/* ============================================================
   Brasil Escolas — Utilitários
   ============================================================ */

function slugify(texto) {
  return texto
    .toLowerCase()
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/[^a-z0-9\s-]/g, '')
    .trim()
    .replace(/\s+/g, '-')
    .replace(/-+/g, '-');
}

function formatarCEP(cep) {
  const s = String(cep).replace(/\D/g, '');
  return s.length === 8 ? `${s.slice(0,5)}-${s.slice(5)}` : cep;
}

function formatarTelefone(tel) {
  const s = String(tel).replace(/\D/g, '');
  if (s.length === 11) return `(${s.slice(0,2)}) ${s.slice(2,7)}-${s.slice(7)}`;
  if (s.length === 10) return `(${s.slice(0,2)}) ${s.slice(2,6)}-${s.slice(6)}`;
  return tel;
}

function formatarData(iso) {
  if (!iso) return '';
  try {
    return new Date(iso).toLocaleDateString('pt-BR', {
      day: 'numeric', month: 'long', year: 'numeric'
    });
  } catch { return iso; }
}

function truncarTexto(texto, limite = 150) {
  if (!texto || texto.length <= limite) return texto;
  return texto.slice(0, limite).replace(/\s+\S*$/, '') + '…';
}

function debounce(fn, delay = 300) {
  let timer;
  return function(...args) {
    clearTimeout(timer);
    timer = setTimeout(() => fn.apply(this, args), delay);
  };
}

function throttle(fn, limit = 200) {
  let inThrottle;
  return function(...args) {
    if (!inThrottle) {
      fn.apply(this, args);
      inThrottle = true;
      setTimeout(() => inThrottle = false, limit);
    }
  };
}

const UFS_VALIDAS = new Set([
  'AC','AL','AM','AP','BA','CE','DF','ES','GO',
  'MA','MG','MS','MT','PA','PB','PE','PI','PR',
  'RJ','RN','RO','RR','RS','SC','SE','SP','TO'
]);

function normalizarUF(uf) {
  if (!uf) return null;
  const upper = String(uf).toUpperCase().trim();
  return UFS_VALIDAS.has(upper) ? upper : null;
}

function gerarPlaceholderSVG(nome, cor = '#0085CA') {
  const iniciais = nome
    .split(' ')
    .filter(w => w.length > 2)
    .slice(0, 2)
    .map(w => w[0].toUpperCase())
    .join('');
  const svg = `<svg xmlns="http://www.w3.org/2000/svg" width="400" height="225" viewBox="0 0 400 225">
  <rect width="400" height="225" fill="${cor}"/>
  <text x="200" y="120" font-family="Arial,sans-serif" font-size="72" font-weight="bold"
    fill="rgba(255,255,255,0.9)" text-anchor="middle" dominant-baseline="middle">${iniciais}</text>
  <text x="200" y="185" font-family="Arial,sans-serif" font-size="16"
    fill="rgba(255,255,255,0.6)" text-anchor="middle">🏫</text>
</svg>`;
  return 'data:image/svg+xml;base64,' + btoa(unescape(encodeURIComponent(svg)));
}

function lazyLoadImages() {
  if (!('IntersectionObserver' in window)) {
    document.querySelectorAll('img[data-src]').forEach(img => {
      img.src = img.dataset.src;
    });
    return;
  }
  const observer = new IntersectionObserver((entries) => {
    entries.forEach(entry => {
      if (entry.isIntersecting) {
        const img = entry.target;
        img.src = img.dataset.src;
        if (img.dataset.srcset) img.srcset = img.dataset.srcset;
        img.removeAttribute('data-src');
        observer.unobserve(img);
      }
    });
  }, { rootMargin: '200px 0px' });
  document.querySelectorAll('img[data-src]').forEach(img => observer.observe(img));
}

function lazyLoadIframes() {
  if (!('IntersectionObserver' in window)) return;
  const observer = new IntersectionObserver((entries) => {
    entries.forEach(entry => {
      if (entry.isIntersecting) {
        const el = entry.target;
        if (el.dataset.src) {
          el.src = el.dataset.src;
          el.removeAttribute('data-src');
        }
        observer.unobserve(el);
      }
    });
  }, { rootMargin: '300px 0px' });
  document.querySelectorAll('iframe[data-src]').forEach(el => observer.observe(el));
}

function calcularTempoLeitura(texto) {
  const palavras = texto.trim().split(/\s+/).length;
  const minutos = Math.ceil(palavras / 200);
  return minutos <= 1 ? '1 min de leitura' : `${minutos} min de leitura`;
}

function contarPalavras(texto) {
  return texto.trim() === '' ? 0 : texto.trim().split(/\s+/).length;
}

/* ── Histórico de buscas ── */
function salvarBuscaRecente(termo) {
  if (!termo || termo.trim().length < 2) return;
  let hist = JSON.parse(localStorage.getItem('be_busca_historico') || '[]');
  hist = hist.filter(h => h !== termo);
  hist.unshift(termo);
  hist = hist.slice(0, CONFIG.HISTORICO_MAX);
  localStorage.setItem('be_busca_historico', JSON.stringify(hist));
}

function obterBuscasRecentes() {
  return JSON.parse(localStorage.getItem('be_busca_historico') || '[]');
}

function limparHistoricoBusca() {
  localStorage.removeItem('be_busca_historico');
}

/* ── Header/Footer dinâmicos ── */
function iniciarMenu() {
  const hamburger = document.getElementById('hamburger');
  const drawer    = document.getElementById('mobileDrawer');
  const overlay   = document.getElementById('mobileOverlay');
  const btnClose  = document.getElementById('drawerClose');

  function abrir() {
    drawer.classList.add('open');
    overlay.classList.add('open');
    hamburger.setAttribute('aria-expanded', 'true');
    document.body.style.overflow = 'hidden';
    btnClose.focus();
  }
  function fechar() {
    drawer.classList.remove('open');
    overlay.classList.remove('open');
    hamburger.setAttribute('aria-expanded', 'false');
    document.body.style.overflow = '';
    hamburger.focus();
  }

  if (hamburger) hamburger.addEventListener('click', abrir);
  if (overlay)   overlay.addEventListener('click', fechar);
  if (btnClose)  btnClose.addEventListener('click', fechar);

  document.addEventListener('keydown', e => {
    if (e.key === 'Escape' && drawer?.classList.contains('open')) fechar();
  });

  // Marcar link ativo
  const currentPath = location.pathname;
  document.querySelectorAll('.main-nav a, .mobile-drawer nav a').forEach(a => {
    if (a.getAttribute('href') === currentPath ||
        (currentPath !== '/' && a.getAttribute('href') !== '/' && currentPath.startsWith(a.getAttribute('href')))) {
      a.classList.add('active');
    }
  });
}

/* ── FAQ accordion ── */
function iniciarFAQ() {
  document.querySelectorAll('.faq-question').forEach(btn => {
    btn.addEventListener('click', () => {
      const item = btn.closest('.faq-item');
      const isOpen = item.classList.contains('open');
      document.querySelectorAll('.faq-item').forEach(i => i.classList.remove('open'));
      if (!isOpen) item.classList.add('open');
    });
  });
}

/* ── countUp animado ── */
function animarContadores() {
  const els = document.querySelectorAll('[data-count]');
  if (!els.length) return;
  const observer = new IntersectionObserver((entries) => {
    entries.forEach(entry => {
      if (!entry.isIntersecting) return;
      const el = entry.target;
      const target = parseInt(el.dataset.count, 10);
      const prefix = el.dataset.prefix || '';
      const suffix = el.dataset.suffix || '';
      const duration = 1500;
      const start = Date.now();
      function update() {
        const elapsed = Date.now() - start;
        const progress = Math.min(elapsed / duration, 1);
        const eased = 1 - Math.pow(1 - progress, 3);
        el.textContent = prefix + Math.floor(target * eased).toLocaleString('pt-BR') + suffix;
        if (progress < 1) requestAnimationFrame(update);
      }
      requestAnimationFrame(update);
      observer.unobserve(el);
    });
  }, { threshold: 0.3 });
  els.forEach(el => observer.observe(el));
}

document.addEventListener('DOMContentLoaded', () => {
  iniciarMenu();
  iniciarFAQ();
  lazyLoadImages();
  lazyLoadIframes();
  animarContadores();
});
