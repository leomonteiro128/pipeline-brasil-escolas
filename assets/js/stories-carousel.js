/* ============================================================
   Brasil Escolas — Web Stories Carrossel
   Touch/swipe, keyboard navigation, scroll snap
   ============================================================ */

class StoriesCarousel {
  constructor(trackEl, opts = {}) {
    this.track   = trackEl;
    this.wrapper = trackEl.parentElement;
    this.dots    = opts.dotsEl || null;
    this._startX = 0;
    this._isDragging = false;
    this._init();
  }

  _init() {
    // Touch/swipe
    this.track.addEventListener('touchstart', e => {
      this._startX = e.touches[0].clientX;
    }, { passive: true });
    this.track.addEventListener('touchend', e => {
      const dx = e.changedTouches[0].clientX - this._startX;
      if (Math.abs(dx) > 50) {
        if (dx < 0) this._scroll(1);
        else this._scroll(-1);
      }
    }, { passive: true });

    // Keyboard
    this.track.setAttribute('tabindex', '0');
    this.track.addEventListener('keydown', e => {
      if (e.key === 'ArrowRight') { e.preventDefault(); this._scroll(1); }
      if (e.key === 'ArrowLeft')  { e.preventDefault(); this._scroll(-1); }
    });

    // Dots ativos
    this.track.addEventListener('scroll', throttle(() => this._atualizarDots(), 100));
  }

  _scroll(direcao) {
    const cards = this.track.querySelectorAll('.story-card');
    if (!cards.length) return;
    const cardW = cards[0].offsetWidth + 16;
    this.track.scrollBy({ left: direcao * cardW * 2, behavior: 'smooth' });
  }

  _atualizarDots() {
    if (!this.dots) return;
    const cards = this.track.querySelectorAll('.story-card');
    if (!cards.length) return;
    const centro = this.track.scrollLeft + this.track.offsetWidth / 2;
    let closest = 0, minDist = Infinity;
    cards.forEach((c, i) => {
      const dist = Math.abs(c.offsetLeft + c.offsetWidth / 2 - centro);
      if (dist < minDist) { minDist = dist; closest = i; }
    });
    this.dots.querySelectorAll('.carousel-dot').forEach((d, i) => {
      d.classList.toggle('active', i === closest);
      d.setAttribute('aria-selected', i === closest);
    });
  }
}

/* ── Gerar dots dinamicamente ── */
function criarDots(carouselEl, total) {
  const dotsEl = document.createElement('div');
  dotsEl.className = 'carousel-dots';
  dotsEl.setAttribute('role', 'tablist');
  dotsEl.setAttribute('aria-label', 'Navegação dos stories');
  for (let i = 0; i < total; i++) {
    const dot = document.createElement('button');
    dot.className = `carousel-dot ${i === 0 ? 'active' : ''}`;
    dot.setAttribute('role', 'tab');
    dot.setAttribute('aria-selected', i === 0 ? 'true' : 'false');
    dot.setAttribute('aria-label', `Story ${i + 1}`);
    dot.addEventListener('click', () => {
      const track = carouselEl.querySelector('.stories-carousel');
      const cards = track.querySelectorAll('.story-card');
      if (cards[i]) cards[i].scrollIntoView({ behavior: 'smooth', block: 'nearest', inline: 'center' });
    });
    dotsEl.appendChild(dot);
  }
  return dotsEl;
}

/* ── Inicializar carrossel ── */
async function iniciarStoriesCarrossel(containerEl) {
  if (!containerEl) return;
  const track = containerEl.querySelector('.stories-carousel');
  if (!track) return;

  // Carregar stories do Supabase
  try {
    const stories = await buscarStories(8);
    if (!stories.length) {
      containerEl.closest('.stories-section')?.classList.add('hidden');
      return;
    }

    track.innerHTML = stories.map(s => `
      <a href="/web-stories/${s.slug}/" class="story-card"
         aria-label="${s.titulo}">
        ${s.capa_url
          ? `<img data-src="${s.capa_url}" alt="${s.titulo}" loading="lazy">`
          : `<div style="background:var(--azul-escuro);width:100%;height:100%;display:flex;align-items:center;justify-content:center;font-size:40px;">📖</div>`}
        <div class="story-card-overlay">
          <span class="story-card-title">${s.titulo}</span>
        </div>
      </a>`).join('');

    const dots = criarDots(containerEl, stories.length);
    track.parentElement.insertAdjacentElement('afterend', dots);

    new StoriesCarousel(track, { dotsEl: dots });
    lazyLoadImages();

  } catch {
    containerEl.closest('.stories-section')?.classList.add('hidden');
  }
}
