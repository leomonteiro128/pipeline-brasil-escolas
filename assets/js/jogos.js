/* ============================================================
   Brasil Escolas — Jogos Educativos Acessíveis
   WCAG 2.1 AA | Sem timer | Feedback 100% positivo | Sem flashes
   ============================================================ */

/* ═══ JOGO MEMÓRIA ═══ */
class JogoMemoria {
  constructor(container, nivel = 'normal') {
    this.container = container;
    this.nivel = nivel;
    this.pares = [
      { id: 1, emoji: '👨‍⚕️', nome: 'Médico' },
      { id: 2, emoji: '👩‍🏫', nome: 'Professor' },
      { id: 3, emoji: '👨‍🚒', nome: 'Bombeiro' },
      { id: 4, emoji: '👷', nome: 'Engenheiro' },
      { id: 5, emoji: '🎨', nome: 'Pintor' },
      { id: 6, emoji: '🎵', nome: 'Músico' },
      { id: 7, emoji: '👨‍🍳', nome: 'Chef' },
      { id: 8, emoji: '🚀', nome: 'Astronauta' },
    ];
    this.cartasViradas  = [];
    this.paresEncontrados = 0;
    this.bloqueado = false;
    this.render();
  }

  render() {
    const cards = [...this.pares, ...this.pares]
      .map((p, i) => ({ ...p, uniqueId: i }))
      .sort(() => Math.random() - 0.5);

    this.container.setAttribute('role', 'game');
    this.container.setAttribute('aria-label', 'Jogo da Memória de Profissões');

    this.container.innerHTML = `
      <div class="jogo-header">
        <h3 class="jogo-titulo">🎴 Jogo da Memória</h3>
        <div class="jogo-placar" aria-live="polite" aria-label="Placar">
          <span>Pares: <span class="jogo-placar-valor" id="placarPares">0</span> / 8</span>
        </div>
      </div>
      <div class="jogo-feedback neutro" id="feedbackMemoria" aria-live="polite" aria-atomic="true">
        Encontre os pares! Clique nas cartas para virá-las.
      </div>
      <div class="memoria-grid" id="memoriaGrid" role="list">
        ${cards.map(c => `
          <div class="carta" tabindex="0" role="listitem"
               data-par="${c.id}" data-uid="${c.uniqueId}"
               aria-label="Carta ${c.uniqueId + 1} — virada para baixo"
               aria-pressed="false">
            <div class="carta-inner" aria-hidden="true">
              <div class="carta-frente">🃏</div>
              <div class="carta-verso">${c.emoji}</div>
            </div>
          </div>`).join('')}
      </div>
      <div class="jogo-controles">
        <button class="btn btn-primary btn-sm" id="btnReiniciarMem"
          aria-label="Reiniciar jogo da memória">
          🔄 Reiniciar
        </button>
      </div>`;

    this._bindEventos();
  }

  _bindEventos() {
    const grid = this.container.querySelector('#memoriaGrid');
    const reiniciar = this.container.querySelector('#btnReiniciarMem');

    grid.addEventListener('click', e => {
      const carta = e.target.closest('.carta');
      if (carta) this._virarCarta(carta);
    });
    grid.addEventListener('keydown', e => {
      if (e.key === 'Enter' || e.key === ' ') {
        e.preventDefault();
        const carta = e.target.closest('.carta');
        if (carta) this._virarCarta(carta);
      }
    });
    reiniciar.addEventListener('click', () => this.render());
  }

  _virarCarta(carta) {
    if (this.bloqueado) return;
    if (carta.classList.contains('virada') || carta.classList.contains('encontrada')) return;
    if (this.cartasViradas.length === 2) return;

    carta.classList.add('virada');
    carta.setAttribute('aria-pressed', 'true');
    carta.setAttribute('aria-label', `Carta virada — ${this._getNomePar(carta.dataset.par)}`);
    this.cartasViradas.push(carta);

    if (this.cartasViradas.length === 2) this._checarPar();
  }

  _checarPar() {
    this.bloqueado = true;
    const [a, b] = this.cartasViradas;
    const acertou = a.dataset.par === b.dataset.par;

    const delay = this.nivel === 'tea' ? 1200 : 700;
    setTimeout(() => {
      if (acertou) {
        a.classList.add('encontrada');
        b.classList.add('encontrada');
        a.classList.remove('virada');
        b.classList.remove('virada');
        a.setAttribute('aria-label', `Par encontrado — ${this._getNomePar(a.dataset.par)}`);
        b.setAttribute('aria-label', `Par encontrado — ${this._getNomePar(b.dataset.par)}`);
        this.paresEncontrados++;
        this._atualizarPlacar();
        this._feedback(true);
        if (this.paresEncontrados === 8) this._vitoria();
      } else {
        a.classList.remove('virada');
        b.classList.remove('virada');
        a.setAttribute('aria-pressed', 'false');
        b.setAttribute('aria-pressed', 'false');
        a.setAttribute('aria-label', `Carta — virada para baixo`);
        b.setAttribute('aria-label', `Carta — virada para baixo`);
        this._feedback(false);
      }
      this.cartasViradas = [];
      this.bloqueado = false;
    }, delay);
  }

  _getNomePar(id) {
    return this.pares.find(p => p.id == id)?.nome || 'Profissão';
  }

  _atualizarPlacar() {
    const el = this.container.querySelector('#placarPares');
    if (el) el.textContent = this.paresEncontrados;
  }

  _feedback(acertou) {
    const feedbacks_ok = [
      '🌟 Muito bem! Par encontrado!',
      '✨ Incrível! Você é ótimo!',
      '🎉 Parabéns! Continue assim!',
      '💪 Show! Você conseguiu!',
      '🏆 Excelente! Que memória!'
    ];
    const feedbacks_tente = [
      '🤔 Quase! Continue tentando!',
      '💙 Não desista! Você vai conseguir!',
      '🌈 Continue! Você está indo bem!',
    ];
    const el = this.container.querySelector('#feedbackMemoria');
    if (!el) return;
    const msg = acertou
      ? feedbacks_ok[Math.floor(Math.random() * feedbacks_ok.length)]
      : feedbacks_tente[Math.floor(Math.random() * feedbacks_tente.length)];
    el.textContent = msg;
    el.className = `jogo-feedback ${acertou ? 'positivo' : 'neutro'}`;
  }

  _vitoria() {
    const el = this.container.querySelector('#feedbackMemoria');
    if (el) {
      el.textContent = '🏆 PARABÉNS! Você encontrou todos os pares! Você é incrível! 🌟';
      el.className = 'jogo-feedback positivo';
    }
  }
}

/* ═══ QUIZ EDUCATIVO ═══ */
class QuizEducativo {
  constructor(container) {
    this.container = container;
    this.perguntas = [
      { texto: 'Que animal faz "miau"?', emoji: '🐱', opcoes: ['Cachorro 🐶','Gato 🐱','Peixe 🐟'], correta: 1 },
      { texto: 'Que cor é o céu?', emoji: '☁️', opcoes: ['Verde 🟢','Vermelho 🔴','Azul 🔵'], correta: 2 },
      { texto: 'Qual fruta é amarela?', emoji: '🍌', opcoes: ['Banana 🍌','Maçã 🍎','Morango 🍓'], correta: 0 },
      { texto: 'Quem cuida dos nossos dentes?', emoji: '🦷', opcoes: ['Dentista 🦷','Bombeiro 🚒','Chef 👨‍🍳'], correta: 0 },
      { texto: 'Que forma tem uma bola?', emoji: '⚽', opcoes: ['Quadrado ⬜','Triângulo 🔺','Círculo ⭕'], correta: 2 },
      { texto: 'Qual animal tem tromba?', emoji: '🐘', opcoes: ['Leão 🦁','Elefante 🐘','Girafa 🦒'], correta: 1 },
      { texto: 'Que cor é a grama?', emoji: '🌿', opcoes: ['Azul 💙','Verde 💚','Rosa 🩷'], correta: 1 },
      { texto: 'Quem apaga incêndios?', emoji: '🚒', opcoes: ['Bombeiro 🚒','Médico 👨‍⚕️','Professor 👩‍🏫'], correta: 0 },
      { texto: 'Quantos lados tem um triângulo?', emoji: '🔺', opcoes: ['2 lados','3 lados','4 lados'], correta: 1 },
      { texto: 'Qual animal bota ovos?', emoji: '🐔', opcoes: ['Cachorro 🐶','Gato 🐱','Galinha 🐔'], correta: 2 },
    ];
    this.atual = 0;
    this.acertos = 0;
    this.bloqueado = false;
    this.render();
  }

  render() {
    this.container.setAttribute('role', 'game');
    this.container.setAttribute('aria-label', 'Quiz Educativo');
    this._renderPergunta();
  }

  _renderPergunta() {
    const total = this.perguntas.length;
    const p = this.perguntas[this.atual];
    const pct = Math.round((this.atual / total) * 100);

    this.container.innerHTML = `
      <div class="jogo-header">
        <h3 class="jogo-titulo">🧠 Quiz Educativo</h3>
        <div class="jogo-placar" aria-live="polite">
          <span>Pergunta <span class="jogo-placar-valor">${this.atual + 1}</span> de ${total}</span>
        </div>
      </div>
      <div class="quiz-progress" role="progressbar" aria-valuenow="${pct}" aria-valuemin="0" aria-valuemax="100" aria-label="Progresso do quiz">
        <div class="quiz-progress-fill" style="width:${pct}%"></div>
      </div>
      <div class="quiz-pergunta">
        <div class="quiz-imagem" aria-hidden="true" style="font-size:80px;background:var(--bg-inclusao);border-radius:var(--radius);">${p.emoji}</div>
        <p class="quiz-texto" id="quizPergunta">${p.texto}</p>
      </div>
      <div class="quiz-opcoes" role="listbox" aria-labelledby="quizPergunta">
        ${p.opcoes.map((op, i) => `
          <button class="quiz-opcao" data-index="${i}" tabindex="0"
            role="option" aria-selected="false">${op}</button>`).join('')}
      </div>
      <div class="jogo-feedback neutro" id="quizFeedback" aria-live="polite" aria-atomic="true" style="margin-top:16px;"></div>`;

    this._bindOpcoes();
    this.bloqueado = false;
  }

  _bindOpcoes() {
    this.container.querySelectorAll('.quiz-opcao').forEach(btn => {
      btn.addEventListener('click', () => this._responder(parseInt(btn.dataset.index)));
      btn.addEventListener('keydown', e => {
        if (e.key === 'Enter' || e.key === ' ') {
          e.preventDefault();
          this._responder(parseInt(btn.dataset.index));
        }
      });
    });
  }

  _responder(idx) {
    if (this.bloqueado) return;
    this.bloqueado = true;

    const p = this.perguntas[this.atual];
    const botoes = this.container.querySelectorAll('.quiz-opcao');
    const feedback = this.container.querySelector('#quizFeedback');

    botoes.forEach(b => {
      b.disabled = true;
      b.setAttribute('aria-selected', 'false');
    });
    botoes[idx].setAttribute('aria-selected', 'true');

    const acertou = idx === p.correta;
    if (acertou) {
      this.acertos++;
      botoes[idx].classList.add('correta');
      feedback.textContent = '🌟 Muito bem! Resposta correta!';
      feedback.className = 'jogo-feedback positivo';
    } else {
      botoes[idx].style.opacity = '0.5';
      botoes[p.correta].classList.add('correta');
      feedback.textContent = `💙 Quase! A resposta certa era: ${p.opcoes[p.correta]}`;
      feedback.className = 'jogo-feedback neutro';
    }

    setTimeout(() => {
      this.atual++;
      if (this.atual < this.perguntas.length) this._renderPergunta();
      else this._resultado();
    }, acertou ? 1200 : 1800);
  }

  _resultado() {
    const msgs = [
      '🌟 Você é incrível! Continue aprendendo!',
      '🎉 Excelente! Você arrasou no quiz!',
      '💪 Muito bem! Você é muito inteligente!',
      '🏆 Parabéns! Você é um campeão do saber!'
    ];
    const msg = msgs[Math.floor(Math.random() * msgs.length)];
    this.container.innerHTML = `
      <div class="quiz-resultado">
        <div class="quiz-resultado-emoji">🎊</div>
        <h3 class="quiz-resultado-titulo">${msg}</h3>
        <p class="quiz-resultado-msg">Você acertou <strong>${this.acertos}</strong> de <strong>${this.perguntas.length}</strong> perguntas!</p>
        <button class="btn btn-primary" id="btnJogarNovamente">🔄 Jogar novamente</button>
      </div>`;
    this.container.querySelector('#btnJogarNovamente').addEventListener('click', () => {
      this.atual = 0; this.acertos = 0;
      this.perguntas.sort(() => Math.random() - 0.5);
      this.render();
    });
  }
}

/* ═══ JOGO SEQUÊNCIA ═══ */
class JogoSequencia {
  constructor(container) {
    this.container = container;
    this.nivel = 1;
    this.bloqueado = false;
    this.formas = ['⭕','⬜','🔺','🟡','💜','🟢'];
    this.render();
  }

  _gerarSequencia() {
    const tam = 3 + this.nivel;
    const base = [];
    for (let i = 0; i < tam; i++) base.push(this.formas[i % this.formas.length]);
    return base;
  }

  render() {
    const seq = this._gerarSequencia();
    const correta = seq[seq.length - 1];
    const exibida = seq.slice(0, -1);

    // Opções: correta + 2 aleatórias diferentes
    let opcoes = [correta];
    const outras = this.formas.filter(f => f !== correta);
    outras.sort(() => Math.random() - 0.5);
    opcoes = [...opcoes, ...outras.slice(0, 2)].sort(() => Math.random() - 0.5);

    this.container.setAttribute('role', 'game');
    this.container.setAttribute('aria-label', 'Jogo de Sequências');

    this.container.innerHTML = `
      <div class="jogo-header">
        <h3 class="jogo-titulo">🔢 Jogo de Sequências</h3>
        <div class="jogo-placar"><span>Nível <span class="jogo-placar-valor">${this.nivel}</span></span></div>
      </div>
      <div class="jogo-feedback neutro" id="feedbackSeq" aria-live="polite" aria-atomic="true">
        Qual forma vem a seguir?
      </div>
      <div class="sequencia-container">
        <div class="sequencia-display" aria-label="Sequência de formas" role="list">
          ${exibida.map((f,i) => `<div class="seq-item" role="listitem" aria-label="Forma ${i+1}: ${f}" style="background:var(--bg-primary);font-size:36px;display:flex;align-items:center;justify-content:center;">${f}</div>`).join('')}
          <div class="seq-item interrogacao" aria-label="Próxima forma — a descobrir">❓</div>
        </div>
        <div class="sequencia-opcoes" role="listbox" aria-label="Escolha a próxima forma">
          ${opcoes.map(op => `
            <button class="seq-opcao" data-forma="${op}"
              role="option" aria-selected="false"
              aria-label="Forma ${op}" tabindex="0">${op}</button>`).join('')}
        </div>
      </div>
      <div class="jogo-controles">
        <button class="btn btn-secondary btn-sm" id="btnReiniciarSeq" aria-label="Reiniciar sequências">
          🔄 Reiniciar
        </button>
      </div>`;

    this.container.querySelectorAll('.seq-opcao').forEach(btn => {
      btn.addEventListener('click', () => this._responder(btn.dataset.forma, correta));
      btn.addEventListener('keydown', e => {
        if (e.key === 'Enter' || e.key === ' ') {
          e.preventDefault();
          this._responder(btn.dataset.forma, correta);
        }
      });
    });
    this.container.querySelector('#btnReiniciarSeq').addEventListener('click', () => {
      this.nivel = 1;
      this.render();
    });
    this.bloqueado = false;
  }

  _responder(escolha, correta) {
    if (this.bloqueado) return;
    this.bloqueado = true;

    const feedback = this.container.querySelector('#feedbackSeq');
    const botoes = this.container.querySelectorAll('.seq-opcao');

    botoes.forEach(b => { b.disabled = true; b.setAttribute('aria-selected', 'false'); });

    if (escolha === correta) {
      this.container.querySelector(`[data-forma="${escolha}"]`).classList.add('correta');
      const msgs = ['🌟 Perfeito! Você acertou!','🎉 Incrível! Continue!','✨ Muito bem! Você é esperto!','💪 Show! Sequência correta!'];
      feedback.textContent = msgs[Math.floor(Math.random() * msgs.length)];
      feedback.className = 'jogo-feedback positivo';
      setTimeout(() => {
        if (this.nivel < 5) this.nivel++;
        this.render();
      }, 1500);
    } else {
      this.container.querySelector(`[data-forma="${correta}"]`).classList.add('correta');
      feedback.textContent = `💙 Quase! A resposta era ${correta}. Continue tentando!`;
      feedback.className = 'jogo-feedback neutro';
      setTimeout(() => this.render(), 1800);
    }
  }
}

/* ── Inicializar jogos via tabs ── */
function iniciarJogosEscola(container) {
  const tabs  = container.querySelectorAll('.jogo-tab');
  const panels = container.querySelectorAll('.jogo-panel');

  let instancias = {};

  function ativarJogo(nome) {
    panels.forEach(p => p.classList.remove('active'));
    tabs.forEach(t => {
      t.classList.toggle('active', t.dataset.jogo === nome);
      t.setAttribute('aria-selected', t.dataset.jogo === nome);
    });
    const panel = container.querySelector(`#jogo-${nome}`);
    if (panel) {
      panel.classList.add('active');
      if (!instancias[nome]) {
        const el = panel.querySelector('.jogo-container');
        if (nome === 'memoria')   instancias.memoria  = new JogoMemoria(el);
        if (nome === 'quiz')      instancias.quiz     = new QuizEducativo(el);
        if (nome === 'sequencia') instancias.sequencia = new JogoSequencia(el);
      }
    }
  }

  tabs.forEach(tab => {
    tab.addEventListener('click', () => ativarJogo(tab.dataset.jogo));
    tab.addEventListener('keydown', e => {
      if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); ativarJogo(tab.dataset.jogo); }
    });
  });

  // Ativar primeiro jogo automaticamente
  if (tabs.length) ativarJogo(tabs[0].dataset.jogo);
}
