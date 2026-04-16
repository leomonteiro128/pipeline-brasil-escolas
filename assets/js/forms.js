/* ============================================================
   Brasil Escolas — Validação e envio de formulários
   ============================================================ */

/* ── Máscaras ── */
function aplicarMascaraTelefone(input) {
  input.addEventListener('input', () => {
    let v = input.value.replace(/\D/g, '').slice(0, 11);
    if (v.length >= 7) {
      v = v.length === 11
        ? `(${v.slice(0,2)}) ${v.slice(2,7)}-${v.slice(7)}`
        : `(${v.slice(0,2)}) ${v.slice(2,6)}-${v.slice(6)}`;
    } else if (v.length >= 3) {
      v = `(${v.slice(0,2)}) ${v.slice(2)}`;
    } else if (v.length >= 1) {
      v = `(${v}`;
    }
    input.value = v;
  });
}

function aplicarMascaraCEP(input) {
  input.addEventListener('input', () => {
    let v = input.value.replace(/\D/g, '').slice(0, 8);
    if (v.length > 5) v = `${v.slice(0,5)}-${v.slice(5)}`;
    input.value = v;
  });
}

function aplicarMascaraCNPJ(input) {
  input.addEventListener('input', () => {
    let v = input.value.replace(/\D/g, '').slice(0, 14);
    if (v.length > 12) v = `${v.slice(0,2)}.${v.slice(2,5)}.${v.slice(5,8)}/${v.slice(8,12)}-${v.slice(12)}`;
    else if (v.length > 8) v = `${v.slice(0,2)}.${v.slice(2,5)}.${v.slice(5,8)}/${v.slice(8)}`;
    else if (v.length > 5) v = `${v.slice(0,2)}.${v.slice(2,5)}.${v.slice(5)}`;
    else if (v.length > 2) v = `${v.slice(0,2)}.${v.slice(2)}`;
    input.value = v;
  });
}

/* ── Validação CNPJ ── */
function validarCNPJ(cnpj) {
  cnpj = cnpj.replace(/\D/g, '');
  if (cnpj.length !== 14 || /^(\d)\1+$/.test(cnpj)) return false;
  const calc = (str, pesos) => {
    let s = 0;
    for (let i = 0; i < pesos.length; i++) s += parseInt(str[i]) * pesos[i];
    const r = s % 11;
    return r < 2 ? 0 : 11 - r;
  };
  const d1 = calc(cnpj, [5,4,3,2,9,8,7,6,5,4,3,2]);
  const d2 = calc(cnpj, [6,5,4,3,2,9,8,7,6,5,4,3,2]);
  return parseInt(cnpj[12]) === d1 && parseInt(cnpj[13]) === d2;
}

/* ── Autocomplete CEP ── */
async function autocompleteCEP(cepInput, campos) {
  const cep = cepInput.value.replace(/\D/g, '');
  if (cep.length !== 8) return;
  try {
    const r = await fetch(`https://viacep.com.br/ws/${cep}/json/`);
    const d = await r.json();
    if (d.erro) { mostrarFeedbackCampo(cepInput, false, 'CEP não encontrado'); return; }
    if (campos.rua   && d.logradouro) campos.rua.value   = d.logradouro;
    if (campos.bairro && d.bairro)   campos.bairro.value  = d.bairro;
    if (campos.cidade && d.localidade) campos.cidade.value = d.localidade;
    if (campos.uf    && d.uf)        campos.uf.value      = d.uf;
    mostrarFeedbackCampo(cepInput, true, 'CEP encontrado');
    if (campos.numero) campos.numero.focus();
  } catch {
    mostrarFeedbackCampo(cepInput, false, 'Erro ao buscar CEP');
  }
}

/* ── Feedback visual de campo ── */
function mostrarFeedbackCampo(input, valido, msg = '') {
  input.classList.toggle('is-valid', valido);
  input.classList.toggle('is-invalid', !valido);
  let fb = input.parentElement.querySelector('.form-feedback');
  if (!fb) {
    fb = document.createElement('span');
    fb.className = 'form-feedback';
    input.parentElement.appendChild(fb);
  }
  fb.textContent = msg;
  fb.className = `form-feedback ${valido ? 'valid' : 'invalid'}`;
}

/* ── Preview de imagem ── */
function iniciarUploadPreview(inputEl, previewEl, maxKB = 150) {
  inputEl.addEventListener('change', () => {
    previewEl.innerHTML = '';
    Array.from(inputEl.files).forEach(file => {
      if (file.size > maxKB * 1024) {
        alert(`A imagem "${file.name}" ultrapassa ${maxKB}KB. Por favor, reduza o tamanho.`);
        return;
      }
      const reader = new FileReader();
      reader.onload = e => {
        const div = document.createElement('div');
        div.className = 'preview-img';
        div.innerHTML = `<img src="${e.target.result}" alt="Preview">
          <button class="preview-remove" type="button" aria-label="Remover imagem">✕</button>`;
        div.querySelector('.preview-remove').addEventListener('click', () => div.remove());
        previewEl.appendChild(div);
      };
      reader.readAsDataURL(file);
    });
  });
}

/* ── Contador de palavras ── */
function iniciarContadorPalavras(textarea, counterEl, minPalavras = 300) {
  function atualizar() {
    const count = contarPalavras(textarea.value);
    counterEl.textContent = `${count} / ${minPalavras} palavras mínimas`;
    counterEl.className = `word-counter ${count >= minPalavras ? 'ok' : 'insufficient'}`;
  }
  textarea.addEventListener('input', atualizar);
  atualizar();
}

/* ── Stepper ── */
class Stepper {
  constructor(containerEl) {
    this.container = containerEl;
    this.panels    = Array.from(containerEl.querySelectorAll('.step-panel'));
    this.steps     = Array.from(containerEl.querySelectorAll('.step'));
    this.progressFill = containerEl.querySelector('.progress-bar-fill');
    this.progressLabel = containerEl.querySelector('.progress-label');
    this.atual = 0;
    this._atualizar();
  }

  _atualizar() {
    const total = this.panels.length;
    this.panels.forEach((p, i) => p.classList.toggle('active', i === this.atual));
    this.steps.forEach((s, i) => {
      s.classList.toggle('active', i === this.atual);
      s.classList.toggle('done', i < this.atual);
    });
    const pct = Math.round(((this.atual + 1) / total) * 100);
    if (this.progressFill) this.progressFill.style.width = `${pct}%`;
    if (this.progressLabel) this.progressLabel.textContent = `Etapa ${this.atual + 1} de ${total}`;
    window.scrollTo({ top: this.container.offsetTop - 100, behavior: 'smooth' });
  }

  avancar() {
    if (this.atual < this.panels.length - 1) {
      this.atual++;
      this._atualizar();
    }
  }
  voltar() {
    if (this.atual > 0) {
      this.atual--;
      this._atualizar();
    }
  }
  get etapa() { return this.atual; }
}

/* ── Envio de cadastro de escola ── */
async function enviarCadastroEscola(dados, btnEl) {
  btnEl.disabled = true;
  const originalText = btnEl.innerHTML;
  btnEl.innerHTML = '<span class="spinner"></span> Enviando...';

  try {
    await inserirCadastroManual(dados);

    // Notificação EmailJS (se configurado)
    if (typeof emailjs !== 'undefined') {
      emailjs.send('service_brasilescolas', 'template_cadastro', {
        escola_nome: dados.nome,
        municipio: `${dados.municipio}/${dados.uf}`,
        para: 'contatoibetp@gmail.com',
      }).catch(() => {});
    }

    return { sucesso: true };
  } catch (err) {
    return { sucesso: false, mensagem: err.message };
  } finally {
    btnEl.disabled = false;
    btnEl.innerHTML = originalText;
  }
}

/* ── Envio de vaga ── */
async function enviarVaga(dados, btnEl) {
  btnEl.disabled = true;
  const originalText = btnEl.innerHTML;
  btnEl.innerHTML = '<span class="spinner"></span> Enviando...';
  try {
    await inserirVaga(dados);
    return { sucesso: true };
  } catch (err) {
    return { sucesso: false, mensagem: err.message };
  } finally {
    btnEl.disabled = false;
    btnEl.innerHTML = originalText;
  }
}
