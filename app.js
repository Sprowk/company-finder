const App = {
    config: {
      owner: "sprowk",
      repo: "company-finder",
      branch: "main",
      pageSize: 100
    },
    state: {
      groupedFiles: {},
      header: [],
      rowsByCat: {},
      counts: {},
      currentCategory: "all",
      currentPage: 1,
      currentDate: null,
      rawRows: [],
      cityFilter: "",
      regionFilter: "",
      regionOptions: [],
      lastFilteredCount: 0,
      isLoadingParts: false
    },
    elements: {},
  
    init() {
      this.cacheElements();
    this._debouncedRender = this.utils.debounce(() => this.render(), 500);
      this.addEventListeners();
      this.loadSnapshots();
    },
  
    cacheElements() {
      this.elements = {
        statusContainer: document.getElementById('status-container'),
        headerStatus: document.getElementById('header-status'),
        dateSelect: document.getElementById('date-select'),
        lastUpdatedBox: document.getElementById('last-updated-box'),
        downloadBtn: document.getElementById('download-btn'),
        statsGrid: document.getElementById('stats-grid'),
        tableCard: document.getElementById('table-card'),
        tableWrapper: document.getElementById('table-wrapper'),
        pagination: document.getElementById('pagination'),
        filterButtons: document.querySelectorAll('#filter-control-container button'),
        filterControlContainer: document.getElementById('filter-control-container'),
        geoFilterGroup: document.getElementById('geo-filter-group'),
        cityFilterInput: document.getElementById('city-filter-input'),
        regionFilterSelect: document.getElementById('region-filter-select'),
      };
    },
  
    addEventListeners() {
      this.elements.filterButtons.forEach(btn => {
        btn.addEventListener('click', () => {
          const prevScroll = this.elements.tableWrapper ? this.elements.tableWrapper.scrollTop : 0;
          this.state.currentCategory = btn.dataset.cat;
          this.state.currentPage = 1;
          this.render();
          if (this.elements.tableWrapper) {
            this.elements.tableWrapper.scrollTop = prevScroll; // restore so smooth animates from previous position
          }
          requestAnimationFrame(() => {
            requestAnimationFrame(() => {
              if (this.elements.tableWrapper) {
                this.utils.scrollElementToTopSmooth(this.elements.tableWrapper);
              }
              this.utils.scrollWindowToTopSmooth();
            });
          });
        });
      });
  
      if (this.elements.dateSelect) {
        this.elements.dateSelect.addEventListener('change', (e) => this.loadDate(e.target.value));
      }
  
      this.elements.pagination.addEventListener('click', e => {
        const target = e.target.closest('button[data-page]');
        if (target) {
          const page = parseInt(target.dataset.page, 10);
          if (page !== this.state.currentPage) {
            const prevScroll = this.elements.tableWrapper ? this.elements.tableWrapper.scrollTop : 0;
            this.state.currentPage = page;
            this.render();
            if (this.elements.tableWrapper) {
              this.elements.tableWrapper.scrollTop = prevScroll; // restore so smooth animates from previous position
            }
            requestAnimationFrame(() => {
              requestAnimationFrame(() => {
                if (this.elements.tableWrapper) {
                  this.utils.scrollElementToTopSmooth(this.elements.tableWrapper);
                }
                this.utils.scrollWindowToTopSmooth();
              });
            });
          }
        }
      });
  
      if (this.elements.cityFilterInput) {
        this.elements.cityFilterInput.addEventListener('input', (e) => {
          this.state.cityFilter = e.target.value || "";
          this.state.currentPage = 1;
          this._debouncedRender();
        });
      }
  
      if (this.elements.regionFilterSelect) {
        this.elements.regionFilterSelect.addEventListener('change', (e) => {
          const prevScroll = this.elements.tableWrapper ? this.elements.tableWrapper.scrollTop : 0;
          this.state.regionFilter = e.target.value || "";
          this.state.currentPage = 1;
          if (document.activeElement === this.elements.regionFilterSelect) {
            this.elements.regionFilterSelect.blur();
          }
          this.render();
          if (this.elements.tableWrapper) {
            this.elements.tableWrapper.scrollTop = prevScroll;
          }
          requestAnimationFrame(() => {
            requestAnimationFrame(() => {
              if (this.elements.tableWrapper) {
                this.utils.scrollElementToTopSmooth(this.elements.tableWrapper);
              }
              this.utils.scrollWindowToTopSmooth();
            });
          });
        });
      }
    },
  
    utils: {
      baseStaticUrl: () =>
        `${window.location.origin}${window.location.pathname.replace(/\/$/, "")}/snapshots/`,
      
      // Práca s DMY formátom, podporuje oddeľovač '-' aj '.'
      parseDmyToDate(dmy) {
        const m = (dmy || "").match(/^(\d{2})[.-](\d{2})[.-](\d{4})$/);
        if (!m) return null;
        const dd = Number(m[1]);
        const mm = Number(m[2]);
        const yyyy = Number(m[3]);
        // Použijeme Date.UTC pre deterministické porovnávanie
        return new Date(Date.UTC(yyyy, mm - 1, dd));
      },
      formatDmyForDisplay(dmy) {
        const m = (dmy || "").match(/^(\d{2})-(\d{2})-(\d{4})$/);
        if (!m) return dmy || "";
        return `${m[1]}.${m[2]}.${m[3]}`;
      },
  
      classifyRow(row) {
        const norm = (row["source_register"] || "")
          .normalize('NFD')
          .replace(/[\u0300-\u036f]/g, '')
          .toLowerCase();
        if (norm.includes("obchodny")) return "orsr";
        if (norm.includes("zivnost")) return "zrsr";
        return "other";
      },
  
      formatNumber: num => num.toLocaleString('sk-SK'),
  
      getPaginationModel(currentPage, totalPages) {
        const delta = 1, range = [];
        for (let i = Math.max(2, currentPage - delta); i <= Math.min(totalPages - 1, currentPage + delta); i++) {
          range.push(i);
        }
        if (currentPage - delta > 2) range.unshift("...");
        if (currentPage + delta < totalPages - 1) range.push("...");
        range.unshift(1);
        if (totalPages > 1) range.push(totalPages);
        return range;
      },
  
      normalizeText(t) {
        return (t || "")
          .toString()
          .normalize('NFD')
          .replace(/[\u0300-\u036f]/g, '')
          .toLowerCase();
      },

      debounce(fn, wait) {
        let timeoutId;
        return function(...args) {
          clearTimeout(timeoutId);
          timeoutId = setTimeout(() => fn.apply(this, args), wait);
        };
      },

      // Safe smooth scroll helpers with wide browser fallback
      scrollElementToTopSmooth(el) {
        if (!el) return;
        try {
          el.scrollTo({ top: 0, behavior: 'smooth' });
        } catch (err) {
          try {
            el.scrollTo(0, 0);
          } catch (err2) {
            el.scrollTop = 0;
          }
        }
      },
      scrollWindowToTopSmooth() {
        try {
          window.scrollTo({ top: 0, behavior: 'smooth' });
        } catch (err) {
          try {
            window.scrollTo(0, 0);
          } catch (err2) {
            document.documentElement.scrollTop = 0;
            document.body.scrollTop = 0;
          }
        }
      },
      
    },
  
  async loadSnapshots() {
      this.renderPlaceholder('Načítavam dostupné snapshoty...');
      try {
        const res = await fetch(
          `https://api.github.com/repos/${this.config.owner}/${this.config.repo}/contents/snapshots?ref=${this.config.branch}`
        );
        if (!res.ok) throw new Error(`GitHub API Error: ${res.statusText}`);
        const files = await res.json();

        // Získaj všetky CSV part súbory (bez skupinovania podľa dátumu)
        const partFiles = files
          .filter(f => f.name.endsWith('.csv.gz'))
          .map(f => ({ name: f.name }))
          .sort((a, b) => {
            const ma = a.name.match(/part(\d+)\.csv\.gz$/);
            const mb = b.name.match(/part(\d+)\.csv\.gz$/);
            if (ma && mb) return Number(ma[1]) - Number(mb[1]);
            return a.name.localeCompare(b.name, 'sk-SK');
          });

        if (!partFiles.length) throw new Error('Nenašli sa žiadne snapshoty.');

        // Načítaj last_updated.txt a nastav referenčný dátum + badge
        try {
          const duRes = await fetch(this.utils.baseStaticUrl() + 'last_updated.txt');
          if (duRes.ok) {
            const txt = (await duRes.text()).trim();
            this.state.currentDate = txt || null;
            if (this.elements.lastUpdatedBox) {
              const display = this.utils.formatDmyForDisplay(txt);
              this.elements.lastUpdatedBox.textContent = display || '—';
            }
          } else {
            if (this.elements.lastUpdatedBox) this.elements.lastUpdatedBox.textContent = '—';
          }
        } catch (e) {
          if (this.elements.lastUpdatedBox) this.elements.lastUpdatedBox.textContent = '—';
        }

        // Reset stav a načítaj dáta
        this.state.header = [];
        this.state.rowsByCat = {};
        this.state.counts = {};
        this.state.currentCategory = 'all';
        this.state.currentPage = 1;
        this.state.cityFilter = "";
        this.state.regionFilter = "";
        this.state.regionOptions = [];
        this.state.lastFilteredCount = 0;
        if (this.elements.cityFilterInput) this.elements.cityFilterInput.value = "";
        if (this.elements.regionFilterSelect) this.elements.regionFilterSelect.value = "";

        this.loadPartsIncrementally(partFiles);
      } catch (err) {
        this.renderStatus(`Chyba pri načítaní: ${err.message}`);
      }
    },
    
  
    async loadPartsIncrementally(files) {
      this.renderPlaceholder(`Načítavam dáta – časť 1 z ${files.length}...`);
    this.elements.filterControlContainer.style.display = 'none';
    if (this.elements.downloadBtn) {
      this.elements.downloadBtn.style.display = 'none';
    }
      if (this.elements.geoFilterGroup) {
        this.elements.geoFilterGroup.style.display = 'none';
      }
      this.state.isLoadingParts = true;
      this.setHeaderStatus(`
        <span class="spinner"></span>
        Načítavam dáta – časť 1 z ${files.length}...
      `);
  
      for (let i = 0; i < files.length; i++) {
        const f = files[i];
        const url = this.utils.baseStaticUrl() + f.name;
  
        if (i > 0) {
          this.setHeaderStatus(`
            <span class="spinner"></span>
            Načítavam dáta – časť ${i + 1} z ${files.length}...
          `);
        }
  
        try {
          const partRows = await this.loadAndParseFile(url, f.name);
  
          if (i === 0) {
            this.initDataFromPart(partRows);
            this.clearStatus();
            this.render();
            this.elements.filterControlContainer.style.display = 'inline-flex';
            if (this.elements.downloadBtn) {
              this.elements.downloadBtn.style.display = 'inline-flex';
            }
            if (this.elements.geoFilterGroup) {
              this.elements.geoFilterGroup.style.display = 'inline-flex';
            }
          } else {
            this.appendDataFromPart(partRows);
            this.renderDuringLoad();  // počas načítavania updatni len štatistiky a stránkovanie
            // Umožniť prehliadaču dýchnuť medzi veľkými dávkami
            await new Promise(requestAnimationFrame);
          }
        } catch (err) {
          this.renderStatus(`Chyba pri načítaní časti ${i + 1} z ${files.length}: ${err.message}`);
          return;
        }
      }
  
      this.clearInlineStatus();
      this.state.isLoadingParts = false;
      // po dočítaní všetkých častí doplňme výber krajov naraz (menej lagov)
      this.updateRegionFilterOptions();
      this.clearHeaderStatus();
    },
  
    async loadAndParseFile(url, filename) {
      const res = await fetch(url);
      if (!res.ok) throw new Error(`Chyba pri sťahovaní (HTTP ${res.status})`);
  
      const arrayBuffer = await res.arrayBuffer();
      const csvText = pako.ungzip(new Uint8Array(arrayBuffer), { to: "string" });
  
      return await new Promise((resolve, reject) => {
        Papa.parse(csvText, {
          header: true,
          skipEmptyLines: true,
          worker: true,
          complete: results => {
            if (!results.data.length || !results.meta.fields) {
              reject(new Error("Súbor je prázdny alebo neplatný."));
              return;
            }
            if (!this.state.header.length) {
              this.state.header = results.meta.fields;
            }
            resolve(results.data);
          },
          error: err => reject(err)
        });
      });
    },
  
    // prvá časť – inicializácia
    initDataFromPart(rows) {
      const all = [];
      const orsr = [];
      const zrsr = [];
      const other = [];
      for (let i = 0; i < rows.length; i++) {
        const r = rows[i];
        r.__cat = this.utils.classifyRow(r);
        all.push(r);
        if (r.__cat === 'orsr') orsr.push(r);
        else if (r.__cat === 'zrsr') zrsr.push(r);
        else other.push(r);
      }

      this.state.rowsByCat = { all, orsr, zrsr, other };
  
      this.state.counts = Object.keys(this.state.rowsByCat).reduce(
        (acc, key) => ({ ...acc, [key]: this.state.rowsByCat[key].length }),
        {}
      );
  
      const regionSet = new Set();
      all.forEach(r => {
        const reg = (r.region || "").trim();
        if (reg) regionSet.add(reg);
      });
      this.state.regionOptions = Array.from(regionSet).sort((a, b) => a.localeCompare(b, 'sk-SK'));
      this.updateRegionFilterOptions();
  
      this.ensureCurrentPageInRange();
    },
  
    // ďalšie časti – len append (bez re-sortu)
    appendDataFromPart(rows) {
      if (!this.state.rowsByCat.all) {
        this.initDataFromPart(rows);
        return;
      }

      for (let i = 0; i < rows.length; i++) {
        const r = rows[i];
        r.__cat = this.utils.classifyRow(r);
        this.state.rowsByCat.all.push(r);
        if (r.__cat === 'orsr') this.state.rowsByCat.orsr.push(r);
        else if (r.__cat === 'zrsr') this.state.rowsByCat.zrsr.push(r);
        else this.state.rowsByCat.other.push(r);
      }
  
      this.state.counts.all = this.state.rowsByCat.all.length;
      this.state.counts.orsr = this.state.rowsByCat.orsr.length;
      this.state.counts.zrsr = this.state.rowsByCat.zrsr.length;
      this.state.counts.other = this.state.rowsByCat.other.length;
  
      for (let i = 0; i < rows.length; i++) {
        const r = rows[i];
        const reg = (r.region || "").trim();
        if (!reg) continue;
        if (!this.state.regionOptions.includes(reg)) {
          this.state.regionOptions.push(reg);
        }
      }
      if (!this.state.isLoadingParts) {
        this.state.regionOptions.sort((a, b) => a.localeCompare(b, 'sk-SK'));
        this.updateRegionFilterOptions();
      }
  
      this.ensureCurrentPageInRange();
    },
  
    updateRegionFilterOptions() {
      const select = this.elements.regionFilterSelect;
      if (!select) return;
  
      const options = this.state.regionOptions;
      const current = this.state.regionFilter || "";
  
      select.innerHTML =
        `<option value="">Všetky kraje</option>` +
        options.map(r => `<option value="${r}">${r}</option>`).join('');
  
      if (current && options.includes(current)) {
        select.value = current;
      } else {
        select.value = "";
        this.state.regionFilter = "";
      }
    },
  
    ensureCurrentPageInRange() {
      const list = this.state.rowsByCat[this.state.currentCategory] || [];
      const total = list.length;
      const maxPage = Math.max(1, Math.ceil(total / this.config.pageSize));
      if (this.state.currentPage > maxPage) {
        this.state.currentPage = maxPage;
      }
    },
  
    render() {
      this.elements.filterButtons.forEach(b =>
        b.classList.toggle('active', b.dataset.cat === this.state.currentCategory)
      );
      if (!this.state.counts.all) {
        this.renderPlaceholder("Žiadne dáta na zobrazenie.");
        return;
      }
      this.renderStats();
      this.renderTable();
      this.renderPagination();
    },

    // Počas načítavania ďalších častí updatujeme len štatistiky a stránkovanie,
    // aby sa minimalizoval lag v UI (tlačidlá, selecty, tabuľka ostávajú nedotknuté).
    renderDuringLoad() {
      this.renderStats();
      this.state.lastFilteredCount = this.getFilteredLength();
      this.renderPagination();
    },

    // Rýchly prepočet počtu záznamov po aplikovaní filtrov bez renderovania tabuľky
    getFilteredLength() {
      const list = this.state.rowsByCat[this.state.currentCategory] || [];
      const cityFilterNorm = this.utils.normalizeText(this.state.cityFilter);
      const regionFilter = (this.state.regionFilter || "").trim();
      if (!cityFilterNorm && !regionFilter) return list.length;
      let count = 0;
      for (let i = 0; i < list.length; i++) {
        const row = list[i];
        if (cityFilterNorm) {
          const cityNorm = this.utils.normalizeText(row.city);
          if (!cityNorm.includes(cityFilterNorm)) continue;
        }
        if (regionFilter) {
          if ((row.region || "") !== regionFilter) continue;
        }
        count++;
      }
      return count;
    },
  
    renderStats() {
      const { all = 0, orsr = 0, zrsr = 0, other = 0 } = this.state.counts;
      const stats = [
        { title: 'Celkovo subjektov', value: all },
        { title: 'Firmy', value: orsr },
        { title: 'Živnosti', value: zrsr },
        { title: 'Ostatné', value: other },
      ];
      this.elements.statsGrid.innerHTML = stats
        .map(
          s => `
            <div class="stat-card">
              <p class="title">${s.title}</p>
              <p class="value">${this.utils.formatNumber(s.value)}</p>
            </div>`
        )
        .join('');
    },
  
    renderTable() {
      const allData = this.state.rowsByCat[this.state.currentCategory] || [];
      if (allData.length === 0) {
        this.elements.tableWrapper.innerHTML =
          `<div class="placeholder">Žiadne záznamy pre túto kategóriu.</div>`;
        this.elements.pagination.innerHTML = '';
        this.state.lastFilteredCount = 0;
        return;
      }
  
      const cityFilterNorm = this.utils.normalizeText(this.state.cityFilter);
      const regionFilter = (this.state.regionFilter || "").trim();
  
      const filtered = allData.filter(row => {
        if (cityFilterNorm) {
          const cityNorm = this.utils.normalizeText(row.city);
          if (!cityNorm.includes(cityFilterNorm)) return false;
        }
        if (regionFilter) {
          if ((row.region || "") !== regionFilter) return false;
        }
        return true;
      });
  
      this.state.lastFilteredCount = filtered.length;
  
      if (filtered.length === 0) {
        this.elements.tableWrapper.innerHTML =
          `<div class="placeholder">Žiadne záznamy pre zvolený filter.</div>`;
        this.elements.pagination.innerHTML = '';
        return;
      }
  
      const totalPages = Math.ceil(filtered.length / this.config.pageSize);
      if (this.state.currentPage > totalPages) {
        this.state.currentPage = 1;
      }
  
      const start = (this.state.currentPage - 1) * this.config.pageSize;
      const pageRows = filtered.slice(start, start + this.config.pageSize);
  
      const cols = [
        "name",
        "ico",
        "city",
        "region",
        "established_on",
        "last_modified",
        "terminated_on",
      ].filter(c => this.state.header.includes(c));
  
      const colNames = {
        "name": "Názov",
        "ico": "IČO",
        "city": "Mesto",
        "region": "Kraj",
        "established_on": "Založené",
        "last_modified": "Posledná zmena",
        "terminated_on": "Ukončené",
      };
  
      const colWidths = {
        name: "26%",
        ico: "10%",
        city: "16%",
        region: "14%",
        established_on: "12%",
        last_modified: "12%",
        terminated_on: "12%",
      };
  
      const head = `
        <thead>
          <tr>
            ${cols
              .map(
                c => `
                  <th class="${['established_on','last_modified','terminated_on'].includes(c) ? 'date-header' : ''}"
                      style="width: ${colWidths[c] || 'auto'}">
                    ${colNames[c] || c}
                  </th>`
              )
              .join('')}
          </tr>
        </thead>`;
  
      const body = `
        <tbody>
          ${pageRows
            .map(row => {
              const establishedDate = this.utils.parseDmyToDate(row["established_on"]);
              const refDate = this.utils.parseDmyToDate(this.state.currentDate) || new Date();
              const dayMs = 24 * 60 * 60 * 1000;
              let recentClass = '';
              let diffDays = null;
              if (establishedDate) {
                const diffMs = refDate.getTime() - establishedDate.getTime();
                diffDays = Math.floor(diffMs / dayMs);
                if (diffDays >= 0 && diffDays <= 6) {
                  recentClass = `recent-founded recent-founded-${diffDays}`;
                }
              }
              const recentDotHtml = (diffDays !== null && diffDays >= 0 && diffDays <= 6)
                ? `<span class="recent-dot recent-dot-${diffDays}" aria-hidden="true"></span>`
                : "";
              return `
                <tr class="${recentClass}">
                  ${cols
                    .map(c => {
                      const val = row[c] || '—';
                      if (c === "name") {
                        return `<td class="name-cell">${recentDotHtml}${val}</td>`;
                      }
                      if (["established_on","last_modified","terminated_on"].includes(c)) {
                        return `<td class="date-cell">${val}</td>`;
                      }
                      if (c === "ico") {
                        return `<td class="ico-cell">${val}</td>`;
                      }
                      return `<td>${val}</td>`;
                    })
                    .join('')}
                </tr>`;
            })
            .join('')}
        </tbody>`;
  
      this.elements.tableWrapper.innerHTML = `<table>${head}${body}</table>`;
    },
  
    renderPagination() {
      const { currentPage } = this.state;
      const total = this.state.lastFilteredCount || 0;
      const totalPages = Math.ceil(total / this.config.pageSize);
  
      if (totalPages <= 1 || total === 0) {
        this.elements.pagination.innerHTML = '';
        return;
      }
  
      const start = (currentPage - 1) * this.config.pageSize;
  
      const info = `
        <div class="pagination-info">
          Záznamy
          <strong>${this.utils.formatNumber(start + 1)}&nbsp;–&nbsp;${this.utils.formatNumber(
            Math.min(start + this.config.pageSize, total)
          )}</strong>
          z ${this.utils.formatNumber(total)}
        </div>`;
  
      const pageNumbers = this.utils
        .getPaginationModel(currentPage, totalPages)
        .map(p =>
          p === "..."
            ? `<span class="ellipsis">...</span>`
            : `<button class="btn page-number ${p === currentPage ? 'active' : ''}" data-page="${p}">${p}</button>`
        )
        .join('');
  
      const prevButton =
        currentPage > 1
          ? `<button class="btn" data-page="${currentPage - 1}">Predchádzajúca</button>`
          : '';
      const nextButton =
        currentPage < totalPages
          ? `<button class="btn" data-page="${currentPage + 1}">Ďalšia</button>`
          : '';
  
      const controlsWrapper = `
        <div class="pagination-controls-wrapper">
          <div class="pagination-controls">
            ${prevButton}
            ${pageNumbers}
            ${nextButton}
          </div>
        </div>`;
  
      this.elements.pagination.innerHTML =
        `<div class="pagination-spacer"></div>${controlsWrapper}${info}`;
    },
  
    renderStatus(message) {
      this.elements.statusContainer.innerHTML = `<div class="alert">${message}</div>`;
      this.elements.tableCard.style.display = 'none';
    },
  
    clearStatus() {
      this.elements.statusContainer.innerHTML = "";
      this.elements.tableCard.style.display = 'flex';
    },
  
    showInlineStatus(message) {
      this.elements.statusContainer.innerHTML =
        `<div class="alert" style="font-size:0.875rem; opacity:0.8;">${message}</div>`;
      this.elements.tableCard.style.display = 'flex';
    },
  
    clearInlineStatus() {
      this.elements.statusContainer.innerHTML = "";
    },

    setHeaderStatus(message) {
      if (this.elements.headerStatus) {
        this.elements.headerStatus.innerHTML = message;
      }
    },
    clearHeaderStatus() {
      if (this.elements.headerStatus) {
        this.elements.headerStatus.innerHTML = "";
      }
    },
  
    renderPlaceholder(text) {
      this.clearStatus();
      this.elements.tableCard.style.display = 'flex';
      this.elements.pagination.innerHTML = '';
      this.elements.tableWrapper.innerHTML = `
        <div class="placeholder">
          <div class="spinner"></div>
          <p style="margin-top: 1rem;">${text}</p>
        </div>`;
    }
  };
  
  document.addEventListener('DOMContentLoaded', () => App.init());
  