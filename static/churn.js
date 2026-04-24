/* churn.js — Churn Prediction page logic
   Reads all data from the <script id="page-data" type="application/json"> block.
   No inline scripts or event handler attributes used (CSP-compliant).
*/
(function () {
  'use strict';

  var PAGE = JSON.parse(document.getElementById('page-data').textContent);

  // ── Set bar widths from data-pct ──────────────────────────────────────────
  document.querySelectorAll('.bar-fill[data-pct]').forEach(function (el) {
    el.style.width = el.getAttribute('data-pct') + '%';
  });

  // ── Refresh button ────────────────────────────────────────────────────────
  var refreshBtn = document.getElementById('refreshBtn');
  if (refreshBtn) {
    refreshBtn.addEventListener('click', function () {
      refreshBtn.textContent = 'Running...';
      refreshBtn.disabled = true;
      var body = new URLSearchParams({ csrf_token: PAGE.csrfToken });
      fetch(PAGE.refreshUrl, { method: 'POST', body: body })
        .then(function () {
          document.getElementById('content').innerHTML =
            '<div class="card full"><div class="state-center">' +
            '<div class="spinner"></div>' +
            '<p><strong>Re-running churn analysis...</strong></p>' +
            '<p>Engineering features and training model.</p>' +
            '</div></div>';
          startPolling();
        });
    });
  }

  // ── Polling ───────────────────────────────────────────────────────────────
  function startPolling() {
    var interval = setInterval(function () {
      fetch(PAGE.pollUrl)
        .then(function (r) { return r.json(); })
        .then(function (json) {
          if (json.status === 'done') {
            clearInterval(interval);
            window.location.reload();
          } else if (json.status === 'error') {
            clearInterval(interval);
            document.getElementById('content').innerHTML =
              '<div class="card full"><div class="state-center">' +
              '<p class="error">Analysis error</p>' +
              '<p>' + json.message + '</p></div></div>';
          }
        });
    }, 3000);
  }

  if (!PAGE.hasResults && !PAGE.hasError) {
    startPolling();
  }

  if (!PAGE.hasResults) { return; }

  // ── Chart helpers ─────────────────────────────────────────────────────────
  var BLUE   = 'rgba(11,99,206,0.7)';
  var BLUE_L = 'rgba(11,99,206,0.15)';
  var RED    = 'rgba(177,38,38,0.7)';
  var RED_L  = 'rgba(177,38,38,0.15)';

  function hBar(canvasId, labels, values, colorFn, title) {
    var canvas = document.getElementById(canvasId);
    if (!canvas) { return; }
    var colors = values.map(colorFn);
    new Chart(canvas, {
      type: 'bar',
      data: {
        labels: labels,
        datasets: [{ data: values, backgroundColor: colors, borderWidth: 0, borderRadius: 3 }]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        indexAxis: 'y',
        plugins: {
          legend: { display: false },
          title: { display: false }
        },
        scales: {
          x: { ticks: { color: '#5a6475', font: { size: 10 } }, grid: { color: '#eee' } },
          y: { ticks: { color: '#1c2230', font: { size: 10 } }, grid: { display: false } }
        }
      }
    });
  }

  // ── Correlation chart ─────────────────────────────────────────────────────
  (function () {
    var corrs  = PAGE.correlations.slice(0, 10);
    var labels = corrs.map(function (c) { return c.feature.replace(/_/g, ' '); });
    var values = corrs.map(function (c) { return c.correlation; });

    var wrap = document.getElementById('corrChart-wrap');
    if (!wrap) { return; }
    var canvas = document.createElement('canvas');
    canvas.id = 'corrChart';
    wrap.appendChild(canvas);

    new Chart(canvas, {
      type: 'bar',
      data: {
        labels: labels,
        datasets: [{
          data: values,
          backgroundColor: values.map(function (v) { return v >= 0 ? RED_L : BLUE_L; }),
          borderColor:     values.map(function (v) { return v >= 0 ? RED   : BLUE;   }),
          borderWidth: 1.5,
          borderRadius: 3
        }]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        indexAxis: 'y',
        plugins: { legend: { display: false } },
        scales: {
          x: {
            ticks: { color: '#5a6475', font: { size: 10 } },
            grid: { color: '#eee' }
          },
          y: { ticks: { color: '#1c2230', font: { size: 10 } }, grid: { display: false } }
        }
      }
    });
  }());

  // ── Coefficient chart ─────────────────────────────────────────────────────
  (function () {
    var coefs  = PAGE.coefficients.slice(0, 10);
    var labels = coefs.map(function (c) { return c.feature.replace(/_/g, ' '); });
    var values = coefs.map(function (c) { return c.coefficient; });

    var wrap = document.getElementById('coefChart-wrap');
    if (!wrap) { return; }
    var canvas = document.createElement('canvas');
    canvas.id = 'coefChart';
    wrap.appendChild(canvas);

    new Chart(canvas, {
      type: 'bar',
      data: {
        labels: labels,
        datasets: [{
          data: values,
          backgroundColor: values.map(function (v) { return v >= 0 ? RED_L : BLUE_L; }),
          borderColor:     values.map(function (v) { return v >= 0 ? RED   : BLUE;   }),
          borderWidth: 1.5,
          borderRadius: 3
        }]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        indexAxis: 'y',
        plugins: { legend: { display: false } },
        scales: {
          x: { ticks: { color: '#5a6475', font: { size: 10 } }, grid: { color: '#eee' } },
          y: { ticks: { color: '#1c2230', font: { size: 10 } }, grid: { display: false } }
        }
      }
    });
  }());

  // ── ROC curve ─────────────────────────────────────────────────────────────
  (function () {
    var roc = PAGE.rocData;
    if (!roc || !roc.length) { return; }
    new Chart(document.getElementById('rocChart'), {
      type: 'line',
      data: {
        datasets: [
          {
            label: 'ROC curve',
            data: roc.map(function (p) { return { x: p.fpr, y: p.tpr }; }),
            borderColor: BLUE,
            backgroundColor: BLUE_L,
            borderWidth: 2,
            fill: true,
            pointRadius: 0,
            tension: 0.3
          },
          {
            label: 'Random',
            data: [{ x: 0, y: 0 }, { x: 1, y: 1 }],
            borderColor: '#ccc',
            borderDash: [5, 5],
            borderWidth: 1,
            pointRadius: 0,
            fill: false
          }
        ]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          legend: { display: false }
        },
        scales: {
          x: {
            type: 'linear',
            title: { display: true, text: 'False Positive Rate', font: { size: 10 }, color: '#5a6475' },
            min: 0, max: 1,
            ticks: { color: '#5a6475', font: { size: 10 } },
            grid: { color: '#eee' }
          },
          y: {
            type: 'linear',
            title: { display: true, text: 'True Positive Rate', font: { size: 10 }, color: '#5a6475' },
            min: 0, max: 1,
            ticks: { color: '#5a6475', font: { size: 10 } },
            grid: { color: '#eee' }
          }
        }
      }
    });
  }());

  // ── Segment bar charts ────────────────────────────────────────────────────
  (function () {
    var container = document.getElementById('segmentCharts');
    if (!container) { return; }

    var segmentLabels = {
      age_range:         'Age Range',
      income_range:      'Income Range',
      loyalty_flag:      'Loyalty',
      hshd_composition:  'Household Composition',
      children:          'Children in Household'
    };

    var segments = PAGE.segments;
    Object.keys(segments).forEach(function (key) {
      var rows   = segments[key];
      var labels = rows.map(function (r) { return r.label; });
      var values = rows.map(function (r) { return r.churn_rate; });

      var card = document.createElement('div');
      card.className = 'segment-card';

      var h4 = document.createElement('h4');
      h4.textContent = segmentLabels[key] || key.replace(/_/g, ' ');
      card.appendChild(h4);

      var wrap = document.createElement('div');
      wrap.className = 'segment-chart-box';
      var canvas = document.createElement('canvas');
      wrap.appendChild(canvas);
      card.appendChild(wrap);
      container.appendChild(card);

      new Chart(canvas, {
        type: 'bar',
        data: {
          labels: labels,
          datasets: [{
            data: values,
            backgroundColor: RED_L,
            borderColor: RED,
            borderWidth: 1.5,
            borderRadius: 3
          }]
        },
        options: {
          responsive: true,
          maintainAspectRatio: false,
          plugins: {
            legend: { display: false },
            tooltip: {
              callbacks: {
                label: function (ctx) { return ctx.parsed.y + '% churn rate'; }
              }
            }
          },
          scales: {
            x: { ticks: { color: '#1c2230', font: { size: 9 } }, grid: { display: false } },
            y: {
              ticks: {
                color: '#5a6475',
                font: { size: 9 },
                callback: function (v) { return v + '%'; }
              },
              grid: { color: '#eee' }
            }
          }
        }
      });
    });
  }());

}());