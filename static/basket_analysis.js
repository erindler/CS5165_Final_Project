(function () {
  'use strict';

  var PAGE = JSON.parse(document.getElementById('page-data').textContent);

  // ── Set bar widths from data-pct attributes ────────────────────────────────
  // Inline style="width:X%" is blocked by CSP style-src-attr, so widths are
  // stored as data attributes and applied here by JS instead.
  document.querySelectorAll('.bar-fill, .pred-fill').forEach(function (el) {
    var pct = el.getAttribute('data-pct');
    if (pct) {
      el.style.width = pct + '%';
    }
  });

  document.addEventListener('click', function (e) {
    var header = e.target.closest('.cs-header');
    if (header) {
      header.closest('.cs-item').classList.toggle('open');
    }
  });

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
            '<p><strong>Re-running analysis...</strong></p>' +
            '<p>Training Random Forest models on household data.</p>' +
            '</div></div>';
          startPolling();
        });
    });
  }

  if (PAGE.hasResults && PAGE.cooccurrence.length) {
    var labels = PAGE.cooccurrence.map(function (p) {
      return p.commodity_a + ' x ' + p.commodity_b;
    });
    var values = PAGE.cooccurrence.map(function (p) {
      return p.joint_households;
    });
    new Chart(document.getElementById('coChart'), {
      type: 'bar',
      data: {
        labels: labels,
        datasets: [{
          data: values,
          backgroundColor: 'rgba(11,99,206,0.15)',
          borderColor: 'rgba(11,99,206,0.7)',
          borderWidth: 1.5,
          borderRadius: 4
        }]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        indexAxis: 'y',
        plugins: { legend: { display: false } },
        scales: {
          x: {
            min: 300,
            ticks: { 
                color: '#5a6475',
                font: { size: 10 },
                },
            grid: { color: '#eee' }
          },
          y: {
            ticks: { color: '#1c2230', font: { size: 10 } },
            grid: { display: false }
          }
        }
      }
    });
  }

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
              '<p>' + json.message + '</p>' +
              '</div></div>';
          }
        });
    }, 3000);
  }

  // Start polling automatically if the page loaded without results yet
  if (!PAGE.hasResults && !PAGE.hasError) {
    startPolling();
  }

}());