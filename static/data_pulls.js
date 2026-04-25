(function () {
  var form = document.getElementById('dataPullForm');
  var input = document.getElementById('hshd_num');
  var clearBtn = document.getElementById('clearBtn');
  var hshd10Btn = document.getElementById('hshd10Btn');

  if (!form || !input || !clearBtn || !hshd10Btn) {
    return;
  }

  clearBtn.addEventListener('click', function () {
    input.value = '';
    input.focus();
  });

  hshd10Btn.addEventListener('click', function () {
    input.value = '0010';
    form.submit();
  });
})();
