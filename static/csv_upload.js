(function () {
  var zones = document.querySelectorAll('.drop-zone');

  if (!zones.length) {
    return;
  }

  function updateFileLabel(inputId, fileName) {
    var label = document.querySelector('[data-file-label="' + inputId + '"]');
    if (!label) {
      return;
    }
    label.textContent = fileName || 'Drop file here or click to choose';
  }

  function bindDropZone(zone) {
    var targetId = zone.getAttribute('data-target');
    if (!targetId) {
      return;
    }

    var input = document.getElementById(targetId);
    if (!input) {
      return;
    }

    input.addEventListener('change', function () {
      if (input.files && input.files.length > 0) {
        updateFileLabel(targetId, input.files[0].name);
      }
    });

    ['dragenter', 'dragover'].forEach(function (eventName) {
      zone.addEventListener(eventName, function (event) {
        event.preventDefault();
        event.stopPropagation();
        zone.classList.add('is-dragover');
      });
    });

    ['dragleave', 'drop'].forEach(function (eventName) {
      zone.addEventListener(eventName, function (event) {
        event.preventDefault();
        event.stopPropagation();
        zone.classList.remove('is-dragover');
      });
    });

    zone.addEventListener('drop', function (event) {
      var dt = event.dataTransfer;
      if (!dt || !dt.files || dt.files.length === 0) {
        return;
      }

      input.files = dt.files;
      updateFileLabel(targetId, dt.files[0].name);
    });
  }

  zones.forEach(bindDropZone);
})();
