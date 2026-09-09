// Boot watchdog: classic script, runs even if the module app fails to load.
(function () {
  window.__roveBoot = false;
  window.__roveBootError = '';
  window.addEventListener('error', function (event) {
    if (!window.__roveBoot && !window.__roveBootError && event && event.message) {
      window.__roveBootError = String(event.message).slice(0, 200);
    }
  }, true);
  setTimeout(function () {
    if (window.__roveBoot) return;
    var connection = document.getElementById('connection');
    if (connection) connection.textContent = "App didn't start";
    var notice = document.getElementById('notice');
    if (notice) {
      notice.hidden = false;
      notice.setAttribute('data-kind', 'error');
      notice.textContent = "The app didn't start"
        + (window.__roveBootError ? ': ' + window.__roveBootError : ' (a script failed to load)')
        + '. Hard-refresh with Cmd+Shift+R (Mac) or Ctrl+Shift+R, then report exactly what this says.';
    }
  }, 8000);
})();
