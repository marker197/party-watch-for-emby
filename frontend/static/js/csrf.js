/**
 * CSRF double-submit cookie protection.
 *
 * Reads the csrf_token cookie (set by the server on first response) and
 * injects it as X-CSRF-Token header on every POST/PUT/DELETE/PATCH fetch.
 * No changes needed to existing fetch() call-sites.
 */
(function () {
  var _origFetch = window.fetch;

  function getCsrfToken() {
    var match = document.cookie.match(/(?:^|;\s*)csrf_token=([^\s;]+)/);
    return match ? match[1] : '';
  }

  window.fetch = function (url, opts) {
    opts = opts || {};
    var method = (opts.method || 'GET').toUpperCase();
    if (method === 'POST' || method === 'PUT' || method === 'DELETE' || method === 'PATCH') {
      var token = getCsrfToken();
      if (token) {
        if (opts.headers instanceof Headers) {
          if (!opts.headers.has('X-CSRF-Token')) {
            opts.headers.set('X-CSRF-Token', token);
          }
        } else if (typeof opts.headers === 'object' && opts.headers !== null) {
          if (!opts.headers['X-CSRF-Token']) {
            opts.headers['X-CSRF-Token'] = token;
          }
        } else {
          opts.headers = { 'X-CSRF-Token': token };
        }
      }
    }
    return _origFetch.call(this, url, opts);
  };
})();
