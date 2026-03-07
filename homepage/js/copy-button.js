/**
 * Copy to Clipboard Functionality
 * Owner: Scenarios 4, 15 - Quick Start and Copy Functionality
 */

function copyToClipboard(text) {
  return navigator.clipboard.writeText(text).then(function() {
    return true;
  }).catch(function() {
    return false;
  });
}

function initCopyButtons() {
  document.querySelectorAll('.code-block__copy-btn').forEach(function(btn) {
    btn.addEventListener('click', function() {
      var codeBlock = this.closest('.code-block');
      var code = codeBlock.querySelector('code');
      if (code) {
        copyToClipboard(code.textContent).then(function(success) {
          if (success) {
            btn.querySelector('span').textContent = 'Copied!';
            setTimeout(function() {
              btn.querySelector('span').textContent = 'Copy';
            }, 2000);
          }
        });
      }
    });
  });
}

document.addEventListener('DOMContentLoaded', initCopyButtons);
