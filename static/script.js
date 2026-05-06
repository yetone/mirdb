document.addEventListener('DOMContentLoaded', function() {
  setupClipboardButtons();
});

/**
 * Sets up clipboard functionality for all copy buttons
 * Adds click handlers that copy command to clipboard with visual confirmation
 */
function setupClipboardButtons() {
  const buttons = document.querySelectorAll('.copy-btn');
  buttons.forEach(button => {
    button.addEventListener('click', () => {
      const command = button.getAttribute('data-command');
      navigator.clipboard.writeText(command).then(() => {
        // Visual feedback
        const originalText = button.textContent;
        button.textContent = 'Copied!';
        button.style.backgroundColor = '#4CAF50';

        setTimeout(() => {
          button.textContent = originalText;
          button.style.backgroundColor = '';
        }, 2000);
      }).catch(err => {
        console.error('Failed to copy:', err);
      });
    });
  });
}