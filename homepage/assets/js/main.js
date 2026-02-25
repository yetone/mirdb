/**
 * Main JavaScript
 * Owner: Scenario 8 - Navigation and Footer
 *
 * Expected functionality:
 * - Smooth scroll for anchor links
 * - Mobile hamburger menu toggle
 * - Copy-to-clipboard for code blocks (Scenario 4)
 */

// Smooth scroll for anchor links
document.querySelectorAll('a[href^="#"]').forEach(anchor => {
  anchor.addEventListener('click', function (e) {
    e.preventDefault();
    const target = document.querySelector(this.getAttribute('href'));
    if (target) {
      target.scrollIntoView({
        behavior: 'smooth',
        block: 'start'
      });
    }
  });
});

// ============================================
// SECTION: Copy-to-Clipboard (Scenario 4)
// ============================================

/**
 * Initialize copy-to-clipboard functionality for code blocks.
 * When a copy button is clicked, copy the code content to clipboard
 * and provide visual feedback.
 */
function initCopyToClipboard() {
  const copyButtons = document.querySelectorAll('.code-block__copy');

  copyButtons.forEach(button => {
    button.addEventListener('click', async function() {
      const codeBlock = this.closest('.code-block');
      const codeElement = codeBlock.querySelector('code');
      const textToCopy = codeElement.textContent;
      const copyText = this.querySelector('.code-block__copy-text');

      try {
        await navigator.clipboard.writeText(textToCopy);

        // Visual feedback
        const originalText = copyText.textContent;
        copyText.textContent = 'Copied!';
        this.classList.add('code-block__copy--success');

        // Reset after 2 seconds
        setTimeout(() => {
          copyText.textContent = originalText;
          this.classList.remove('code-block__copy--success');
        }, 2000);
      } catch (err) {
        // Fallback for older browsers
        const textArea = document.createElement('textarea');
        textArea.value = textToCopy;
        textArea.style.position = 'fixed';
        textArea.style.left = '-9999px';
        document.body.appendChild(textArea);
        textArea.select();

        try {
          document.execCommand('copy');
          copyText.textContent = 'Copied!';
          this.classList.add('code-block__copy--success');

          setTimeout(() => {
            copyText.textContent = 'Copy';
            this.classList.remove('code-block__copy--success');
          }, 2000);
        } catch (fallbackErr) {
          copyText.textContent = 'Failed';
          setTimeout(() => {
            copyText.textContent = 'Copy';
          }, 2000);
        }

        document.body.removeChild(textArea);
      }
    });
  });
}

// Initialize copy functionality when DOM is ready
document.addEventListener('DOMContentLoaded', initCopyToClipboard);
