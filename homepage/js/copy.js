/*
 * Owner: Scenario 3 - Installation Instructions.
 * Expected exports:
 *   initCopyButtons(): void
 *     Attaches click handlers to all .copy-btn elements; on click, copies the
 *     associated code block content to clipboard via navigator.clipboard.writeText.
 *     MUST handle clipboard API absence (graceful fallback to document.execCommand).
 */

const COPIED_CLASS = 'copied';
const COPIED_TIMEOUT_MS = 2000;

function getCodeText(button) {
  // Find the associated code block: sibling or ancestor's descendant
  const codeBlock = button.closest('.code-block');
  if (codeBlock) {
    const code = codeBlock.querySelector('code[data-copy]');
    if (code) {
      return code.textContent;
    }
    const preCode = codeBlock.querySelector('pre code');
    if (preCode) {
      return preCode.textContent;
    }
  }
  // Fallback: look within parent container
  const container = button.parentElement;
  if (container) {
    const code = container.querySelector('code[data-copy]');
    if (code) return code.textContent;
    const preCode = container.querySelector('pre code');
    if (preCode) return preCode.textContent;
  }
  return '';
}

async function copyToClipboard(text) {
  if (navigator.clipboard && navigator.clipboard.writeText) {
    await navigator.clipboard.writeText(text);
    return;
  }

  // Fallback: document.execCommand('copy')
  const textarea = document.createElement('textarea');
  textarea.value = text;
  textarea.setAttribute('readonly', '');
  textarea.style.position = 'absolute';
  textarea.style.left = '-9999px';
  document.body.appendChild(textarea);
  textarea.select();
  document.execCommand('copy');
  document.body.removeChild(textarea);
}

function showCopiedFeedback(button) {
  button.classList.add(COPIED_CLASS);
  setTimeout(() => {
    button.classList.remove(COPIED_CLASS);
  }, COPIED_TIMEOUT_MS);
}

export function initCopyButtons() {
  const buttons = document.querySelectorAll('.copy-btn');

  buttons.forEach(button => {
    // Prevent duplicate handlers
    if (button.dataset.copyWired === 'true') return;
    button.dataset.copyWired = 'true';

    button.addEventListener('click', async () => {
      const text = getCodeText(button);
      if (!text) return;

      try {
        await copyToClipboard(text);
        showCopiedFeedback(button);
      } catch {
        // Silently fail if clipboard is unavailable
      }
    });
  });
}
