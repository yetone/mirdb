/**
 * Clipboard Module
 * Owner: Scenario 3 - Getting Started Section
 */

export function initClipboard() {
  document.querySelectorAll('.copy-btn').forEach(btn => {
    btn.addEventListener('click', async () => {
      const text = btn.getAttribute('data-copy');
      if (text) {
        const success = await copyToClipboard(text);
        if (success) {
          showFeedback(btn);
        }
      }
    });
  });
}

export async function copyToClipboard(text) {
  try {
    if (navigator.clipboard && navigator.clipboard.writeText) {
      await navigator.clipboard.writeText(text);
      return true;
    }
    // Fallback for older browsers
    return fallbackCopy(text);
  } catch (err) {
    return fallbackCopy(text);
  }
}

function fallbackCopy(text) {
  const textarea = document.createElement('textarea');
  textarea.value = text;
  textarea.style.position = 'fixed';
  textarea.style.opacity = '0';
  document.body.appendChild(textarea);
  textarea.select();
  try {
    document.execCommand('copy');
    return true;
  } catch (err) {
    return false;
  } finally {
    document.body.removeChild(textarea);
  }
}

function showFeedback(btn) {
  const icon = btn.querySelector('.copy-icon');
  if (icon) {
    const original = icon.textContent;
    icon.textContent = 'Copied!';
    setTimeout(() => {
      icon.textContent = original;
    }, 2000);
  }
}
