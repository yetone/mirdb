/**
 * Clipboard Utility.
 * Owner: Scenario 3 - Quick Start Section
 *
 * Provides copy-to-clipboard functionality with visual feedback.
 */

/**
 * Copies the provided text to the clipboard.
 * @param text - The text to copy to the clipboard
 * @returns Promise resolving to true on success, false on failure
 */
export async function copyToClipboard(text: string): Promise<boolean> {
  try {
    if (navigator.clipboard && navigator.clipboard.writeText) {
      await navigator.clipboard.writeText(text);
      return true;
    }

    // Fallback for older browsers
    const textArea = document.createElement('textarea');
    textArea.value = text;
    textArea.style.position = 'fixed';
    textArea.style.left = '-9999px';
    textArea.style.top = '-9999px';
    document.body.appendChild(textArea);
    textArea.focus();
    textArea.select();

    const success = document.execCommand('copy');
    document.body.removeChild(textArea);
    return success;
  } catch {
    return false;
  }
}

/**
 * Creates a copy button element.
 * @returns HTMLButtonElement configured as a copy button
 */
function createCopyButton(): HTMLButtonElement {
  const button = document.createElement('button');
  button.type = 'button';
  button.className = 'copy-button';
  button.setAttribute('aria-label', 'Copy code to clipboard');
  button.innerHTML = `
    <svg class="copy-icon" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
      <rect x="9" y="9" width="13" height="13" rx="2" ry="2"></rect>
      <path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"></path>
    </svg>
    <svg class="check-icon" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="display: none;">
      <polyline points="20 6 9 17 4 12"></polyline>
    </svg>
    <span class="copy-text">Copy</span>
  `;
  return button;
}

/**
 * Shows visual feedback when copy is successful.
 * @param button - The copy button element
 */
function showCopySuccess(button: HTMLButtonElement): void {
  const copyIcon = button.querySelector('.copy-icon') as SVGElement;
  const checkIcon = button.querySelector('.check-icon') as SVGElement;
  const copyText = button.querySelector('.copy-text') as HTMLSpanElement;

  if (copyIcon && checkIcon && copyText) {
    copyIcon.style.display = 'none';
    checkIcon.style.display = 'block';
    copyText.textContent = 'Copied!';
    button.classList.add('copied');

    setTimeout(() => {
      copyIcon.style.display = 'block';
      checkIcon.style.display = 'none';
      copyText.textContent = 'Copy';
      button.classList.remove('copied');
    }, 2000);
  }
}

/**
 * Attaches a copy button to a code block element.
 * @param codeBlock - The code block element (pre or code element)
 */
export function attachCopyButton(codeBlock: HTMLElement): void {
  const wrapper = document.createElement('div');
  wrapper.className = 'code-block-wrapper';

  const button = createCopyButton();

  // Get the parent pre element if codeBlock is a code element
  const preElement = codeBlock.tagName === 'CODE' ? codeBlock.parentElement : codeBlock;
  if (!preElement) return;

  // Wrap the pre element
  preElement.parentNode?.insertBefore(wrapper, preElement);
  wrapper.appendChild(preElement);
  wrapper.appendChild(button);

  button.addEventListener('click', async () => {
    const codeElement = preElement.querySelector('code') || preElement;
    const text = codeElement.textContent || '';
    const success = await copyToClipboard(text);
    if (success) {
      showCopySuccess(button);
    }
  });
}
