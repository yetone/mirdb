/**
 * UI Utilities
 * Owner: First builder (shared UI components)
 *
 * Expected exports:
 * - showModal(content) - Display modal overlay
 * - hideModal() - Close modal
 * - showToast(message, type) - Show notification toast
 * - showConfirm(message, onConfirm) - Confirmation dialog
 * - copyToClipboard(text) - Copy text to clipboard
 */

/**
 * Copy text to clipboard
 * @param {string} text - The text to copy
 * @returns {Promise<boolean>} - Returns true if successful
 */
async function copyToClipboard(text) {
    try {
        if (navigator.clipboard && navigator.clipboard.writeText) {
            await navigator.clipboard.writeText(text);
            return true;
        }
        // Fallback for older browsers
        return fallbackCopyToClipboard(text);
    } catch (err) {
        console.error('Failed to copy text:', err);
        return fallbackCopyToClipboard(text);
    }
}

/**
 * Fallback copy method for browsers without Clipboard API
 * @param {string} text - The text to copy
 * @returns {boolean} - Returns true if successful
 */
function fallbackCopyToClipboard(text) {
    var textArea = document.createElement('textarea');
    textArea.value = text;
    textArea.style.position = 'fixed';
    textArea.style.left = '-9999px';
    textArea.style.top = '-9999px';
    textArea.setAttribute('readonly', '');
    document.body.appendChild(textArea);

    try {
        textArea.select();
        textArea.setSelectionRange(0, 99999);
        var successful = document.execCommand('copy');
        document.body.removeChild(textArea);
        return successful;
    } catch (err) {
        console.error('Fallback copy failed:', err);
        document.body.removeChild(textArea);
        return false;
    }
}

/**
 * Initialize copy buttons with click handlers
 */
function initCopyButtons() {
    var copyButtons = document.querySelectorAll('.copy-btn[data-copy-target]');

    copyButtons.forEach(function(button) {
        button.addEventListener('click', async function(event) {
            event.preventDefault();
            var targetId = button.getAttribute('data-copy-target');
            var targetElement = document.getElementById(targetId);

            if (!targetElement) {
                console.error('Copy target not found:', targetId);
                return;
            }

            var textToCopy = targetElement.textContent || targetElement.innerText;
            var success = await copyToClipboard(textToCopy);

            if (success) {
                showCopyFeedback(button);
            }
        });
    });
}

/**
 * Show visual feedback when copy is successful
 * @param {HTMLElement} button - The copy button element
 */
function showCopyFeedback(button) {
    var copyText = button.querySelector('.copy-text');
    var originalText = copyText ? copyText.textContent : 'Copy';

    button.classList.add('copied');
    if (copyText) {
        copyText.textContent = 'Copied!';
    }

    setTimeout(function() {
        button.classList.remove('copied');
        if (copyText) {
            copyText.textContent = originalText;
        }
    }, 2000);
}

/**
 * Show a toast notification
 * @param {string} message - The message to display
 * @param {string} type - The type of toast ('success', 'error', 'info')
 */
function showToast(message, type) {
    type = type || 'info';

    // Remove existing toasts
    var existingToast = document.querySelector('.toast');
    if (existingToast) {
        existingToast.remove();
    }

    var toast = document.createElement('div');
    toast.className = 'toast toast-' + type;
    toast.textContent = message;
    toast.setAttribute('role', 'alert');
    toast.setAttribute('aria-live', 'polite');

    document.body.appendChild(toast);

    // Trigger animation
    requestAnimationFrame(function() {
        toast.classList.add('toast-visible');
    });

    // Remove after delay
    setTimeout(function() {
        toast.classList.remove('toast-visible');
        setTimeout(function() { toast.remove(); }, 300);
    }, 3000);
}

/**
 * Show a modal overlay
 * @param {string|HTMLElement} content - The content to display in the modal
 */
function showModal(content) {
    var overlay = document.createElement('div');
    overlay.className = 'modal-overlay';
    overlay.id = 'modal-overlay';
    overlay.setAttribute('role', 'dialog');
    overlay.setAttribute('aria-modal', 'true');

    var modal = document.createElement('div');
    modal.className = 'modal';

    var closeBtn = document.createElement('button');
    closeBtn.className = 'modal-close';
    closeBtn.innerHTML = '&times;';
    closeBtn.setAttribute('aria-label', 'Close modal');
    closeBtn.onclick = hideModal;

    var contentContainer = document.createElement('div');
    contentContainer.className = 'modal-content';

    if (typeof content === 'string') {
        contentContainer.innerHTML = content;
    } else {
        contentContainer.appendChild(content);
    }

    modal.appendChild(closeBtn);
    modal.appendChild(contentContainer);
    overlay.appendChild(modal);
    document.body.appendChild(overlay);

    // Focus trap
    closeBtn.focus();

    // Close on overlay click
    overlay.addEventListener('click', function(e) {
        if (e.target === overlay) {
            hideModal();
        }
    });

    // Close on Escape key
    document.addEventListener('keydown', handleModalKeydown);
}

/**
 * Hide the current modal
 */
function hideModal() {
    var overlay = document.getElementById('modal-overlay');
    if (overlay) {
        overlay.remove();
        document.removeEventListener('keydown', handleModalKeydown);
    }
}

/**
 * Handle keydown events for modal
 * @param {KeyboardEvent} e - The keyboard event
 */
function handleModalKeydown(e) {
    if (e.key === 'Escape') {
        hideModal();
    }
}

/**
 * Show a confirmation dialog
 * @param {string} message - The confirmation message
 * @param {Function} onConfirm - Callback when confirmed
 * @param {Function} [onCancel] - Optional callback when cancelled
 */
function showConfirm(message, onConfirm, onCancel) {
    var content = document.createElement('div');
    content.className = 'confirm-dialog';

    var text = document.createElement('p');
    text.textContent = message;

    var buttons = document.createElement('div');
    buttons.className = 'confirm-buttons modal-buttons';

    var cancelBtn = document.createElement('button');
    cancelBtn.className = 'btn btn-secondary';
    cancelBtn.textContent = 'Cancel';
    cancelBtn.onclick = function() {
        hideModal();
        if (onCancel) onCancel();
    };

    var confirmBtn = document.createElement('button');
    confirmBtn.className = 'btn btn-primary btn-danger';
    confirmBtn.textContent = 'Confirm';
    confirmBtn.onclick = function() {
        hideModal();
        if (onConfirm) onConfirm();
    };

    buttons.appendChild(cancelBtn);
    buttons.appendChild(confirmBtn);
    content.appendChild(text);
    content.appendChild(buttons);

    showModal(content);
}

// Export functions for use by other modules
if (typeof window !== 'undefined') {
    window.UI = {
        copyToClipboard: copyToClipboard,
        initCopyButtons: initCopyButtons,
        showToast: showToast,
        showModal: showModal,
        hideModal: hideModal,
        showConfirm: showConfirm
    };
}

// Also export as MirDBUI for compatibility
var MirDBUI = {
    showModal: showModal,
    hideModal: hideModal,
    showToast: showToast,
    showConfirm: showConfirm,
    copyToClipboard: copyToClipboard
};
