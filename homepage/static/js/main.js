/**
 * Main JavaScript
 * Owner: Scenario 11 - Dark/Light Theme Toggle
 * Extended by: Scenario 3 - Copy-to-clipboard functionality
 */

// Theme Management
function initTheme() {
    const savedTheme = localStorage.getItem('theme');
    const prefersDark = window.matchMedia('(prefers-color-scheme: dark)').matches;

    if (savedTheme) {
        document.documentElement.setAttribute('data-theme', savedTheme);
        updateThemeIcon(savedTheme);
    } else if (prefersDark) {
        document.documentElement.setAttribute('data-theme', 'dark');
        updateThemeIcon('dark');
    }
}

function toggleTheme() {
    const currentTheme = document.documentElement.getAttribute('data-theme');
    const newTheme = currentTheme === 'dark' ? 'light' : 'dark';

    document.documentElement.setAttribute('data-theme', newTheme);
    localStorage.setItem('theme', newTheme);
    updateThemeIcon(newTheme);
}

function updateThemeIcon(theme) {
    const icon = document.querySelector('.theme-icon');
    if (icon) {
        icon.textContent = theme === 'dark' ? '☀️' : '🌙';
    }
}

// =============================================================================
// Scenario 3: Copy-to-Clipboard Functionality
// =============================================================================

/**
 * Copy text content to clipboard
 * @param {string} text - Text to copy
 * @returns {Promise<boolean>} - Whether copy was successful
 */
async function copyToClipboard(text) {
    try {
        await navigator.clipboard.writeText(text);
        return true;
    } catch (err) {
        // Fallback for older browsers
        const textArea = document.createElement('textarea');
        textArea.value = text;
        textArea.style.position = 'fixed';
        textArea.style.left = '-9999px';
        document.body.appendChild(textArea);
        textArea.select();
        try {
            document.execCommand('copy');
            document.body.removeChild(textArea);
            return true;
        } catch (fallbackErr) {
            document.body.removeChild(textArea);
            return false;
        }
    }
}

/**
 * Initialize copy buttons on code blocks
 */
function initCopyButtons() {
    const copyButtons = document.querySelectorAll('.copy-button');

    copyButtons.forEach(button => {
        button.addEventListener('click', async () => {
            const targetId = button.getAttribute('data-copy-target');
            const codeElement = document.getElementById(targetId);

            if (!codeElement) {
                console.error(`Copy target not found: ${targetId}`);
                return;
            }

            const textToCopy = codeElement.textContent;
            const success = await copyToClipboard(textToCopy);

            if (success) {
                // Show "Copied!" feedback
                const copyIcon = button.querySelector('.copy-icon');
                const copiedIcon = button.querySelector('.copied-icon');

                if (copyIcon && copiedIcon) {
                    copyIcon.hidden = true;
                    copiedIcon.hidden = false;

                    // Reset after 2 seconds
                    setTimeout(() => {
                        copyIcon.hidden = false;
                        copiedIcon.hidden = true;
                    }, 2000);
                }

                // Add visual feedback class
                button.classList.add('copied');
                setTimeout(() => {
                    button.classList.remove('copied');
                }, 2000);
            }
        });
    });
}

// Initialize on DOM ready
document.addEventListener('DOMContentLoaded', function() {
    initTheme();

    // Initialize copy buttons (Scenario 3)
    initCopyButtons();

    const themeToggle = document.getElementById('theme-toggle');
    if (themeToggle) {
        themeToggle.addEventListener('click', toggleTheme);
    }
});
