/**
 * Theme toggle behaviour — Scenario 8.
 *
 * Expected exports:
 *   - initThemeToggle(button: HTMLElement): void
 *     Reads localStorage("theme"); applies data-theme on <html>;
 *     persists user choice.
 *   - getCurrentTheme(): "light" | "dark"
 */

const THEME_STORAGE_KEY = "theme";
const VALID_THEMES = ["light", "dark"];

/**
 * Detect system preference via prefers-color-scheme.
 * @returns {"dark" | "light"}
 */
function getSystemTheme() {
    if (typeof window !== "undefined" && window.matchMedia) {
        return window.matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light";
    }
    return "light";
}

/**
 * Get the current active theme.
 * Falls back to system preference if no valid stored theme.
 * @returns {"light" | "dark"}
 */
export function getCurrentTheme() {
    let stored;
    try {
        if (typeof localStorage !== "undefined") {
            stored = localStorage.getItem(THEME_STORAGE_KEY);
        }
    } catch (_e) {
        // localStorage may be unavailable (e.g. private mode)
    }

    if (stored && VALID_THEMES.includes(stored)) {
        return stored;
    }

    return getSystemTheme();
}

/**
 * Apply a theme to the document root.
 * @param {"light" | "dark"} theme
 */
export function applyTheme(theme) {
    const root = document.documentElement;
    if (root) {
        root.dataset.theme = theme;
    }
}

/**
 * Persist a theme choice to localStorage.
 * @param {"light" | "dark"} theme
 */
function persistTheme(theme) {
    try {
        if (typeof localStorage !== "undefined") {
            localStorage.setItem(THEME_STORAGE_KEY, theme);
        }
    } catch (_e) {
        // localStorage may be unavailable
    }
}

/**
 * Toggle between light and dark themes.
 * @returns {"light" | "dark"} The new theme after toggle
 */
export function toggleTheme() {
    const current = getCurrentTheme();
    const next = current === "light" ? "dark" : "light";
    applyTheme(next);
    persistTheme(next);
    return next;
}

/**
 * Initialize the theme toggle on a button element.
 * Applies the current theme and wires up click / keyboard handlers.
 *
 * @param {HTMLElement} button - The toggle button element
 */
export function initThemeToggle(button) {
    if (!button) {
        return;
    }

    // Idempotent: don't double-register
    if (button.dataset.themeToggleInitialized === "true") {
        return;
    }
    button.dataset.themeToggleInitialized = "true";

    // Apply initial theme before any paint
    const theme = getCurrentTheme();
    applyTheme(theme);
    button.setAttribute("aria-pressed", String(theme === "dark"));

    function updateButtonState(newTheme) {
        button.setAttribute("aria-pressed", String(newTheme === "dark"));
    }

    function handleToggle() {
        const newTheme = toggleTheme();
        updateButtonState(newTheme);
    }

    button.addEventListener("click", handleToggle);

    // Keyboard parity: Enter / Space toggles theme identically to click
    button.addEventListener("keydown", (event) => {
        if (event.key === "Enter" || event.key === " ") {
            event.preventDefault();
            handleToggle();
        }
    });

    // Listen for system theme changes (only when no explicit user choice is stored)
    if (typeof window !== "undefined" && window.matchMedia) {
        const mq = window.matchMedia("(prefers-color-scheme: dark)");
        mq.addEventListener("change", (event) => {
            let hasStoredTheme = false;
            try {
                const stored = localStorage.getItem(THEME_STORAGE_KEY);
                hasStoredTheme = stored !== null && VALID_THEMES.includes(stored);
            } catch (_e) {
                // ignore
            }
            if (!hasStoredTheme) {
                const systemTheme = event.matches ? "dark" : "light";
                applyTheme(systemTheme);
                updateButtonState(systemTheme);
            }
        });
    }
}
