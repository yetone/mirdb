import { describe, it, expect, beforeEach, vi, afterEach } from "vitest";
import { mountComponent } from "../helpers/dom-helpers.js";
import {
    initThemeToggle,
    getCurrentTheme,
    applyTheme,
    toggleTheme,
} from "../../components/theme-toggle/theme-toggle.js";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, resolve } from "node:path";

const __dirname = dirname(fileURLToPath(import.meta.url));
const THEME_CSS_PATH = resolve(__dirname, "../../css/theme.css");
const TOGGLE_CSS_PATH = resolve(__dirname, "../../components/theme-toggle/theme-toggle.css");

function injectCss(cssPath) {
    const css = readFileSync(cssPath, "utf8");
    const style = document.createElement("style");
    style.textContent = css;
    document.head.appendChild(style);
    return style;
}

function injectThemeCss() {
    return injectCss(THEME_CSS_PATH);
}

function injectToggleCss() {
    return injectCss(TOGGLE_CSS_PATH);
}

/**
 * Convert hex color to sRGB components [r, g, b] (0-1 range)
 */
function hexToRgb(hex) {
    const clean = hex.replace("#", "");
    const bigint = parseInt(clean.length === 3
        ? clean.split("").map(c => c + c).join("")
        : clean, 16);
    return [
        ((bigint >> 16) & 0xff) / 255,
        ((bigint >> 8) & 0xff) / 255,
        (bigint & 0xff) / 255,
    ];
}

/**
 * Calculate relative luminance per WCAG 2.1
 */
function relativeLuminance([r, g, b]) {
    function channel(c) {
        return c <= 0.03928
            ? c / 12.92
            : Math.pow((c + 0.055) / 1.055, 2.4);
    }
    return 0.2126 * channel(r) + 0.7152 * channel(g) + 0.0722 * channel(b);
}

/**
 * Calculate contrast ratio between two colors
 */
function contrastRatio(colorA, colorB) {
    const lumA = relativeLuminance(colorA);
    const lumB = relativeLuminance(colorB);
    const lighter = Math.max(lumA, lumB);
    const darker = Math.min(lumA, lumB);
    return (lighter + 0.05) / (darker + 0.05);
}

describe("Theme Toggle (PRD REQ-10)", () => {
    let container;

    beforeEach(async () => {
        container = document.createElement("div");
        container.setAttribute("data-component", "theme-toggle");
        document.body.appendChild(container);

        // Reset localStorage and data-theme before each test
        try {
            localStorage.removeItem("theme");
        } catch (_e) {
            // ignore
        }
        delete document.documentElement.dataset.theme;

        // Mount the toggle component
        await mountComponent("theme-toggle", container);
    });

    afterEach(() => {
        delete document.documentElement.dataset.theme;
        try {
            localStorage.removeItem("theme");
        } catch (_e) {
            // ignore
        }
        // Reset matchMedia to default
        if (globalThis.matchMedia) {
            vi.restoreAllMocks();
        }
    });

    // ---------- TC 1: integration — prefers-color-scheme honoured ----------
    it("sets data-theme to 'dark' when system prefers dark scheme and no stored theme", () => {
        // Mock prefers-color-scheme: dark
        const originalMatchMedia = window.matchMedia;
        window.matchMedia = (query) => ({
            matches: query.includes("(prefers-color-scheme: dark)"),
            media: query,
            onchange: null,
            addListener: () => {},
            removeListener: () => {},
            addEventListener: () => {},
            removeEventListener: () => {},
            dispatchEvent: () => false,
        });

        const theme = getCurrentTheme();
        expect(theme).toBe("dark");

        applyTheme(theme);
        expect(document.documentElement.dataset.theme).toBe("dark");

        window.matchMedia = originalMatchMedia;
    });

    it("sets data-theme to 'light' when system prefers light scheme and no stored theme", () => {
        const originalMatchMedia = window.matchMedia;
        window.matchMedia = (query) => ({
            matches: false,
            media: query,
            onchange: null,
            addListener: () => {},
            removeListener: () => {},
            addEventListener: () => {},
            removeEventListener: () => {},
            dispatchEvent: () => false,
        });

        const theme = getCurrentTheme();
        expect(theme).toBe("light");

        applyTheme(theme);
        expect(document.documentElement.dataset.theme).toBe("light");

        window.matchMedia = originalMatchMedia;
    });

    // ---------- TC 2: integration — click toggles theme ----------
    it("clicking the toggle switches from light to dark and updates aria-pressed and localStorage", () => {
        // Start with light theme
        applyTheme("light");
        localStorage.setItem("theme", "light");

        const button = document.querySelector(".theme-toggle");
        expect(button).not.toBeNull();

        initThemeToggle(button);
        expect(button.getAttribute("aria-pressed")).toBe("false");

        button.click();

        expect(document.documentElement.dataset.theme).toBe("dark");
        expect(button.getAttribute("aria-pressed")).toBe("true");
        expect(localStorage.getItem("theme")).toBe("dark");
    });

    it("clicking the toggle switches from dark to light", () => {
        applyTheme("dark");
        localStorage.setItem("theme", "dark");

        const button = document.querySelector(".theme-toggle");
        initThemeToggle(button);
        expect(button.getAttribute("aria-pressed")).toBe("true");

        button.click();

        expect(document.documentElement.dataset.theme).toBe("light");
        expect(button.getAttribute("aria-pressed")).toBe("false");
        expect(localStorage.getItem("theme")).toBe("light");
    });

    // ---------- TC 3: e2e — persistence across reload ----------
    it("restores the dark theme from localStorage on re-initialization", () => {
        // Simulate user toggling to dark
        applyTheme("dark");
        localStorage.setItem("theme", "dark");

        // Simulate page reload by re-creating button and re-initializing
        const freshContainer = document.createElement("div");
        document.body.appendChild(freshContainer);
        freshContainer.innerHTML = container.querySelector(".theme-toggle").outerHTML; // copy the markup

        const freshButton = freshContainer.querySelector(".theme-toggle");
        initThemeToggle(freshButton);

        expect(document.documentElement.dataset.theme).toBe("dark");
        expect(freshButton.getAttribute("aria-pressed")).toBe("true");
    });

    it("restores the light theme from localStorage on re-initialization", () => {
        applyTheme("light");
        localStorage.setItem("theme", "light");

        const freshContainer = document.createElement("div");
        document.body.appendChild(freshContainer);
        freshContainer.innerHTML = `<button class="theme-toggle" aria-pressed="false" aria-label="Toggle dark mode" type="button"></button>`;

        const freshButton = freshContainer.querySelector(".theme-toggle");
        initThemeToggle(freshButton);

        expect(document.documentElement.dataset.theme).toBe("light");
        expect(freshButton.getAttribute("aria-pressed")).toBe("false");
    });

    // ---------- TC 4: unit — CSS custom properties differ and contrast is sufficient ----------
    it("has different background colors for light and dark themes with WCAG AA contrast", () => {
        injectThemeCss();

        // Light mode
        document.documentElement.dataset.theme = "light";
        const lightBg = getComputedStyle(document.documentElement)
            .getPropertyValue("--color-background")
            .trim();
        const lightText = getComputedStyle(document.documentElement)
            .getPropertyValue("--color-text")
            .trim();

        // Dark mode
        document.documentElement.dataset.theme = "dark";
        const darkBg = getComputedStyle(document.documentElement)
            .getPropertyValue("--color-background")
            .trim();
        const darkText = getComputedStyle(document.documentElement)
            .getPropertyValue("--color-text")
            .trim();

        expect(lightBg).not.toBe(darkBg);
        expect(lightText).not.toBe(darkText);

        // Verify contrast ratios
        const lightContrast = contrastRatio(hexToRgb(lightBg), hexToRgb(lightText));
        const darkContrast = contrastRatio(hexToRgb(darkBg), hexToRgb(darkText));

        expect(lightContrast).toBeGreaterThanOrEqual(4.5);
        expect(darkContrast).toBeGreaterThanOrEqual(4.5);
    });

    // ---------- TC 5: unit — corrupt localStorage value handled gracefully ----------
    it("falls back to system preference when localStorage contains an invalid theme", () => {
        localStorage.setItem("theme", "banana");

        const originalMatchMedia = window.matchMedia;
        window.matchMedia = (query) => ({
            matches: true,
            media: query,
            onchange: null,
            addListener: () => {},
            removeListener: () => {},
            addEventListener: () => {},
            removeEventListener: () => {},
            dispatchEvent: () => false,
        });

        expect(() => getCurrentTheme()).not.toThrow();
        expect(getCurrentTheme()).toBe("dark"); // system prefers dark

        window.matchMedia = originalMatchMedia;
    });

    it("falls back to light when localStorage is invalid and system prefers light", () => {
        localStorage.setItem("theme", "invalid-theme-123");

        const originalMatchMedia = window.matchMedia;
        window.matchMedia = (query) => ({
            matches: false,
            media: query,
            onchange: null,
            addListener: () => {},
            removeListener: () => {},
            addEventListener: () => {},
            removeEventListener: () => {},
            dispatchEvent: () => false,
        });

        expect(() => getCurrentTheme()).not.toThrow();
        expect(getCurrentTheme()).toBe("light");

        window.matchMedia = originalMatchMedia;
    });

    // ---------- TC 6: e2e — keyboard parity (Enter / Space) ----------
    it("toggles theme identically when pressing Enter key", () => {
        applyTheme("light");
        localStorage.setItem("theme", "light");

        const button = document.querySelector(".theme-toggle");
        initThemeToggle(button);
        expect(button.getAttribute("aria-pressed")).toBe("false");

        const enterEvent = new KeyboardEvent("keydown", { key: "Enter", bubbles: true });
        button.dispatchEvent(enterEvent);

        expect(document.documentElement.dataset.theme).toBe("dark");
        expect(button.getAttribute("aria-pressed")).toBe("true");
        expect(localStorage.getItem("theme")).toBe("dark");
    });

    it("toggles theme identically when pressing Space key", () => {
        applyTheme("light");
        localStorage.setItem("theme", "light");

        const button = document.querySelector(".theme-toggle");
        initThemeToggle(button);
        expect(button.getAttribute("aria-pressed")).toBe("false");

        const spaceEvent = new KeyboardEvent("keydown", { key: " ", bubbles: true });
        button.dispatchEvent(spaceEvent);

        expect(document.documentElement.dataset.theme).toBe("dark");
        expect(button.getAttribute("aria-pressed")).toBe("true");
        expect(localStorage.getItem("theme")).toBe("dark");
    });

    // ---------- Resilience tests ----------
    it("returns early when button is null", () => {
        expect(() => initThemeToggle(null)).not.toThrow();
    });

    it("is idempotent — calling initThemeToggle twice does not double-register handlers", () => {
        const button = document.querySelector(".theme-toggle");
        applyTheme("light");
        localStorage.setItem("theme", "light");

        initThemeToggle(button);
        initThemeToggle(button);

        // Click once — should toggle once, not toggle twice and end up back at light
        button.click();
        expect(document.documentElement.dataset.theme).toBe("dark");

        // Click again — should toggle back
        button.click();
        expect(document.documentElement.dataset.theme).toBe("light");
    });

    it("has correct initial accessibility attributes", () => {
        const button = document.querySelector(".theme-toggle");
        expect(button).not.toBeNull();
        expect(button.getAttribute("aria-pressed")).toBe("false");
        expect(button.getAttribute("aria-label")).toBe("Toggle dark mode");
        expect(button.getAttribute("type")).toBe("button");
    });

    it("has visually-hidden label for screen readers", () => {
        const label = document.querySelector(".theme-toggle__label");
        expect(label).not.toBeNull();
        expect(label.classList.contains("visually-hidden")).toBe(true);
    });

    it("toggleTheme returns the new theme and applies it", () => {
        applyTheme("light");
        localStorage.setItem("theme", "light");

        const result = toggleTheme();
        expect(result).toBe("dark");
        expect(document.documentElement.dataset.theme).toBe("dark");
        expect(localStorage.getItem("theme")).toBe("dark");

        const result2 = toggleTheme();
        expect(result2).toBe("light");
        expect(document.documentElement.dataset.theme).toBe("light");
    });
});

describe("Theme Toggle — prefers-color-scheme media query change", () => {
    it("follows system theme changes when no explicit user choice is stored", () => {
        localStorage.removeItem("theme");

        // Mock matchMedia with change event support
        let darkMode = false;
        const listeners = [];
        const originalMatchMedia = window.matchMedia;

        window.matchMedia = (query) => ({
            get matches() {
                return query.includes("(prefers-color-scheme: dark)")
                    ? darkMode
                    : false;
            },
            media: query,
            onchange: null,
            addListener: () => {},
            removeListener: () => {},
            addEventListener: (type, handler) => {
                if (type === "change") listeners.push(handler);
            },
            removeEventListener: () => {},
            dispatchEvent: () => false,
        });

        const button = document.createElement("button");
        button.className = "theme-toggle";
        button.setAttribute("aria-pressed", "false");
        button.setAttribute("aria-label", "Toggle dark mode");
        document.body.appendChild(button);

        initThemeToggle(button);

        // Initially light (darkMode = false)
        expect(document.documentElement.dataset.theme).toBe("light");

        // Simulate system switching to dark
        darkMode = true;
        listeners.forEach((fn) => fn({ matches: true }));

        expect(document.documentElement.dataset.theme).toBe("dark");
        expect(button.getAttribute("aria-pressed")).toBe("true");

        window.matchMedia = originalMatchMedia;
        listeners.length = 0;
    });

    it("ignores system theme changes when user has made an explicit choice", () => {
        localStorage.setItem("theme", "light");
        applyTheme("light");

        const listeners = [];
        const originalMatchMedia = window.matchMedia;

        window.matchMedia = (query) => ({
            get matches() {
                return query.includes("(prefers-color-scheme: dark)")
                    ? true
                    : false;
            },
            media: query,
            onchange: null,
            addListener: () => {},
            removeListener: () => {},
            addEventListener: (type, handler) => {
                if (type === "change") listeners.push(handler);
            },
            removeEventListener: () => {},
            dispatchEvent: () => false,
        });

        const button = document.createElement("button");
        button.className = "theme-toggle";
        button.setAttribute("aria-pressed", "false");
        button.setAttribute("aria-label", "Toggle dark mode");
        document.body.appendChild(button);

        initThemeToggle(button);

        // Start light (user choice)
        expect(document.documentElement.dataset.theme).toBe("light");

        // Simulate system switching to dark
        listeners.forEach((fn) => fn({ matches: true }));

        // Should stay light because user made an explicit choice
        expect(document.documentElement.dataset.theme).toBe("light");

        window.matchMedia = originalMatchMedia;
        listeners.length = 0;
    });
});
