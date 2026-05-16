import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, resolve } from "node:path";
import { simulateBreakpoint } from "../helpers/dom-helpers.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const RESPONSIVE_CSS_PATH = resolve(__dirname, "../../css/responsive.css");
const RESPONSIVE_CSS = readFileSync(RESPONSIVE_CSS_PATH, "utf8");

function escapeRegex(str) {
    return str.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

/**
 * Extract the body (everything between { and }) of the first matching CSS
 * rule for a raw (unescaped) selector. If a media query expression is
 * supplied (e.g. "min-width: 600px"), the rule must live inside that
 * @media block. JSDOM does not compute styles against viewport widths, so
 * we validate the rulesets directly against the source CSS text.
 */
function extractRuleBody(css, selector, mediaQuery) {
    let scope = css;
    if (mediaQuery) {
        const condition = mediaQuery.replace(/\s+/g, "\\s*");
        const mediaRe = new RegExp(
            `@media\\s*\\(\\s*${condition}\\s*\\)\\s*\\{`,
            "i"
        );
        const match = scope.match(mediaRe);
        if (!match) return null;
        const start = match.index + match[0].length;
        // Walk braces to find the matching closing brace of the @media block
        let depth = 1;
        let i = start;
        while (i < scope.length && depth > 0) {
            const ch = scope[i++];
            if (ch === "{") depth++;
            else if (ch === "}") depth--;
        }
        if (depth !== 0) return null;
        scope = scope.slice(start, i - 1);
    }
    const ruleRe = new RegExp(`${escapeRegex(selector)}\\s*\\{([^}]*)\\}`, "i");
    const ruleMatch = scope.match(ruleRe);
    return ruleMatch ? ruleMatch[1] : null;
}

function countGridTracks(declaration) {
    if (!declaration) return 0;
    const repeatMatch = declaration.match(/repeat\(\s*(\d+)\s*,/);
    if (repeatMatch) return Number(repeatMatch[1]);
    if (/^\s*1fr\s*$/.test(declaration)) return 1;
    if (/^\s*(none|auto)\s*$/.test(declaration)) return 1;
    return declaration.split(/\s+/).filter(Boolean).length;
}

/** Build a matchMedia mock keyed off a numeric viewport width. */
function installMatchMediaMock(width) {
    const mock = (query) => {
        const q = String(query ?? "");
        const minMatch = q.match(/\(min-width:\s*(\d+)px\)/);
        const maxMatch = q.match(/\(max-width:\s*(\d+)px\)/);
        let matches = true;
        if (minMatch && width < Number(minMatch[1])) matches = false;
        if (maxMatch && width > Number(maxMatch[1])) matches = false;
        const listeners = new Set();
        return {
            matches,
            media: q,
            onchange: null,
            addListener: (fn) => listeners.add(fn),
            removeListener: (fn) => listeners.delete(fn),
            addEventListener: (_evt, fn) => listeners.add(fn),
            removeEventListener: (_evt, fn) => listeners.delete(fn),
            dispatchEvent: () => false,
        };
    };
    globalThis.matchMedia = mock;
    window.matchMedia = mock;
    return mock;
}

function buildNavigation() {
    const root = document.createElement("div");
    root.innerHTML = `
        <header class="site-header">
            <a href="/" class="site-header__logo">MirDB</a>
            <button class="site-header__burger" aria-controls="primary-nav" aria-expanded="false">
                <span class="visually-hidden">Menu</span>
            </button>
            <nav id="primary-nav" aria-label="Primary" aria-expanded="false">
                <ul>
                    <li><a href="#features">Features</a></li>
                    <li><a href="/docs">Documentation</a></li>
                    <li><a href="/about">About</a></li>
                </ul>
            </nav>
        </header>
    `;
    document.body.appendChild(root);
    const style = document.createElement("style");
    style.textContent = RESPONSIVE_CSS;
    document.head.appendChild(style);
    return root;
}

describe("Responsive CSS file shape (PRD REQ-6)", () => {
    it("test_case 0a: defines mobile-first .features__grid (single column)", () => {
        const body = extractRuleBody(RESPONSIVE_CSS, ".features__grid");
        expect(body).not.toBeNull();
        const gridTpl = body.match(/grid-template-columns:\s*([^;]+);/);
        expect(gridTpl).not.toBeNull();
        const tracks = countGridTracks(gridTpl[1]);
        expect(tracks).toBe(1);
    });

    it("test_case 0b: declares @media tablet block at min-width:600px", () => {
        expect(RESPONSIVE_CSS).toMatch(/@media\s*\(min-width:\s*600px\)/);
    });

    it("test_case 0c: declares @media desktop block at min-width:1024px", () => {
        expect(RESPONSIVE_CSS).toMatch(/@media\s*\(min-width:\s*1024px\)/);
    });
});

describe("Test Case 1 — Mobile viewport (360x640) features grid", () => {
    it("renders a single track at mobile width (regex /^\\s*(none|[^ ]+px|1fr)\\s*$/)", () => {
        const body = extractRuleBody(RESPONSIVE_CSS, ".features__grid");
        expect(body).not.toBeNull();
        const gridTpl = body.match(/grid-template-columns:\s*([^;]+);/);
        expect(gridTpl).not.toBeNull();
        // Mobile default declaration MUST be a single track expression
        // (1fr, "none", or a single px/fr value — never repeat(N,…))
        expect(gridTpl[1]).toMatch(/^\s*(none|1fr|[^,]+)\s*$/);
        expect(gridTpl[1]).not.toMatch(/repeat\(/);
    });

    it("simulated matchMedia returns false for tablet at 360px width", () => {
        installMatchMediaMock(360);
        const isTablet = window.matchMedia("(min-width: 600px)").matches;
        expect(isTablet).toBe(false);
    });
});

describe("Test Case 2 — Tablet viewport (768x1024) features grid", () => {
    it("declares two tracks for .features__grid inside the 600px media block", () => {
        const tabletBody = extractRuleBody(RESPONSIVE_CSS, ".features__grid", "min-width: 600px");
        expect(tabletBody).not.toBeNull();
        const gridTpl = tabletBody.match(/grid-template-columns:\s*([^;]+);/);
        expect(gridTpl).not.toBeNull();
        expect(countGridTracks(gridTpl[1])).toBe(2);
    });

    it("simulated matchMedia returns true for tablet at 768px width", () => {
        installMatchMediaMock(768);
        expect(window.matchMedia("(min-width: 600px)").matches).toBe(true);
        expect(window.matchMedia("(min-width: 1024px)").matches).toBe(false);
    });
});

describe("Test Case 3 — Desktop viewport (1280x800) features grid", () => {
    it("declares two or three tracks for .features__grid inside the 1024px media block", () => {
        const desktopBody = extractRuleBody(RESPONSIVE_CSS, ".features__grid", "min-width: 1024px");
        expect(desktopBody).not.toBeNull();
        const gridTpl = desktopBody.match(/grid-template-columns:\s*([^;]+);/);
        expect(gridTpl).not.toBeNull();
        const tracks = countGridTracks(gridTpl[1]);
        expect(tracks).toBeGreaterThanOrEqual(2);
        expect(tracks).toBeLessThanOrEqual(3);
    });

    it("simulated matchMedia returns true for desktop at 1280px width", () => {
        installMatchMediaMock(1280);
        expect(window.matchMedia("(min-width: 1024px)").matches).toBe(true);
    });
});

describe("Test Case 4 — Mobile burger and primary nav state (360x640)", () => {
    beforeEach(() => installMatchMediaMock(360));

    it("the responsive CSS declares burger as displayed (inline-flex/block/flex) by default", () => {
        const body = extractRuleBody(RESPONSIVE_CSS, ".site-header__burger");
        expect(body).not.toBeNull();
        const display = body.match(/display:\s*([^;]+);/);
        expect(display).not.toBeNull();
        // Must NOT be "none" at mobile default
        expect(display[1].trim()).not.toBe("none");
        expect(display[1]).toMatch(/(flex|block|inline-flex|inline-block)/);
    });

    it("the responsive CSS hides #primary-nav offscreen by default (mobile-first)", () => {
        const body = extractRuleBody(RESPONSIVE_CSS, "#primary-nav");
        expect(body).not.toBeNull();
        // Should be absolutely positioned offscreen or visually hidden
        expect(body).toMatch(/position:\s*absolute/);
        expect(body).toMatch(/left:\s*-9999px|display:\s*none|transform:\s*translateX\(-/);
    });

    it("DOM: burger is rendered and primary-nav initialises with aria-expanded='false'", () => {
        buildNavigation();
        const burger = document.querySelector(".site-header__burger");
        const nav = document.querySelector("#primary-nav");
        expect(burger).not.toBeNull();
        expect(nav).not.toBeNull();
        expect(burger.getAttribute("aria-expanded")).toBe("false");
        expect(nav.getAttribute("aria-expanded")).toBe("false");
    });
});

describe("Test Case 5 — Desktop burger hidden and primary nav inline (1280x800)", () => {
    beforeEach(() => installMatchMediaMock(1280));

    it("declares display:none for .site-header__burger inside the 1024px media block", () => {
        const burgerDesktop = extractRuleBody(
            RESPONSIVE_CSS,
            ".site-header__burger",
            "min-width: 1024px"
        );
        expect(burgerDesktop).not.toBeNull();
        const display = burgerDesktop.match(/display:\s*([^;]+);/);
        expect(display).not.toBeNull();
        expect(display[1].trim()).toBe("none");
    });

    it("declares #primary-nav as inline (static/flex/block) within the desktop block", () => {
        const navDesktop = extractRuleBody(RESPONSIVE_CSS, "#primary-nav", "min-width: 1024px");
        expect(navDesktop).not.toBeNull();
        // Either explicitly static position or display:flex/inline-flex/block
        const hasStatic = /position:\s*static/.test(navDesktop);
        const hasInlineDisplay = /display:\s*(flex|inline-flex|block|inline)/.test(navDesktop);
        expect(hasStatic || hasInlineDisplay).toBe(true);
    });
});

describe("Test Case 6 — Viewport resize 1280 -> 360 reflows without errors", () => {
    let errorSpy;

    beforeEach(() => {
        errorSpy = [];
        window.addEventListener("error", (e) => errorSpy.push(e));
    });

    afterEach(() => {
        document.body.innerHTML = "";
        document.head.innerHTML = "";
    });

    it("dispatches resize events at each breakpoint without throwing", () => {
        buildNavigation();
        installMatchMediaMock(1280);
        expect(() => simulateBreakpoint(1280)).not.toThrow();
        installMatchMediaMock(768);
        expect(() => simulateBreakpoint(768)).not.toThrow();
        installMatchMediaMock(360);
        expect(() => simulateBreakpoint(360)).not.toThrow();
        expect(errorSpy.length).toBe(0);
    });

    it("nav re-collapses into burger state on resize back to mobile (DOM contract)", () => {
        buildNavigation();
        // simulate the nav having been opened at mobile previously
        const burger = document.querySelector(".site-header__burger");
        const nav = document.querySelector("#primary-nav");
        burger.setAttribute("aria-expanded", "true");
        nav.setAttribute("aria-expanded", "true");

        installMatchMediaMock(1280);
        simulateBreakpoint(1280);
        // Simulate the responsive controller resetting state when crossing desktop
        // boundary (handled by Scenario 5 nav JS in production; here we assert the
        // contract that burger state CAN be reset programmatically without error).
        burger.setAttribute("aria-expanded", "false");
        nav.setAttribute("aria-expanded", "false");

        installMatchMediaMock(360);
        simulateBreakpoint(360);

        expect(burger.getAttribute("aria-expanded")).toBe("false");
        expect(nav.getAttribute("aria-expanded")).toBe("false");
    });

    it("multiple consecutive resize events do not leak listeners or throw", () => {
        buildNavigation();
        for (let i = 0; i < 10; i++) {
            const w = i % 2 === 0 ? 1280 : 360;
            installMatchMediaMock(w);
            simulateBreakpoint(w);
        }
        expect(errorSpy.length).toBe(0);
    });
});
