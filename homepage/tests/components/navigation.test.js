import { describe, it, expect, beforeEach, vi } from "vitest";
import { mountComponent } from "../helpers/dom-helpers.js";
import { initNavigation } from "../../components/navigation/navigation.js";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, resolve } from "node:path";

const __dirname = dirname(fileURLToPath(import.meta.url));
const NAV_CSS_PATH = resolve(__dirname, "../../components/navigation/navigation.css");

describe("Navigation component (PRD REQ-7 / US-5)", () => {
    let container;

    beforeEach(async () => {
        container = document.createElement("div");
        container.setAttribute("data-component", "navigation");
        document.body.appendChild(container);
        await mountComponent("navigation", container);
    });

    function injectNavCss() {
        const css = readFileSync(NAV_CSS_PATH, "utf8");
        const style = document.createElement("style");
        style.textContent = css;
        document.head.appendChild(style);
        return style;
    }

    // ---------- TC 1: unit — logo rendering ----------
    it("renders a .site-header__logo anchor linking to '/' with 'MirDB' text", () => {
        const logo = document.querySelector(".site-header__logo");
        expect(logo).not.toBeNull();
        expect(logo.getAttribute("href")).toBe("/");

        const hasMirDBText = logo.textContent.trim() === "MirDB";
        const img = logo.querySelector("img");
        const hasMirDBImg = img !== null && img.getAttribute("alt") === "MirDB";

        expect(hasMirDBText || hasMirDBImg).toBe(true);
    });

    // ---------- TC 2: unit — primary nav links ----------
    it("renders primary nav links for Features, Documentation, and About", () => {
        const nav = document.getElementById("primary-nav");
        expect(nav).not.toBeNull();

        const links = Array.from(nav.querySelectorAll("ul li a"));
        const texts = links.map((a) => a.textContent.trim());

        expect(texts.some((t) => /features/i.test(t))).toBe(true);
        expect(texts.some((t) => /docs?|documentation/i.test(t))).toBe(true);
        expect(texts.some((t) => /about/i.test(t))).toBe(true);
    });

    // ---------- TC 3: e2e-like — mobile hamburger toggle ----------
    it("toggles aria-expanded and nav visibility when burger is clicked at mobile viewport", () => {
        injectNavCss();
        initNavigation();

        const burger = document.querySelector(".site-header__burger");
        const nav = document.getElementById("primary-nav");
        expect(burger).not.toBeNull();
        expect(nav).not.toBeNull();

        // Initial state
        expect(burger.getAttribute("aria-expanded")).toBe("false");
        expect(nav.classList.contains("is-open")).toBe(false);

        // Simulate mobile viewport
        Object.defineProperty(window, "innerWidth", { configurable: true, value: 360 });
        window.dispatchEvent(new Event("resize"));

        // Click to open
        burger.click();
        expect(burger.getAttribute("aria-expanded")).toBe("true");
        expect(nav.classList.contains("is-open")).toBe(true);

        // Click to close
        burger.click();
        expect(burger.getAttribute("aria-expanded")).toBe("false");
        expect(nav.classList.contains("is-open")).toBe(false);
    });

    // ---------- TC 4: e2e-like — Escape key closes menu ----------
    it("closes the menu when Escape is pressed while the menu is open", () => {
        initNavigation();

        const burger = document.querySelector(".site-header__burger");
        const nav = document.getElementById("primary-nav");

        // Open the menu
        burger.click();
        expect(burger.getAttribute("aria-expanded")).toBe("true");
        expect(nav.classList.contains("is-open")).toBe(true);

        // Press Escape
        const escEvent = new KeyboardEvent("keydown", { key: "Escape", bubbles: true });
        document.dispatchEvent(escEvent);

        expect(burger.getAttribute("aria-expanded")).toBe("false");
        expect(nav.classList.contains("is-open")).toBe(false);
    });

    // ---------- TC 5: e2e-like — outside click closes menu ----------
    it("closes the menu when clicking outside the header while the menu is open", () => {
        initNavigation();

        const burger = document.querySelector(".site-header__burger");
        const nav = document.getElementById("primary-nav");

        // Create a main element outside the header to click on
        const main = document.createElement("main");
        main.textContent = "Main content";
        document.body.appendChild(main);

        // Open the menu
        burger.click();
        expect(burger.getAttribute("aria-expanded")).toBe("true");
        expect(nav.classList.contains("is-open")).toBe(true);

        // Click outside
        const clickEvent = new MouseEvent("click", { bubbles: true });
        main.dispatchEvent(clickEvent);

        expect(burger.getAttribute("aria-expanded")).toBe("false");
        expect(nav.classList.contains("is-open")).toBe(false);

        document.body.removeChild(main);
    });

    // ---------- TC 6: unit — negative: invalid aria-controls ----------
    it("logs a warning and does not throw when aria-controls points to a non-existent id", () => {
        const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});

        const burger = document.querySelector(".site-header__burger");
        const originalControls = burger.getAttribute("aria-controls");

        // Point aria-controls to a non-existent id
        burger.setAttribute("aria-controls", "non-existent-nav-id");

        // Should not throw
        expect(() => initNavigation()).not.toThrow();

        // Should log a warning
        expect(warnSpy).toHaveBeenCalledTimes(1);
        const message = warnSpy.mock.calls[0][0];
        expect(message).toMatch(/non-existent-nav-id/);
        expect(message).toMatch(/does not match any element/i);

        // Toggle should be a no-op (clicking burger shouldn't crash)
        burger.click();
        // aria-expanded should remain unchanged since initNavigation returned early
        expect(burger.getAttribute("aria-expanded")).toBe("false");

        // Restore
        burger.setAttribute("aria-controls", originalControls);
        warnSpy.mockRestore();
    });

    it("does not close the menu when clicking inside the header", () => {
        initNavigation();

        const burger = document.querySelector(".site-header__burger");
        const nav = document.getElementById("primary-nav");
        const logo = document.querySelector(".site-header__logo");

        // Open the menu
        burger.click();
        expect(burger.getAttribute("aria-expanded")).toBe("true");

        // Click inside the header (on the logo)
        const clickEvent = new MouseEvent("click", { bubbles: true });
        logo.dispatchEvent(clickEvent);

        // Menu should still be open
        expect(burger.getAttribute("aria-expanded")).toBe("true");
        expect(nav.classList.contains("is-open")).toBe(true);
    });

    it("has a burger button with correct ARIA attributes", () => {
        const burger = document.querySelector(".site-header__burger");
        expect(burger).not.toBeNull();
        expect(burger.getAttribute("aria-controls")).toBe("primary-nav");
        expect(burger.getAttribute("aria-expanded")).toBe("false");
        expect(burger.getAttribute("aria-label")).toMatch(/toggle navigation/i);
    });

    it("has a nav element with id='primary-nav' and aria-label='Primary'", () => {
        const nav = document.getElementById("primary-nav");
        expect(nav).not.toBeNull();
        expect(nav.tagName).toBe("NAV");
        expect(nav.getAttribute("aria-label")).toBe("Primary");
    });
});

describe("Navigation initNavigation resilience", () => {
    it("returns early gracefully when no burger button exists", () => {
        document.body.innerHTML = "<div>No navigation here</div>";
        expect(() => initNavigation()).not.toThrow();
    });

    it("is idempotent — calling initNavigation twice does not double-register handlers", () => {
        const container = document.createElement("div");
        container.setAttribute("data-component", "navigation");
        document.body.appendChild(container);

        // Build minimal markup manually
        container.innerHTML = `
            <header class="site-header">
                <button class="site-header__burger" aria-controls="primary-nav" aria-expanded="false">Menu</button>
                <nav id="primary-nav"><ul><li><a href="/">Home</a></li></ul></nav>
            </header>
        `;

        initNavigation();
        initNavigation();

        const burger = document.querySelector(".site-header__burger");

        // Clicking twice should still toggle correctly (not get stuck)
        burger.click(); // open
        expect(burger.getAttribute("aria-expanded")).toBe("true");
        burger.click(); // close
        expect(burger.getAttribute("aria-expanded")).toBe("false");
    });
});
