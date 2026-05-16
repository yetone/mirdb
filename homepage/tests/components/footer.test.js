import { describe, it, expect, beforeEach, vi } from "vitest";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, resolve } from "node:path";
import { mountComponent } from "../helpers/dom-helpers.js";
import { initFooter, getFooterYear } from "../../components/footer/footer.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const FOOTER_CSS_PATH = resolve(__dirname, "../../components/footer/footer.css");

describe("Footer component (PRD REQ-9 / US-5)", () => {
    let container;

    beforeEach(async () => {
        container = document.createElement("div");
        container.setAttribute("data-component", "footer");
        document.body.appendChild(container);
        await mountComponent("footer", container);
    });

    // ---------- TC 1: unit — column count is 3–4 ----------
    it("renders between 3 and 4 .site-footer__column elements", () => {
        const columns = container.querySelectorAll(".site-footer__column");
        expect(columns.length).toBeGreaterThanOrEqual(3);
        expect(columns.length).toBeLessThanOrEqual(4);
    });

    // ---------- TC 2: unit — required links exist ----------
    it("contains Privacy, Terms, Contact, and at least one social link", () => {
        const anchors = container.querySelectorAll(".site-footer a");
        const hrefs = Array.from(anchors).map((a) => a.getAttribute("href") || "");
        const lowerHrefs = hrefs.map((h) => h.toLowerCase());

        const hasPrivacy = lowerHrefs.some((h) => h.includes("/privacy"));
        const hasTerms = lowerHrefs.some((h) => h.includes("/terms"));
        const hasContact = lowerHrefs.some((h) => h.includes("/contact") || h.startsWith("mailto:"));
        const hasSocial = lowerHrefs.some((h) => h.includes("github.com"));

        expect(hasPrivacy).toBe(true);
        expect(hasTerms).toBe(true);
        expect(hasContact).toBe(true);
        expect(hasSocial).toBe(true);
    });

    // ---------- TC 3: unit — dynamic year ----------
    it("copyright text contains the current year after initFooter", () => {
        initFooter(container);
        const copyright = container.querySelector(".site-footer__copyright");
        expect(copyright).not.toBeNull();
        const currentYear = new Date().getFullYear().toString();
        expect(copyright.textContent).toContain(currentYear);
    });

    // ---------- TC 4: e2e — Privacy footer link href ----------
    it("Privacy footer link has href='/privacy' for no-JS fallback and SPA navigation", () => {
        initFooter(container);
        const privacyLink = Array.from(container.querySelectorAll("a")).find((a) => {
            const href = (a.getAttribute("href") || "").toLowerCase();
            return href === "/privacy";
        });
        expect(privacyLink).not.toBeUndefined();
        expect(privacyLink.getAttribute("href")).toBe("/privacy");

        // Anchor has correct href so native browser navigation works without JS.
        // jsdom does not actually navigate, but the contract (href === '/privacy')
        // is what guarantees the UX.
    });

    // ---------- TC 5: negative — empty social links warns but column persists ----------
    it("renders social column and warns when no social links are present", () => {
        const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});

        // Create a container with empty social links
        const testContainer = document.createElement("div");
        testContainer.innerHTML = `
            <footer class="site-footer">
                <div class="site-footer__columns">
                    <div class="site-footer__column">
                        <h3>About</h3>
                    </div>
                    <div class="site-footer__column">
                        <h3>Product</h3>
                    </div>
                    <div class="site-footer__column">
                        <h3>Legal</h3>
                    </div>
                    <div class="site-footer__column">
                        <h3>Social</h3>
                        <ul class="site-footer__social-links"></ul>
                    </div>
                </div>
            </footer>
        `;
        document.body.appendChild(testContainer);

        initFooter(testContainer);

        // Column should not be omitted
        const columns = testContainer.querySelectorAll(".site-footer__column");
        expect(columns.length).toBe(4);

        // Warning should be logged
        expect(warnSpy).toHaveBeenCalledTimes(1);
        const message = warnSpy.mock.calls[0][0];
        expect(message).toMatch(/no social links/i);

        warnSpy.mockRestore();
        document.body.removeChild(testContainer);
    });
});

describe("Footer semantic structure", () => {
    let container;

    beforeEach(async () => {
        container = document.createElement("div");
        document.body.appendChild(container);
        await mountComponent("footer", container);
    });

    it("uses a semantic <footer> element", () => {
        const footer = container.querySelector("footer.site-footer");
        expect(footer).not.toBeNull();
        expect(footer.tagName).toBe("FOOTER");
    });

    it("has a copyright element with .site-footer__copyright class", () => {
        const copyright = container.querySelector(".site-footer__copyright");
        expect(copyright).not.toBeNull();
    });
});

describe("Footer CSS (desktop)", () => {
    it("declares a grid-template-columns for .site-footer__columns", () => {
        const css = readFileSync(FOOTER_CSS_PATH, "utf8");
        const hasGrid = css.match(/\.site-footer__columns\s*\{[^}]*grid-template-columns:/);
        expect(hasGrid).not.toBeNull();
    });

    it("declares 4 columns by default (desktop)", () => {
        const css = readFileSync(FOOTER_CSS_PATH, "utf8");
        const match = css.match(
            /\.site-footer__columns\s*\{[^}]*grid-template-columns:\s*repeat\((\d+)/
        );
        expect(match).not.toBeNull();
        expect(Number(match[1])).toBe(4);
    });
});

describe("getFooterYear utility", () => {
    it("returns the current year as a string", () => {
        const year = getFooterYear();
        expect(year).toBe(new Date().getFullYear().toString());
    });
});
