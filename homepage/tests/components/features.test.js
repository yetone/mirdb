import { describe, it, expect, vi } from "vitest";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, resolve } from "node:path";
import { mountComponent } from "../helpers/dom-helpers.js";
import { FEATURES } from "../../components/features/features.data.js";
import { renderFeatures, dedupeFeatures } from "../../components/features/features.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const FEATURES_CSS_PATH = resolve(__dirname, "../../components/features/features.css");

describe("Features data (PRD REQ-3 / US-2)", () => {
    it("test_case 1: contains 3 to 5 entries", () => {
        expect(FEATURES.length).toBeGreaterThanOrEqual(3);
        expect(FEATURES.length).toBeLessThanOrEqual(5);
    });

    it("test_case 2: every entry has non-empty id, title, description, icon strings", () => {
        const allValid = FEATURES.every(
            (f) =>
                typeof f.id === "string" &&
                f.id.length > 0 &&
                typeof f.title === "string" &&
                f.title.length > 0 &&
                typeof f.description === "string" &&
                f.description.length > 0 &&
                typeof f.icon === "string" &&
                f.icon.length > 0
        );
        expect(allValid).toBe(true);
    });

    it("test_case 5a: default FEATURES has no duplicate ids", () => {
        const idSet = new Set(FEATURES.map((f) => f.id));
        expect(idSet.size).toBe(FEATURES.length);
    });
});

describe("Features rendering", () => {
    it("test_case 3: rendered cards count matches FEATURES.length", async () => {
        const container = document.createElement("div");
        document.body.appendChild(container);
        await mountComponent("features", container);
        const grid = container.querySelector(".features__grid");
        renderFeatures(grid, FEATURES);
        const cards = document.querySelectorAll(".features .feature-card");
        expect(cards.length).toBe(FEATURES.length);
        document.body.removeChild(container);
    });

    it("test_case 4: each card has exactly one h3 and one description paragraph", async () => {
        const container = document.createElement("div");
        document.body.appendChild(container);
        await mountComponent("features", container);
        const grid = container.querySelector(".features__grid");
        renderFeatures(grid, FEATURES);
        const cards = document.querySelectorAll(".features .feature-card");
        expect(cards.length).toBeGreaterThan(0);
        cards.forEach((card) => {
            const headings = card.querySelectorAll("h3");
            const descriptions = card.querySelectorAll("p.feature-card__description");
            expect(headings.length).toBe(1);
            expect(descriptions.length).toBe(1);
            expect(headings[0].textContent.length).toBeGreaterThan(0);
            expect(descriptions[0].textContent.length).toBeGreaterThan(0);
        });
        document.body.removeChild(container);
    });

    it("each card also has an icon element", async () => {
        const container = document.createElement("div");
        document.body.appendChild(container);
        await mountComponent("features", container);
        const grid = container.querySelector(".features__grid");
        renderFeatures(grid, FEATURES);
        const cards = document.querySelectorAll(".feature-card");
        cards.forEach((card) => {
            const icon = card.querySelector(".feature-card__icon");
            expect(icon).not.toBeNull();
            expect(icon.innerHTML).toContain("<svg");
        });
        document.body.removeChild(container);
    });
});

describe("Features rendering — duplicate id handling (test_case 5)", () => {
    it("drops duplicate ids and warns", () => {
        const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});
        const dupes = [
            { id: "a", icon: "<svg/>", title: "A", description: "Alpha" },
            { id: "b", icon: "<svg/>", title: "B", description: "Beta" },
            { id: "a", icon: "<svg/>", title: "A2", description: "Alpha again" },
        ];
        const unique = dedupeFeatures(dupes);
        expect(unique.length).toBe(2);
        expect(new Set(unique.map((f) => f.id)).size).toBe(unique.length);
        expect(warnSpy).toHaveBeenCalledTimes(1);
        const message = warnSpy.mock.calls[0][0];
        expect(message).toMatch(/duplicate/i);
        warnSpy.mockRestore();
    });

    it("rendering with duplicates produces only unique cards", async () => {
        const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});
        const container = document.createElement("div");
        document.body.appendChild(container);
        await mountComponent("features", container);
        const grid = container.querySelector(".features__grid");
        const dupes = [
            { id: "speed", icon: "<svg/>", title: "Speed", description: "Fast." },
            { id: "speed", icon: "<svg/>", title: "Speed dup", description: "Dup." },
            { id: "durability", icon: "<svg/>", title: "Durable", description: "Safe." },
        ];
        const renderedCount = renderFeatures(grid, dupes);
        expect(renderedCount).toBe(2);
        const cards = document.querySelectorAll(".feature-card");
        expect(cards.length).toBe(2);
        const renderedIds = Array.from(cards).map((c) => c.getAttribute("data-feature-id"));
        expect(new Set(renderedIds).size).toBe(renderedIds.length);
        expect(warnSpy).toHaveBeenCalled();
        warnSpy.mockRestore();
        document.body.removeChild(container);
    });
});

describe("Features grid CSS declares grid-template-columns (test_case 6 — desktop)", () => {
    it("declares a default grid-template-columns", () => {
        const css = readFileSync(FEATURES_CSS_PATH, "utf8");
        const defaultDecl = css.match(/\.features__grid\s*\{[^}]*grid-template-columns:\s*([^;]+);/);
        expect(defaultDecl).not.toBeNull();
        expect(defaultDecl[1]).toMatch(/repeat\(/);
    });

    it("declares >= 2 tracks for the desktop breakpoint (>= 1024px)", () => {
        const css = readFileSync(FEATURES_CSS_PATH, "utf8");
        const desktopBlock = css.match(
            /@media\s*\(min-width:\s*1024px\)\s*\{[\s\S]*?\.features__grid\s*\{[^}]*grid-template-columns:\s*repeat\((\d+)\s*,/
        );
        expect(desktopBlock).not.toBeNull();
        const trackCount = Number(desktopBlock[1]);
        expect(trackCount).toBeGreaterThanOrEqual(2);
    });
});
