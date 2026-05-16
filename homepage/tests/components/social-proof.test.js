import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, resolve } from "node:path";
import { mountComponent } from "../helpers/dom-helpers.js";
import {
    renderSocialProof,
    hasContent,
    formatUserCount,
} from "../../components/social-proof/social-proof.js";
import {
    SOCIAL_PROOF_DATA,
    EMPTY_SOCIAL_PROOF_DATA,
} from "../../components/social-proof/social-proof.data.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const SOCIAL_PROOF_CSS_PATH = resolve(
    __dirname,
    "../../components/social-proof/social-proof.css"
);

async function mountSocialProof() {
    const container = document.createElement("div");
    container.setAttribute("data-component", "social-proof");
    document.body.appendChild(container);
    await mountComponent("social-proof", container);
    return {
        container,
        section: container.querySelector("section.social-proof"),
    };
}

describe("Social Proof component — testimonials (PRD REQ-8, TC 1)", () => {
    let mount;

    beforeEach(async () => {
        mount = await mountSocialProof();
    });

    afterEach(() => {
        if (mount.container.parentNode) {
            mount.container.parentNode.removeChild(mount.container);
        }
    });

    it("test_case 1: renders exactly 3 testimonials when supplied with 3", () => {
        const data = {
            testimonials: [
                { author: "A. Person", body: "Loved it." },
                { author: "B. Person", body: "Excellent." },
                { author: "C. Person", body: "Reliable." },
            ],
            badges: [],
            userCount: null,
        };
        renderSocialProof(mount.section, data);
        const items = document.querySelectorAll(".social-proof__testimonial");
        expect(items.length).toBe(3);
    });

    it("test_case 1: every testimonial has a child .author and .body element", () => {
        const data = {
            testimonials: [
                { author: "A. Person", body: "Loved it." },
                { author: "B. Person", body: "Excellent." },
                { author: "C. Person", body: "Reliable." },
            ],
            badges: [],
            userCount: null,
        };
        renderSocialProof(mount.section, data);
        const items = document.querySelectorAll(".social-proof__testimonial");
        expect(items.length).toBe(3);
        items.forEach((node) => {
            const author = node.querySelector(".author");
            const body = node.querySelector(".body");
            expect(author).not.toBeNull();
            expect(body).not.toBeNull();
            expect(author.textContent.length).toBeGreaterThan(0);
            expect(body.textContent.length).toBeGreaterThan(0);
        });
    });
});

describe("Social Proof component — badges (PRD REQ-8, TC 2)", () => {
    let mount;

    beforeEach(async () => {
        mount = await mountSocialProof();
    });

    afterEach(() => {
        if (mount.container.parentNode) {
            mount.container.parentNode.removeChild(mount.container);
        }
    });

    it("test_case 2: every rendered badge <img> has non-empty alt text", () => {
        const data = {
            testimonials: [],
            badges: [
                { src: "/logos/a.svg", alt: "Acme Corp" },
                { src: "/logos/b.svg", alt: "Beta Industries" },
                { src: "/logos/c.svg", alt: "Cosmos Cloud" },
            ],
            userCount: null,
        };
        renderSocialProof(mount.section, data);
        const imgs = document.querySelectorAll(".social-proof__badge img");
        expect(imgs.length).toBe(3);
        imgs.forEach((img) => {
            const alt = img.getAttribute("alt") || "";
            expect(alt.length).toBeGreaterThan(0);
        });
    });

    it("test_case 2: badge missing alt falls back to a non-empty default and warns", () => {
        const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});
        const data = {
            testimonials: [],
            badges: [{ src: "/logos/no-alt.svg" }],
            userCount: null,
        };
        renderSocialProof(mount.section, data);
        const img = document.querySelector(".social-proof__badge img");
        expect(img).not.toBeNull();
        const alt = img.getAttribute("alt") || "";
        expect(alt.length).toBeGreaterThan(0);
        expect(warnSpy).toHaveBeenCalledTimes(1);
        warnSpy.mockRestore();
    });
});

describe("Social Proof component — empty data behaviour (PRD REQ-8, TC 3)", () => {
    let mount;

    beforeEach(async () => {
        mount = await mountSocialProof();
    });

    afterEach(() => {
        if (mount.container.parentNode) {
            mount.container.parentNode.removeChild(mount.container);
        }
    });

    it("test_case 3: with fully-empty data the section is hidden (offsetHeight === 0)", () => {
        const result = renderSocialProof(mount.section, EMPTY_SOCIAL_PROOF_DATA);
        expect(result.rendered).toBe(false);
        expect(mount.section.hasAttribute("hidden")).toBe(true);
        // In jsdom offsetHeight is always 0 for nodes that aren't laid out;
        // we additionally check that the [hidden] attribute is present, which
        // browsers translate into display: none (offsetHeight === 0).
        expect(mount.section.offsetHeight).toBe(0);
    });

    it("test_case 3: empty render produces no placeholder testimonial cards", () => {
        renderSocialProof(mount.section, EMPTY_SOCIAL_PROOF_DATA);
        const testimonials = document.querySelectorAll(".social-proof__testimonial");
        const badges = document.querySelectorAll(".social-proof__badge");
        expect(testimonials.length).toBe(0);
        expect(badges.length).toBe(0);
    });

    it("test_case 3: empty userCount is not rendered as a numeric '0+ users' artifact", () => {
        renderSocialProof(mount.section, EMPTY_SOCIAL_PROOF_DATA);
        const userCount = mount.section.querySelector(".social-proof__user-count");
        expect(userCount).not.toBeNull();
        expect(userCount.textContent || "").toBe("");
        expect(userCount.getAttribute("data-empty")).toBe("true");
    });

    it("hides the section when only testimonials are empty but userCount is null", () => {
        const result = renderSocialProof(mount.section, {
            testimonials: [],
            badges: [],
            userCount: null,
        });
        expect(result.rendered).toBe(false);
        expect(mount.section.hasAttribute("hidden")).toBe(true);
    });
});

describe("Social Proof component — renders when populated (TC 4 contract)", () => {
    let mount;

    beforeEach(async () => {
        mount = await mountSocialProof();
    });

    afterEach(() => {
        if (mount.container.parentNode) {
            mount.container.parentNode.removeChild(mount.container);
        }
    });

    it("test_case 4: section is visible (not [hidden]) when populated with default data", () => {
        renderSocialProof(mount.section, SOCIAL_PROOF_DATA);
        expect(mount.section.hasAttribute("hidden")).toBe(false);
        expect(mount.section.getAttribute("aria-hidden")).toBeNull();
    });

    it("test_case 4: renders userCount, testimonials, and badges in a single populated call", () => {
        renderSocialProof(mount.section, SOCIAL_PROOF_DATA);
        const testimonials = document.querySelectorAll(".social-proof__testimonial");
        const badges = document.querySelectorAll(".social-proof__badge");
        const userCount = mount.section.querySelector(".social-proof__user-count");
        expect(testimonials.length).toBe(SOCIAL_PROOF_DATA.testimonials.length);
        expect(badges.length).toBe(SOCIAL_PROOF_DATA.badges.length);
        expect(userCount.textContent.length).toBeGreaterThan(0);
        expect(userCount.getAttribute("data-empty")).toBe("false");
    });

    it("test_case 4: dark-mode block exists in the CSS so badges and text remain legible", () => {
        const css = readFileSync(SOCIAL_PROOF_CSS_PATH, "utf8");
        expect(css).toMatch(/prefers-color-scheme:\s*dark/);
        expect(css).toMatch(/\[data-theme="dark"\]/);
        // The dark-mode block must override at least the background color.
        const darkPCS = css.match(
            /@media\s*\(prefers-color-scheme:\s*dark\)\s*\{[\s\S]*?\.social-proof\s*\{[^}]*background-color:/
        );
        expect(darkPCS).not.toBeNull();
    });

    it("test_case 4: testimonial text uses var(--color-text) so contrast follows the active theme", () => {
        const css = readFileSync(SOCIAL_PROOF_CSS_PATH, "utf8");
        const bodyBlock = css.match(
            /\.social-proof__testimonial\s+\.body\s*\{[^}]*color:\s*var\(--color-text/
        );
        expect(bodyBlock).not.toBeNull();
    });
});

describe("Social Proof component — semantic structure", () => {
    let mount;

    beforeEach(async () => {
        mount = await mountSocialProof();
    });

    afterEach(() => {
        if (mount.container.parentNode) {
            mount.container.parentNode.removeChild(mount.container);
        }
    });

    it("uses a single <section class='social-proof'> labelled by its heading", () => {
        const sections = mount.container.querySelectorAll("section.social-proof");
        expect(sections.length).toBe(1);
        const labelledBy = sections[0].getAttribute("aria-labelledby");
        expect(labelledBy).toBe("social-proof-heading");
        const heading = sections[0].querySelector(`#${labelledBy}`);
        expect(heading).not.toBeNull();
        expect(heading.tagName).toBe("H2");
    });

    it("ships hidden by default until renderSocialProof decides whether to show it", () => {
        // Markup straight from social-proof.html ships with `hidden`.
        expect(mount.section.hasAttribute("hidden")).toBe(true);
    });
});

describe("Social Proof helpers", () => {
    it("hasContent returns false for fully-empty data", () => {
        expect(hasContent({ testimonials: [], badges: [], userCount: null })).toBe(false);
    });

    it("hasContent returns true when there is a positive userCount", () => {
        expect(hasContent({ testimonials: [], badges: [], userCount: 42 })).toBe(true);
    });

    it("hasContent returns true when there is at least one testimonial", () => {
        expect(
            hasContent({
                testimonials: [{ author: "A", body: "B" }],
                badges: [],
                userCount: null,
            })
        ).toBe(true);
    });

    it("hasContent returns false for non-object input", () => {
        expect(hasContent(null)).toBe(false);
        expect(hasContent(undefined)).toBe(false);
    });

    it("formatUserCount renders thousands with K+ suffix", () => {
        expect(formatUserCount(2500)).toMatch(/2\.5K\+/);
        expect(formatUserCount(12000)).toMatch(/12K\+/);
    });

    it("formatUserCount renders millions with M+ suffix", () => {
        expect(formatUserCount(1_500_000)).toMatch(/1\.5M\+/);
    });

    it("formatUserCount renders small numbers verbatim", () => {
        expect(formatUserCount(42)).toMatch(/42\+ users/);
    });
});

describe("Social Proof data file contract", () => {
    it("default SOCIAL_PROOF_DATA has between 1 and 5 testimonials each with author + body", () => {
        expect(Array.isArray(SOCIAL_PROOF_DATA.testimonials)).toBe(true);
        expect(SOCIAL_PROOF_DATA.testimonials.length).toBeGreaterThanOrEqual(1);
        expect(SOCIAL_PROOF_DATA.testimonials.length).toBeLessThanOrEqual(5);
        for (const t of SOCIAL_PROOF_DATA.testimonials) {
            expect(typeof t.author).toBe("string");
            expect(t.author.length).toBeGreaterThan(0);
            expect(typeof t.body).toBe("string");
            expect(t.body.length).toBeGreaterThan(0);
        }
    });

    it("default SOCIAL_PROOF_DATA badges all carry non-empty alt text", () => {
        expect(Array.isArray(SOCIAL_PROOF_DATA.badges)).toBe(true);
        for (const b of SOCIAL_PROOF_DATA.badges) {
            expect(typeof b.alt).toBe("string");
            expect(b.alt.length).toBeGreaterThan(0);
        }
    });
});
