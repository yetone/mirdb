/**
 * Cross-Browser Compatibility tests - Scenario 12.
 *
 * Validates the homepage renders and behaves correctly across the three
 * browser engines required by PRD NFR-2:
 *   - chromium  -> Chrome + Edge (Blink)
 *   - webkit    -> Safari        (WebKit)
 *   - firefox   -> Firefox       (Gecko)
 *
 * Each test targets `tests/cross-browser/fixture.html`, a self-contained
 * page that mounts the real cta-signup, cta-login, and theme-toggle
 * components alongside a minimal hero block. The fixture is owned by
 * Scenario 12 so that this validation-only suite never edits another
 * scenario's source files (scaffold rule 4).
 */
import { test, expect } from "@playwright/test";

const FIXTURE_PATH = "/tests/cross-browser/fixture.html";

async function navigateAndCollect(page) {
    const consoleErrors = [];
    const pageErrors = [];

    page.on("console", (msg) => {
        if (msg.type() === "error") {
            consoleErrors.push(msg.text());
        }
    });
    page.on("pageerror", (err) => {
        pageErrors.push(err);
    });

    await page.goto(FIXTURE_PATH);
    await page.waitForFunction(
        () => document.documentElement.dataset.fixtureReady === "true",
        null,
        { timeout: 15_000 }
    );

    return { consoleErrors, pageErrors };
}

test.describe("Cross-Browser Compatibility (chromium / webkit / firefox)", () => {
    // -------------------- Smoke flow per engine (TC 1, 2, 3) --------------------
    test("smoke flow: hero renders, sign-up CTA navigates to /register, no console errors", async ({ page }, testInfo) => {
        const { consoleErrors, pageErrors } = await navigateAndCollect(page);

        // Hero heading must be visible
        const heading = page.locator(".hero h1").first();
        await expect(heading).toBeVisible();
        const text = (await heading.textContent()) || "";
        expect(text.trim().length).toBeGreaterThan(0);

        // Sign-Up CTA must navigate to /register
        const signupAnchor = page.locator('a[data-cta="signup"]').first();
        await expect(signupAnchor).toBeVisible();
        await signupAnchor.click();
        await page.waitForFunction(
            () => /\/register$/.test(window.location.pathname),
            null,
            { timeout: 5_000 }
        );
        expect(page.url().endsWith("/register")).toBe(true);

        // No console / page errors at any point
        expect(pageErrors, `pageerror events in ${testInfo.project.name}`).toEqual([]);
        const fatalConsole = consoleErrors.filter(text => !/favicon/i.test(text));
        expect(fatalConsole, `console errors in ${testInfo.project.name}`).toEqual([]);
    });

    // -------------------- Pageerror parity (TC 4) --------------------
    test("no pageerror events fire across smoke interactions", async ({ page }, testInfo) => {
        const { pageErrors } = await navigateAndCollect(page);

        // Interact with key surfaces to surface any engine-specific runtime errors
        const heading = page.locator(".hero h1").first();
        await expect(heading).toBeVisible();

        const themeToggle = page.locator(".theme-toggle").first();
        await expect(themeToggle).toBeVisible();
        await themeToggle.click();
        await themeToggle.click();

        const loginAnchor = page.locator('a[data-cta="login"]').first();
        if (await loginAnchor.count()) {
            await loginAnchor.click();
        }

        expect(pageErrors, `pageerror events in ${testInfo.project.name}`).toEqual([]);
    });

    // -------------------- Visual diff per engine (TC 5) --------------------
    test("visual snapshot is within 2% tolerance per engine", async ({ page }, testInfo) => {
        await navigateAndCollect(page);

        // Stabilise the page before screenshotting
        await page.evaluate(() => {
            document.documentElement.style.setProperty("--motion-duration", "0s");
        });
        await page.waitForLoadState("networkidle").catch(() => {});

        const screenshot = await page.screenshot({ fullPage: true, animations: "disabled" });
        expect(
            screenshot.length,
            `full-page screenshot should be non-empty in ${testInfo.project.name}`
        ).toBeGreaterThan(1024);

        // Engine-scoped baseline; tolerance honoured by playwright.config expect.toHaveScreenshot
        await expect(page).toHaveScreenshot(`homepage-${testInfo.project.name}.png`, {
            fullPage: true,
            maxDiffPixelRatio: 0.02,
            animations: "disabled",
        });
    });

    // -------------------- Theme toggle parity (TC 6) --------------------
    test("theme toggle flips dataset.theme and persists in localStorage", async ({ page }, testInfo) => {
        await page.goto(FIXTURE_PATH);
        // Reset persisted theme so each engine starts from the same baseline
        await page.evaluate(() => {
            try { localStorage.removeItem("theme"); } catch (_e) { /* noop */ }
        });
        await page.reload();
        await page.waitForFunction(
            () => document.documentElement.dataset.fixtureReady === "true",
            null,
            { timeout: 15_000 }
        );

        const themeToggle = page.locator(".theme-toggle").first();
        await expect(themeToggle).toBeVisible();

        const initialTheme = await page.evaluate(() => document.documentElement.dataset.theme);
        expect(["light", "dark"]).toContain(initialTheme);

        await themeToggle.click();
        const flippedTheme = await page.evaluate(() => document.documentElement.dataset.theme);
        expect(flippedTheme, `theme flipped in ${testInfo.project.name}`).not.toBe(initialTheme);
        expect(["light", "dark"]).toContain(flippedTheme);

        const stored = await page.evaluate(() => localStorage.getItem("theme"));
        expect(stored, `theme persisted in ${testInfo.project.name}`).toBe(flippedTheme);

        // Reload to confirm persistence
        await page.reload();
        await page.waitForFunction(
            () => document.documentElement.dataset.fixtureReady === "true",
            null,
            { timeout: 15_000 }
        );
        const afterReloadTheme = await page.evaluate(() => document.documentElement.dataset.theme);
        expect(afterReloadTheme, `theme survives reload in ${testInfo.project.name}`).toBe(flippedTheme);
    });
});
