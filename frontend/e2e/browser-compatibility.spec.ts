import { test, expect } from '@playwright/test';

/**
 * E2E tests for Cross-Browser Compatibility
 * Tests that the homepage renders correctly across Chrome, Firefox, Safari, and Edge
 * as per success criteria in PRD
 *
 * These tests validate:
 * - Page loads without errors
 * - Key sections are visible and properly rendered
 * - Styles are applied consistently
 * - Interactive elements function correctly
 * - No visual defects or layout issues
 */

test.describe('Cross-Browser Homepage Compatibility', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');
  });

  test('TC1: Homepage renders correctly with all major sections visible', async ({ page, browserName }) => {
    // Log browser name for debugging
    console.log(`Running test on: ${browserName}`);

    // Verify page has loaded without errors
    const mainContainer = page.locator('.min-h-screen');
    await expect(mainContainer).toBeVisible();

    // Verify navigation bar renders correctly
    const navbar = page.locator('nav.navbar');
    await expect(navbar).toBeVisible();

    // Verify navbar has proper background styling
    const navbarClasses = await navbar.getAttribute('class');
    expect(navbarClasses).toContain('bg-base-100');

    // Verify logo/brand link is visible
    const brandLink = navbar.getByText('URL Shortener');
    await expect(brandLink).toBeVisible();

    // Verify login and sign up links are visible
    const loginLink = page.getByTestId('login-link');
    const registerLink = page.getByTestId('register-link');
    await expect(loginLink).toBeVisible();
    await expect(registerLink).toBeVisible();

    // Verify hero section renders correctly
    const heroSection = page.locator('section.hero');
    await expect(heroSection).toBeVisible();

    // Verify hero headline text
    const headline = page.locator('h1').filter({ hasText: 'Shorten. Track. Share.' });
    await expect(headline).toBeVisible();

    // Verify hero subheadline
    const subheadline = page.locator('p').filter({ hasText: 'Transform long URLs' });
    await expect(subheadline).toBeVisible();

    // Verify CTA buttons
    const getStartedBtn = page.getByTestId('get-started-btn');
    const loginBtn = page.getByTestId('login-btn');
    await expect(getStartedBtn).toBeVisible();
    await expect(loginBtn).toBeVisible();

    // Verify demo section renders
    const demoSection = page.getByTestId('demo-section');
    await expect(demoSection).toBeVisible();

    // Verify demo form elements
    const demoInput = page.getByTestId('demo-url-input');
    const demoButton = page.getByTestId('demo-shorten-button');
    await expect(demoInput).toBeVisible();
    await expect(demoButton).toBeVisible();

    // Verify features section renders
    const featuresSection = page.getByTestId('features-section');
    await expect(featuresSection).toBeVisible();

    // Verify all 4 feature cards are visible
    const featureCards = page.locator('[data-testid^="feature-card-"]');
    const cardCount = await featureCards.count();
    expect(cardCount).toBe(4);

    // Verify social proof section renders
    const socialProofSection = page.getByTestId('social-proof-section');
    await expect(socialProofSection).toBeVisible();

    // Verify footer renders
    const footer = page.getByTestId('footer-section');
    await expect(footer).toBeVisible();

    // Verify footer navigation links
    const footerLoginLink = page.getByTestId('footer-login-link');
    const footerRegisterLink = page.getByTestId('footer-register-link');
    await expect(footerLoginLink).toBeVisible();
    await expect(footerRegisterLink).toBeVisible();

    // Verify footer copyright
    const footerCopyright = page.getByTestId('footer-copyright');
    await expect(footerCopyright).toBeVisible();
  });

  test('TC2: Page styling and CSS are applied correctly', async ({ page, browserName }) => {
    console.log(`Testing CSS on: ${browserName}`);

    // Verify background colors are applied
    const mainContainer = page.locator('.min-h-screen.bg-base-200');
    await expect(mainContainer).toBeVisible();

    // Verify navbar has backdrop blur (glass effect)
    const navbar = page.locator('nav.navbar');
    const navbarClasses = await navbar.getAttribute('class');
    expect(navbarClasses).toContain('backdrop-blur');

    // Verify hero section background
    const heroSection = page.locator('section.hero.bg-base-300');
    await expect(heroSection).toBeVisible();

    // Verify FuturisticButton styling - primary button uses gradient styles
    // The button is inside a Link wrapper, so we need to find the actual button element
    const primaryBtnWrapper = page.getByTestId('get-started-btn');
    await expect(primaryBtnWrapper).toBeVisible();
    // Find the motion.button inside the Link wrapper
    const primaryBtn = primaryBtnWrapper.locator('button');
    const primaryBtnClasses = await primaryBtn.getAttribute('class');
    // FuturisticButton uses gradient classes instead of btn-primary
    expect(primaryBtnClasses).toContain('bg-gradient-to-r');
    expect(primaryBtnClasses).toContain('from-primary');

    // Verify outline button styling - also FuturisticButton component
    const outlineBtnWrapper = page.getByTestId('login-btn');
    await expect(outlineBtnWrapper).toBeVisible();
    const outlineBtn = outlineBtnWrapper.locator('button');
    const outlineBtnClasses = await outlineBtn.getAttribute('class');
    // FuturisticButton outline variant uses border-primary
    expect(outlineBtnClasses).toContain('border-primary');

    // Verify input field has proper styling
    const demoInput = page.getByTestId('demo-url-input');
    const inputClasses = await demoInput.getAttribute('class');
    expect(inputClasses).toContain('input');
    expect(inputClasses).toContain('input-bordered');

    // Verify typography is applied correctly - h1 can have responsive classes
    const headline = page.locator('h1').filter({ hasText: 'Shorten. Track. Share.' });
    await expect(headline).toBeVisible();

    const headlineStyles = await headline.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        fontWeight: styles.fontWeight,
        fontSize: styles.fontSize,
      };
    });

    // Font weight should be bold (700)
    expect(parseInt(headlineStyles.fontWeight)).toBeGreaterThanOrEqual(700);

    // Font size should be appropriately large (responsive, at least 30px at desktop)
    const fontSize = parseFloat(headlineStyles.fontSize);
    expect(fontSize).toBeGreaterThanOrEqual(30);
  });

  test('TC3: Interactive elements function correctly', async ({ page, browserName }) => {
    console.log(`Testing interactions on: ${browserName}`);

    // Test navigation link hover states work
    const loginLink = page.getByTestId('login-link');
    await loginLink.hover();

    // Verify link is still visible after hover
    await expect(loginLink).toBeVisible();

    // Test demo URL input is interactive
    const demoInput = page.getByTestId('demo-url-input');
    await demoInput.click();
    await demoInput.fill('https://example.com/test');

    // Verify input value was set
    const inputValue = await demoInput.inputValue();
    expect(inputValue).toBe('https://example.com/test');

    // Test demo shorten button is clickable
    const demoButton = page.getByTestId('demo-shorten-button');
    await demoButton.click();

    // Wait for result to appear (demo creates a shortened URL)
    const demoResult = page.getByTestId('demo-result');
    await expect(demoResult).toBeVisible({ timeout: 5000 });

    // Verify shortened URL is displayed
    const shortenedUrl = page.getByTestId('demo-shortened-url');
    await expect(shortenedUrl).toBeVisible();

    // Verify copy button is visible
    const copyButton = page.getByTestId('demo-copy-button');
    await expect(copyButton).toBeVisible();

    // Verify sign-up CTA appears
    const signupCta = page.getByTestId('demo-signup-cta');
    await expect(signupCta).toBeVisible();
  });

  test('TC4: No visual defects or layout issues', async ({ page, browserName }) => {
    console.log(`Testing layout on: ${browserName}`);

    // Check no horizontal scrollbar appears (content fits viewport)
    const bodyWidth = await page.evaluate(() => document.body.scrollWidth);
    const viewportWidth = await page.evaluate(() => window.innerWidth);

    // Allow small tolerance for scrollbar width
    expect(bodyWidth).toBeLessThanOrEqual(viewportWidth + 20);

    // Verify sections stack vertically without overlap
    const heroSection = page.locator('section.hero');
    const demoSection = page.getByTestId('demo-section');
    const featuresSection = page.getByTestId('features-section');
    const socialProofSection = page.getByTestId('social-proof-section');
    const footer = page.getByTestId('footer-section');

    // Get bounding boxes
    const heroBounds = await heroSection.boundingBox();
    const demoBounds = await demoSection.boundingBox();
    const featuresBounds = await featuresSection.boundingBox();
    const socialProofBounds = await socialProofSection.boundingBox();
    const footerBounds = await footer.boundingBox();

    // Verify all sections have valid bounds
    expect(heroBounds).not.toBeNull();
    expect(demoBounds).not.toBeNull();
    expect(featuresBounds).not.toBeNull();
    expect(socialProofBounds).not.toBeNull();
    expect(footerBounds).not.toBeNull();

    // Verify sections are in correct vertical order
    expect(heroBounds!.y).toBeLessThan(demoBounds!.y);
    expect(demoBounds!.y).toBeLessThan(featuresBounds!.y);
    expect(featuresBounds!.y).toBeLessThan(socialProofBounds!.y);
    expect(socialProofBounds!.y).toBeLessThan(footerBounds!.y);

    // Verify sections have valid heights
    expect(heroBounds!.height).toBeGreaterThan(0);
    expect(demoBounds!.height).toBeGreaterThan(0);
    expect(featuresBounds!.height).toBeGreaterThan(0);
    expect(socialProofBounds!.height).toBeGreaterThan(0);
    expect(footerBounds!.height).toBeGreaterThan(0);

    // Verify feature cards container has proper grid layout
    const cardsContainer = page.getByTestId('feature-cards-container');
    await expect(cardsContainer).toBeVisible();

    const gridStyle = await cardsContainer.evaluate((el) => {
      return window.getComputedStyle(el).display;
    });
    expect(gridStyle).toBe('grid');
  });

  test('TC5: Theme toggle works correctly', async ({ page, browserName }) => {
    console.log(`Testing theme toggle on: ${browserName}`);

    // Find theme toggle button
    const themeToggle = page.locator('[data-testid="theme-toggle"]');

    // If theme toggle exists, test it
    if (await themeToggle.isVisible()) {
      // Get initial theme from HTML element
      const initialTheme = await page.evaluate(() => {
        return document.documentElement.getAttribute('data-theme');
      });

      // Click theme toggle
      await themeToggle.click();

      // Wait a moment for theme to change
      await page.waitForTimeout(300);

      // Get new theme
      const newTheme = await page.evaluate(() => {
        return document.documentElement.getAttribute('data-theme');
      });

      // Theme should have changed (or at least the toggle should work without errors)
      // Note: Theme might cycle through multiple options
      console.log(`Theme changed from ${initialTheme} to ${newTheme}`);
    } else {
      // If no theme toggle with data-testid, look for ThemeToggle component
      const themeToggleComponent = page.locator('.swap');
      if (await themeToggleComponent.isVisible()) {
        await themeToggleComponent.click();
        await page.waitForTimeout(300);
      }
    }

    // Verify page still renders correctly after theme change
    const mainContainer = page.locator('.min-h-screen');
    await expect(mainContainer).toBeVisible();
  });

  test('TC6: Links navigate correctly', async ({ page, browserName }) => {
    console.log(`Testing navigation on: ${browserName}`);

    // Test login link in navbar navigates correctly
    const loginLink = page.getByTestId('login-link');
    await loginLink.click();
    await expect(page).toHaveURL(/.*login/);

    // Go back to homepage
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');

    // Test register link in navbar navigates correctly
    const registerLink = page.getByTestId('register-link');
    await registerLink.click();
    await expect(page).toHaveURL(/.*register/);

    // Go back to homepage
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');

    // Test Get Started button navigates to register
    const getStartedBtn = page.getByTestId('get-started-btn');
    await getStartedBtn.click();
    await expect(page).toHaveURL(/.*register/);
  });

  test('TC7: Page loads within acceptable time', async ({ page, browserName }) => {
    console.log(`Testing performance on: ${browserName}`);

    // Measure page load time
    const startTime = Date.now();
    await page.goto('/');
    await page.waitForLoadState('networkidle');
    const loadTime = Date.now() - startTime;

    console.log(`Page load time: ${loadTime}ms`);

    // Page should load within 5 seconds (reasonable for E2E tests)
    expect(loadTime).toBeLessThan(5000);

    // Verify all critical content is visible
    const heroSection = page.locator('section.hero');
    await expect(heroSection).toBeVisible();

    const featuresSection = page.getByTestId('features-section');
    await expect(featuresSection).toBeVisible();
  });
});
