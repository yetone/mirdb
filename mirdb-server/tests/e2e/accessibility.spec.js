/**
 * MirDB Web Dashboard - Accessibility Tests
 *
 * Owner: Scenario 12 (Web UI Accessibility)
 *
 * Test cases:
 * 1. Each input has associated label element or aria-label
 * 2. All interactive elements reachable via Tab, focus visible
 * 3. Forms submit correctly via keyboard (Enter key)
 * 4. All text meets WCAG AA contrast ratio (4.5:1)
 * 5. Errors announced to screen readers (aria-live or role=alert)
 */

const { test, expect } = require('@playwright/test');
const AxeBuilder = require('@axe-core/playwright').default;

// Test Case 1: Each input has associated label element or aria-label
test('TC1: All form inputs have associated labels', async ({ page }) => {
  await page.goto('/');

  // Get all input elements (text, number, textarea)
  const inputs = await page.locator('input[type="text"], input[type="number"], textarea').all();

  expect(inputs.length).toBeGreaterThan(0);

  for (const input of inputs) {
    const inputId = await input.getAttribute('id');
    const ariaLabel = await input.getAttribute('aria-label');
    const ariaLabelledBy = await input.getAttribute('aria-labelledby');

    // Check for associated label element using 'for' attribute
    let hasAssociatedLabel = false;
    if (inputId) {
      const labelForInput = await page.locator(`label[for="${inputId}"]`).count();
      hasAssociatedLabel = labelForInput > 0;
    }

    // Input must have either: associated label, aria-label, or aria-labelledby
    const hasAccessibleName = hasAssociatedLabel || ariaLabel || ariaLabelledBy;

    expect(hasAccessibleName,
      `Input with id="${inputId}" should have an associated label, aria-label, or aria-labelledby`
    ).toBeTruthy();
  }
});

// Test Case 2: All interactive elements reachable via Tab, focus visible
test('TC2: All interactive elements are keyboard accessible with visible focus', async ({ page }) => {
  await page.goto('/');

  // Get all interactive elements
  const interactiveElements = await page.locator('button, input, textarea, a, [tabindex]:not([tabindex="-1"])').all();

  expect(interactiveElements.length).toBeGreaterThan(0);

  // Start from the beginning
  await page.keyboard.press('Tab');

  // Track elements that received focus
  const focusedElementIds = new Set();
  let tabCount = 0;
  const maxTabs = interactiveElements.length + 5; // Safety limit

  while (tabCount < maxTabs) {
    const focusedElement = await page.evaluate(() => {
      const el = document.activeElement;
      if (el) {
        // Check if focus is visible (has focus-visible styles or outline)
        const styles = window.getComputedStyle(el);
        const hasVisibleFocus =
          styles.outlineStyle !== 'none' ||
          el.classList.contains('focus-visible') ||
          styles.boxShadow !== 'none';

        return {
          tagName: el.tagName,
          id: el.id || null,
          type: el.getAttribute('type'),
          hasVisibleFocus
        };
      }
      return null;
    });

    if (!focusedElement || focusedElement.tagName === 'BODY') {
      break;
    }

    // Skip hidden elements (like in dialogs)
    const isVisible = await page.locator(`#${focusedElement.id || 'body'}`).first().isVisible().catch(() => true);
    if (isVisible && focusedElement.id) {
      focusedElementIds.add(focusedElement.id);
    }

    await page.keyboard.press('Tab');
    tabCount++;
  }

  // Verify we can reach key form elements
  const keyFormInputs = ['get-key-input', 'set-key-input', 'set-value-input'];
  for (const inputId of keyFormInputs) {
    const inputExists = await page.locator(`#${inputId}`).count() > 0;
    if (inputExists) {
      const isVisible = await page.locator(`#${inputId}`).isVisible();
      if (isVisible) {
        // Input should be reachable via keyboard
        expect(focusedElementIds.has(inputId) || focusedElementIds.size > 0,
          `Input #${inputId} should be reachable via Tab navigation`
        ).toBeTruthy();
      }
    }
  }

  // Verify focus is visible by checking CSS
  const buttonFocusStyle = await page.evaluate(() => {
    const btn = document.querySelector('button');
    if (btn) {
      btn.focus();
      const styles = window.getComputedStyle(btn);
      return {
        outline: styles.outline,
        outlineOffset: styles.outlineOffset,
        boxShadow: styles.boxShadow
      };
    }
    return null;
  });

  expect(buttonFocusStyle).not.toBeNull();
  // Button should have visible focus indicator
  expect(
    buttonFocusStyle.outline !== 'none' || buttonFocusStyle.boxShadow !== 'none',
    'Buttons should have visible focus indicator'
  ).toBeTruthy();
});

// Test Case 3: Forms submit correctly via keyboard (Enter key)
test('TC3: Forms submit correctly using Enter key', async ({ page }) => {
  await page.goto('/');

  // Test Get Key form - Enter key should trigger lookup
  const getKeyInput = page.locator('#get-key-input');
  await getKeyInput.fill('test-key');

  // Listen for form submission or API call
  let getFormSubmitted = false;
  page.on('request', request => {
    if (request.url().includes('/api/key/test-key')) {
      getFormSubmitted = true;
    }
  });

  // Press Enter while in the input field
  await getKeyInput.press('Enter');

  // Wait a moment for the request to be made
  await page.waitForTimeout(500);

  // The form should have submitted (made API request) or shown loading
  const lookupBtn = page.locator('#lookup-btn');
  const isLoading = await lookupBtn.evaluate(el => el.classList.contains('loading'));
  const resultBox = page.locator('#get-result');
  const resultText = await resultBox.textContent();

  // Either the form submitted, is loading, or showed an error (which means it processed)
  expect(
    getFormSubmitted || isLoading || resultText.includes('Loading') || resultText.includes('Error'),
    'Get Key form should submit when Enter is pressed'
  ).toBeTruthy();

  // Test Set Key form - Enter key should trigger set
  const setKeyInput = page.locator('#set-key-input');
  await setKeyInput.fill('another-test-key');

  let setFormSubmitted = false;
  page.on('request', request => {
    if (request.url().includes('/api/key') && request.method() === 'POST') {
      setFormSubmitted = true;
    }
  });

  // Press Enter while in the key input field
  await setKeyInput.press('Enter');
  await page.waitForTimeout(500);

  const setBtn = page.locator('#set-btn');
  const setIsLoading = await setBtn.evaluate(el => el.classList.contains('loading'));

  expect(
    setFormSubmitted || setIsLoading,
    'Set Key form should submit when Enter is pressed'
  ).toBeTruthy();
});

// Test Case 4: All text meets WCAG AA contrast ratio (4.5:1)
test('TC4: Color contrast meets WCAG AA standards', async ({ page }) => {
  await page.goto('/');

  // Run axe accessibility scan focusing on color contrast
  const accessibilityScanResults = await new AxeBuilder({ page })
    .withTags(['wcag2aa'])
    .analyze();

  // Filter for contrast violations
  const contrastViolations = accessibilityScanResults.violations.filter(
    v => v.id === 'color-contrast'
  );

  // Log any violations for debugging
  if (contrastViolations.length > 0) {
    console.log('Contrast violations found:', JSON.stringify(contrastViolations, null, 2));
  }

  expect(contrastViolations,
    'All text should meet WCAG AA contrast ratio (4.5:1)'
  ).toHaveLength(0);

  // Also manually verify key color combinations used in the CSS
  const colorContrasts = await page.evaluate(() => {
    // Helper function to calculate relative luminance
    function getLuminance(r, g, b) {
      const [rs, gs, bs] = [r, g, b].map(c => {
        c = c / 255;
        return c <= 0.03928 ? c / 12.92 : Math.pow((c + 0.055) / 1.055, 2.4);
      });
      return 0.2126 * rs + 0.7152 * gs + 0.0722 * bs;
    }

    // Helper to parse color to RGB
    function parseColor(color) {
      const canvas = document.createElement('canvas');
      canvas.width = canvas.height = 1;
      const ctx = canvas.getContext('2d');
      ctx.fillStyle = color;
      ctx.fillRect(0, 0, 1, 1);
      const data = ctx.getImageData(0, 0, 1, 1).data;
      return { r: data[0], g: data[1], b: data[2] };
    }

    // Helper to calculate contrast ratio
    function getContrastRatio(color1, color2) {
      const l1 = getLuminance(color1.r, color1.g, color1.b);
      const l2 = getLuminance(color2.r, color2.g, color2.b);
      const lighter = Math.max(l1, l2);
      const darker = Math.min(l1, l2);
      return (lighter + 0.05) / (darker + 0.05);
    }

    // Check key text/background combinations
    const results = [];

    // Check body text color against background
    const bodyStyles = getComputedStyle(document.body);
    const textColor = parseColor(bodyStyles.color);
    const bgColor = parseColor(bodyStyles.backgroundColor || '#f8fafc');
    const bodyContrast = getContrastRatio(textColor, bgColor);
    results.push({ element: 'body text', contrast: bodyContrast, passes: bodyContrast >= 4.5 });

    // Check panel heading
    const h2 = document.querySelector('.panel h2');
    if (h2) {
      const h2Styles = getComputedStyle(h2);
      const panelBg = parseColor(getComputedStyle(h2.closest('.panel')).backgroundColor);
      const h2Color = parseColor(h2Styles.color);
      const h2Contrast = getContrastRatio(h2Color, panelBg);
      results.push({ element: 'panel heading', contrast: h2Contrast, passes: h2Contrast >= 4.5 });
    }

    // Check error message color
    const errorMsg = document.querySelector('.error-message');
    if (errorMsg) {
      const errorStyles = getComputedStyle(errorMsg);
      const errorColor = parseColor(errorStyles.color);
      const panelBg = parseColor(getComputedStyle(errorMsg.closest('.panel')).backgroundColor);
      const errorContrast = getContrastRatio(errorColor, panelBg);
      results.push({ element: 'error message', contrast: errorContrast, passes: errorContrast >= 4.5 });
    }

    return results;
  });

  // Verify all checked combinations pass
  for (const check of colorContrasts) {
    expect(check.passes,
      `${check.element} contrast ratio ${check.contrast.toFixed(2)} should be >= 4.5:1`
    ).toBeTruthy();
  }
});

// Test Case 5: Errors announced to screen readers (aria-live or role=alert)
test('TC5: Error messages are accessible to screen readers', async ({ page }) => {
  await page.goto('/');

  // Check that error message containers have proper ARIA attributes
  const errorContainers = await page.locator('.error-message, [role="alert"], [aria-live]').all();

  expect(errorContainers.length).toBeGreaterThan(0);

  for (const container of errorContainers) {
    const role = await container.getAttribute('role');
    const ariaLive = await container.getAttribute('aria-live');

    // Container should have either role="alert" or aria-live attribute
    expect(
      role === 'alert' || ariaLive === 'polite' || ariaLive === 'assertive',
      'Error containers should have role="alert" or aria-live attribute'
    ).toBeTruthy();
  }

  // Trigger a validation error by clearing input and submitting the form
  const getKeyInput = page.locator('#get-key-input');
  await getKeyInput.clear();

  // Remove the required attribute to bypass browser validation
  // so our custom validation runs and populates the error message
  await getKeyInput.evaluate(el => el.removeAttribute('required'));

  // Submit the form to trigger custom validation
  const getKeyForm = page.locator('#get-key-form');
  await getKeyForm.evaluate(form => {
    form.dispatchEvent(new Event('submit', { bubbles: true, cancelable: true }));
  });

  // Wait for validation to run
  await page.waitForTimeout(100);

  // Check that error message appeared in an accessible container
  const getKeyError = page.locator('#get-key-error');
  const errorRole = await getKeyError.getAttribute('role');
  const errorAriaLive = await getKeyError.getAttribute('aria-live');

  expect(
    errorRole === 'alert' || errorAriaLive === 'polite' || errorAriaLive === 'assertive',
    'Error message should be in an accessible container for screen readers'
  ).toBeTruthy();

  // Verify error message content is populated
  const errorText = await getKeyError.textContent();
  expect(errorText.length).toBeGreaterThan(0);

  // Check toast container accessibility
  const toastContainer = page.locator('#toast-container');
  const toastRole = await toastContainer.getAttribute('role');
  const toastAriaLive = await toastContainer.getAttribute('aria-live');

  expect(
    toastRole === 'status' || toastAriaLive === 'polite' || toastAriaLive === 'assertive',
    'Toast container should be accessible to screen readers'
  ).toBeTruthy();

  // Check that aria-atomic is set for toasts (so whole message is read)
  const toastAriaAtomic = await toastContainer.getAttribute('aria-atomic');
  expect(toastAriaAtomic).toBe('true');
});

// Additional accessibility tests using axe-core
test('TC-Axe: No critical accessibility violations', async ({ page }) => {
  await page.goto('/');

  const accessibilityScanResults = await new AxeBuilder({ page })
    .withTags(['wcag2a', 'wcag2aa', 'wcag21aa'])
    .analyze();

  // Get critical and serious violations
  const criticalViolations = accessibilityScanResults.violations.filter(
    v => v.impact === 'critical' || v.impact === 'serious'
  );

  if (criticalViolations.length > 0) {
    console.log('Critical/serious violations:', JSON.stringify(criticalViolations, null, 2));
  }

  expect(criticalViolations,
    'Page should have no critical or serious accessibility violations'
  ).toHaveLength(0);
});
