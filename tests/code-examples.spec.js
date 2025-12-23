// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * E2E Tests for Code Examples Display
 * Scenario: Verify code examples demonstrate common operations (SET, GET, DELETE)
 * Related Requirements: REQ-4, US-7, NFR-6
 */

test.describe('Code Examples Display', () => {
  test.beforeEach(async ({ page }) => {
    // Navigate to the homepage before each test
    await page.goto('/');
  });

  /**
   * Test Case 1: Check for SET operation example
   * Expected: Code example showing SET command syntax is displayed
   */
  test('TC1: SET operation example is displayed', async ({ page }) => {
    // Navigate to code examples section
    const codeExamplesSection = page.locator('[data-testid="code-examples-section"]');
    await codeExamplesSection.scrollIntoViewIfNeeded();
    await expect(codeExamplesSection).toBeVisible();

    // Verify SET example card exists
    const setExample = page.locator('[data-testid="set-example"]');
    await expect(setExample).toBeVisible();

    // Verify SET example title
    const setTitle = setExample.locator('h3');
    await expect(setTitle).toContainText('SET');

    // Verify SET code block exists and contains SET command syntax
    const setCodeBlock = page.locator('[data-testid="set-code-block"]');
    await expect(setCodeBlock).toBeVisible();

    // Verify the code contains SET command
    const setCodeContent = await setCodeBlock.textContent();
    expect(setCodeContent.toLowerCase()).toContain('set');
    expect(setCodeContent).toContain('mykey');
    expect(setCodeContent).toContain('STORED');
  });

  /**
   * Test Case 2: Check for GET operation example
   * Expected: Code example showing GET command syntax is displayed
   */
  test('TC2: GET operation example is displayed', async ({ page }) => {
    // Navigate to code examples section
    const codeExamplesSection = page.locator('[data-testid="code-examples-section"]');
    await codeExamplesSection.scrollIntoViewIfNeeded();
    await expect(codeExamplesSection).toBeVisible();

    // Verify GET example card exists
    const getExample = page.locator('[data-testid="get-example"]');
    await expect(getExample).toBeVisible();

    // Verify GET example title
    const getTitle = getExample.locator('h3');
    await expect(getTitle).toContainText('GET');

    // Verify GET code block exists and contains GET command syntax
    const getCodeBlock = page.locator('[data-testid="get-code-block"]');
    await expect(getCodeBlock).toBeVisible();

    // Verify the code contains GET command
    const getCodeContent = await getCodeBlock.textContent();
    expect(getCodeContent.toLowerCase()).toContain('get');
    expect(getCodeContent).toContain('mykey');
    expect(getCodeContent).toContain('VALUE');
    expect(getCodeContent).toContain('END');
  });

  /**
   * Test Case 3: Check for DELETE operation example
   * Expected: Code example showing DELETE command syntax is displayed
   */
  test('TC3: DELETE operation example is displayed', async ({ page }) => {
    // Navigate to code examples section
    const codeExamplesSection = page.locator('[data-testid="code-examples-section"]');
    await codeExamplesSection.scrollIntoViewIfNeeded();
    await expect(codeExamplesSection).toBeVisible();

    // Verify DELETE example card exists
    const deleteExample = page.locator('[data-testid="delete-example"]');
    await expect(deleteExample).toBeVisible();

    // Verify DELETE example title
    const deleteTitle = deleteExample.locator('h3');
    await expect(deleteTitle).toContainText('DELETE');

    // Verify DELETE code block exists and contains DELETE command syntax
    const deleteCodeBlock = page.locator('[data-testid="delete-code-block"]');
    await expect(deleteCodeBlock).toBeVisible();

    // Verify the code contains DELETE command
    const deleteCodeContent = await deleteCodeBlock.textContent();
    expect(deleteCodeContent.toLowerCase()).toContain('delete');
    expect(deleteCodeContent).toContain('mykey');
    expect(deleteCodeContent).toContain('DELETED');
  });

  /**
   * Test Case 4: Verify syntax highlighting is applied
   * Expected: Code blocks have syntax highlighting with appropriate colors for language tokens
   */
  test('TC4: Syntax highlighting is applied to code blocks', async ({ page }) => {
    // Navigate to code examples section
    const codeExamplesSection = page.locator('[data-testid="code-examples-section"]');
    await codeExamplesSection.scrollIntoViewIfNeeded();

    // Check SET code block has syntax highlighting tokens
    const setCodeBlock = page.locator('[data-testid="set-code-block"]');
    await expect(setCodeBlock).toBeVisible();

    // Verify syntax highlighting tokens exist (comments, keywords, responses)
    const commentTokens = setCodeBlock.locator('.token.comment');
    const keywordTokens = setCodeBlock.locator('.token.keyword');
    const responseTokens = setCodeBlock.locator('.token.response');

    // Verify comment tokens exist and have appropriate styling
    await expect(commentTokens.first()).toBeVisible();
    const commentColor = await commentTokens.first().evaluate(el =>
      window.getComputedStyle(el).color
    );
    // Comment should have green color (rgb(106, 153, 85) = #6a9955)
    expect(commentColor).not.toBe('rgb(212, 212, 212)'); // Not default text color

    // Verify keyword tokens exist and have appropriate styling
    await expect(keywordTokens.first()).toBeVisible();
    const keywordColor = await keywordTokens.first().evaluate(el =>
      window.getComputedStyle(el).color
    );
    // Keyword should have blue color (rgb(86, 156, 214) = #569cd6)
    expect(keywordColor).not.toBe('rgb(212, 212, 212)'); // Not default text color

    // Verify response tokens exist and have appropriate styling
    await expect(responseTokens.first()).toBeVisible();
    const responseColor = await responseTokens.first().evaluate(el =>
      window.getComputedStyle(el).color
    );
    // Response should have teal color (rgb(78, 201, 176) = #4ec9b0)
    expect(responseColor).not.toBe('rgb(212, 212, 212)'); // Not default text color

    // Verify that syntax highlighting is applied across all code example cards
    const getCodeBlock = page.locator('[data-testid="get-code-block"]');
    const deleteCodeBlock = page.locator('[data-testid="delete-code-block"]');

    await expect(getCodeBlock.locator('.token.keyword').first()).toBeVisible();
    await expect(deleteCodeBlock.locator('.token.keyword').first()).toBeVisible();
  });

  /**
   * Test Case 5: Check for line numbers in multi-line examples
   * Expected: Multi-line code examples display line numbers
   */
  test('TC5: Line numbers are displayed in multi-line code examples', async ({ page }) => {
    // Navigate to code examples section
    const codeExamplesSection = page.locator('[data-testid="code-examples-section"]');
    await codeExamplesSection.scrollIntoViewIfNeeded();

    // Check SET code block (multi-line example)
    const setCodeBlock = page.locator('[data-testid="set-code-block"]');
    await expect(setCodeBlock).toBeVisible();

    // Verify the code block has the with-line-numbers class
    await expect(setCodeBlock).toHaveClass(/with-line-numbers/);

    // Verify line numbers are present
    const lineNumbers = setCodeBlock.locator('.line-number');
    const lineCount = await lineNumbers.count();

    // Should have multiple line numbers (SET example has 7 lines)
    expect(lineCount).toBeGreaterThanOrEqual(5);

    // Verify line numbers are sequential starting from 1
    const firstLineNumber = await lineNumbers.first().textContent();
    expect(firstLineNumber.trim()).toBe('1');

    // Verify line numbers have distinct styling (different from code content)
    const lineNumberColor = await lineNumbers.first().evaluate(el =>
      window.getComputedStyle(el).color
    );
    const codeContentColor = await setCodeBlock.locator('.line-content').first().evaluate(el =>
      window.getComputedStyle(el).color
    );

    // Line numbers should be visually distinct (typically muted/gray)
    expect(lineNumberColor).not.toBe(codeContentColor);

    // Verify other code blocks also have line numbers
    const getCodeBlock = page.locator('[data-testid="get-code-block"]');
    const deleteCodeBlock = page.locator('[data-testid="delete-code-block"]');

    await expect(getCodeBlock).toHaveClass(/with-line-numbers/);
    await expect(deleteCodeBlock).toHaveClass(/with-line-numbers/);

    const getLineNumbers = await getCodeBlock.locator('.line-number').count();
    const deleteLineNumbers = await deleteCodeBlock.locator('.line-number').count();

    expect(getLineNumbers).toBeGreaterThanOrEqual(5);
    expect(deleteLineNumbers).toBeGreaterThanOrEqual(3);
  });
});
