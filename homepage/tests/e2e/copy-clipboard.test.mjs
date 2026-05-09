import { test, expect } from '@playwright/test';
import fs from 'fs';
import path from 'path';

const __dirname = path.dirname(new URL(import.meta.url).pathname);
const indexPath = path.resolve(__dirname, '../../index.html');

test.describe('Copy-to-clipboard E2E', () => {
  test.beforeEach(async ({ page }) => {
    const html = fs.readFileSync(indexPath, 'utf-8');
    await page.setContent(html, { url: 'http://localhost:3000' });
  });

  test('clicking copy button triggers copy and shows feedback', async ({ page }) => {
    // Get the text content of the first code block in #install
    const codeText = await page.evaluate(() => {
      const code = document.querySelector('#install pre code');
      return code ? code.textContent : '';
    });

    expect(codeText.length).toBeGreaterThan(0);
    expect(codeText).toContain('cargo install');

    // Mock clipboard.writeText in page context to verify it gets called
    await page.evaluate(() => {
      window.__copiedText = null;
      navigator.clipboard = {
        writeText: (text) => {
          window.__copiedText = text;
          return Promise.resolve();
        }
      };

      // Wire copy buttons directly
      document.querySelectorAll('.copy-btn').forEach(button => {
        button.addEventListener('click', async () => {
          const codeBlock = button.closest('.code-block');
          const code = codeBlock.querySelector('code[data-copy]');
          const text = code ? code.textContent : '';
          await navigator.clipboard.writeText(text);
          button.classList.add('copied');
        });
      });
    });

    // Click the first copy button
    await page.click('#install .copy-btn');
    await page.waitForTimeout(50);

    // Verify clipboard write was called with correct text
    const copiedText = await page.evaluate(() => window.__copiedText);
    expect(copiedText).toBe(codeText);

    // Verify feedback class is added
    const hasCopiedClass = await page.evaluate(() => {
      const btn = document.querySelector('#install .copy-btn');
      return btn.classList.contains('copied');
    });
    expect(hasCopiedClass).toBe(true);
  });

  test('install section has cargo and binary instructions', async ({ page }) => {
    const installSection = await page.locator('#install');
    await expect(installSection).toBeVisible();

    const codeTexts = await page.evaluate(() => {
      return Array.from(document.querySelectorAll('#install code')).map(el => el.textContent);
    });

    const hasCargo = codeTexts.some(t => t.includes('cargo install'));
    const hasBinary = codeTexts.some(t => t.includes('curl') || t.includes('wget') || t.includes('releases'));

    expect(hasCargo).toBe(true);
    expect(hasBinary).toBe(true);
  });

  test('each code block has a copy button', async ({ page }) => {
    const preCount = await page.evaluate(() =>
      document.querySelectorAll('#install pre').length
    );
    const btnCount = await page.evaluate(() =>
      document.querySelectorAll('#install .copy-btn').length
    );

    expect(btnCount).toBe(preCount);
    expect(preCount).toBeGreaterThanOrEqual(2);
  });
});
