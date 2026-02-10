/**
 * robots.txt Unit Tests
 * Owner: Scenario 11 - SEO and Meta Tags
 *
 * Tests for validating robots.txt file:
 * - File exists
 * - Contains User-agent directive
 * - Allows crawling
 */

import { describe, it, expect, beforeAll } from 'vitest';
import * as fs from 'fs';
import * as path from 'path';

describe('robots.txt', () => {
  let robotsContent: string;

  beforeAll(() => {
    const robotsPath = path.resolve(__dirname, '../../../public/robots.txt');
    robotsContent = fs.readFileSync(robotsPath, 'utf-8');
  });

  it('should exist in public directory', () => {
    const robotsPath = path.resolve(__dirname, '../../../public/robots.txt');
    expect(fs.existsSync(robotsPath)).toBe(true);
  });

  it('should contain User-agent directive', () => {
    expect(robotsContent).toMatch(/User-agent:/i);
  });

  it('should allow all user agents with wildcard', () => {
    expect(robotsContent).toMatch(/User-agent:\s*\*/i);
  });

  it('should allow crawling', () => {
    expect(robotsContent).toMatch(/Allow:\s*\//i);
  });

  it('should not have Disallow: / (which would block everything)', () => {
    // Check that there's no "Disallow: /" that would block the entire site
    const lines = robotsContent.split('\n');
    const disallowRootLines = lines.filter(line =>
      line.trim().match(/^Disallow:\s*\/\s*$/i)
    );
    expect(disallowRootLines.length).toBe(0);
  });
});
