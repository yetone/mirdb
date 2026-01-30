/**
 * HTML Structure Unit Tests
 * Owner: Scenario 1 (base), other scenarios add their sections
 *
 * Tests:
 * - Semantic HTML validation
 * - Heading hierarchy
 * - Required elements presence
 * - Alt text on images
 */

import { test, describe } from 'node:test';
import assert from 'node:assert';
import { readFileSync } from 'fs';
import { join, dirname } from 'path';
import { fileURLToPath } from 'url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const htmlContent = readFileSync(join(__dirname, '../../index.html'), 'utf-8');

describe('HTML Structure', () => {
  describe('Hero Section', () => {
    test('hero section exists with proper id', () => {
      assert.match(htmlContent, /<section[^>]*id=["']hero["'][^>]*>/i);
    });

    test('hero section has aria-labelledby attribute', () => {
      assert.match(htmlContent, /<section[^>]*id=["']hero["'][^>]*aria-labelledby=["']hero-title["']/i);
    });

    test('h1 heading contains MirDB', () => {
      assert.match(htmlContent, /<h1[^>]*>MirDB<\/h1>/i);
    });

    test('tagline paragraph exists with correct text', () => {
      assert.match(htmlContent, /A Persistent Key-Value Store with Memcached Protocol/i);
    });

    test('logo image exists with src pointing to logo.gif', () => {
      assert.match(htmlContent, /<img[^>]*src=["'][^"']*logo\.gif["']/i);
    });

    test('logo image has descriptive alt text', () => {
      // Check that the logo img has a non-empty alt attribute
      const logoImgMatch = htmlContent.match(/<img[^>]*class=["'][^"']*hero__logo[^"']*["'][^>]*>/i);
      assert.ok(logoImgMatch, 'Logo image should exist with hero__logo class');

      const altMatch = logoImgMatch[0].match(/alt=["']([^"']+)["']/i);
      assert.ok(altMatch, 'Logo image should have alt attribute');
      assert.ok(altMatch[1].length > 10, 'Alt text should be descriptive (> 10 chars)');
      assert.match(altMatch[1].toLowerCase(), /mirdb/i, 'Alt text should mention MirDB');
    });

    test('logo image has loading="eager" for above fold content', () => {
      const logoImgMatch = htmlContent.match(/<img[^>]*class=["'][^"']*hero__logo[^"']*["'][^>]*>/i);
      assert.ok(logoImgMatch, 'Logo image should exist');
      assert.match(logoImgMatch[0], /loading=["']eager["']/i, 'Logo should have loading="eager"');
    });

    test('primary CTA button links to GitHub with security attributes', () => {
      assert.match(htmlContent, /href=["']https:\/\/github\.com\/yetone\/mirdb["']/i);
      assert.match(htmlContent, /target=["']_blank["']/i);
      assert.match(htmlContent, /rel=["']noopener noreferrer["']/i);
    });

    test('secondary CTA button links to usage section', () => {
      assert.match(htmlContent, /href=["']#usage["']/i);
    });

    test('CTA buttons have accessible labels', () => {
      assert.match(htmlContent, /aria-label=["'][^"']*GitHub[^"']*["']/i);
      assert.match(htmlContent, /aria-label=["'][^"']*[Gg]et [Ss]tarted[^"']*["']/i);
    });
  });

  describe('Document Structure', () => {
    test('document has proper doctype', () => {
      assert.match(htmlContent, /<!DOCTYPE html>/i);
    });

    test('html has lang attribute', () => {
      assert.match(htmlContent, /<html[^>]*lang=["']en["']/i);
    });

    test('document has meta viewport', () => {
      assert.match(htmlContent, /<meta[^>]*name=["']viewport["']/i);
    });

    test('document has meta description', () => {
      assert.match(htmlContent, /<meta[^>]*name=["']description["'][^>]*content=["'][^"']+["']/i);
    });

    test('document has title tag', () => {
      assert.match(htmlContent, /<title>[^<]+<\/title>/i);
    });

    test('skip link exists for accessibility', () => {
      assert.match(htmlContent, /class=["'][^"']*skip-link[^"']*["']/i);
      assert.match(htmlContent, /href=["']#main-content["']/i);
    });

    test('main element exists with id', () => {
      assert.match(htmlContent, /<main[^>]*id=["']main-content["']/i);
    });
  });
});
