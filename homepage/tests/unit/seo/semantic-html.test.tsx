/**
 * Semantic HTML Structure Tests
 * Owner: Scenario 11 - SEO and Meta Tags
 *
 * Tests for validating semantic HTML5 elements:
 * - header element is present
 * - main element is present
 * - nav element is present
 * - footer element is present
 */

import React from 'react';
import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import { Layout } from '@/components/layout/Layout';
import { ThemeProvider } from '@/context/ThemeContext';

const TestWrapper: React.FC<{ children: React.ReactNode }> = ({ children }) => (
  <ThemeProvider>{children}</ThemeProvider>
);

describe('Semantic HTML Structure', () => {
  describe('Layout Component', () => {
    it('should render a main element with id main-content', () => {
      render(
        <TestWrapper>
          <Layout>
            <div>Test Content</div>
          </Layout>
        </TestWrapper>
      );

      const main = document.querySelector('main#main-content');
      expect(main).toBeTruthy();
    });

    it('should render a header element', () => {
      render(
        <TestWrapper>
          <Layout>
            <div>Test Content</div>
          </Layout>
        </TestWrapper>
      );

      const header = document.querySelector('header');
      expect(header).toBeTruthy();
    });

    it('should render a footer element', () => {
      render(
        <TestWrapper>
          <Layout>
            <div>Test Content</div>
          </Layout>
        </TestWrapper>
      );

      const footer = document.querySelector('footer');
      expect(footer).toBeTruthy();
    });

    it('should render a nav element', () => {
      render(
        <TestWrapper>
          <Layout>
            <div>Test Content</div>
          </Layout>
        </TestWrapper>
      );

      const nav = document.querySelector('nav');
      expect(nav).toBeTruthy();
    });

    it('should have proper ARIA roles on semantic elements', () => {
      render(
        <TestWrapper>
          <Layout>
            <div>Test Content</div>
          </Layout>
        </TestWrapper>
      );

      const header = document.querySelector('header[role="banner"]');
      const footer = document.querySelector('footer[role="contentinfo"]');
      const nav = document.querySelector('nav[aria-label]');

      expect(header).toBeTruthy();
      expect(footer).toBeTruthy();
      expect(nav).toBeTruthy();
    });

    it('should have all semantic HTML5 elements (header, main, nav, footer)', () => {
      render(
        <TestWrapper>
          <Layout>
            <div>Test Content</div>
          </Layout>
        </TestWrapper>
      );

      const semanticElements = {
        header: document.querySelector('header'),
        main: document.querySelector('main'),
        nav: document.querySelector('nav'),
        footer: document.querySelector('footer'),
      };

      expect(semanticElements.header).toBeTruthy();
      expect(semanticElements.main).toBeTruthy();
      expect(semanticElements.nav).toBeTruthy();
      expect(semanticElements.footer).toBeTruthy();
    });
  });

  describe('Header Component', () => {
    it('should have banner role for accessibility', () => {
      render(
        <TestWrapper>
          <Layout>
            <div>Test Content</div>
          </Layout>
        </TestWrapper>
      );

      const header = document.querySelector('header');
      expect(header?.getAttribute('role')).toBe('banner');
    });
  });

  describe('Footer Component', () => {
    it('should have contentinfo role for accessibility', () => {
      render(
        <TestWrapper>
          <Layout>
            <div>Test Content</div>
          </Layout>
        </TestWrapper>
      );

      const footer = document.querySelector('footer');
      expect(footer?.getAttribute('role')).toBe('contentinfo');
    });
  });

  describe('Navigation Component', () => {
    it('should have aria-label for accessibility', () => {
      render(
        <TestWrapper>
          <Layout>
            <div>Test Content</div>
          </Layout>
        </TestWrapper>
      );

      const nav = document.querySelector('nav[aria-label="Main navigation"]');
      expect(nav).toBeTruthy();
    });
  });
});
