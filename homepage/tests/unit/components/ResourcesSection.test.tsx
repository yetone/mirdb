/**
 * Unit tests for ResourcesSection and Footer Components
 * Owner: Scenario 9 - Resource Links and Footer
 *
 * Tests cover:
 * - GitHub repository link presence
 * - Correct GitHub URL href
 * - External links target="_blank" attribute
 * - External links rel="noopener noreferrer" attribute
 * - Footer display at bottom of page
 * - Footer license information
 * - Footer CircleCI build link
 */

import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import { ResourcesSection } from '../../../src/components/sections/ResourcesSection';
import { Footer } from '../../../src/components/layout/Footer';

describe('ResourcesSection', () => {
  // Test Case 1: Section displays GitHub repository link
  it('displays GitHub repository link', () => {
    render(<ResourcesSection />);

    const githubLink = screen.getByTestId('resource-link-github');
    expect(githubLink).toBeInTheDocument();
    expect(githubLink).toHaveTextContent('GitHub Repository');
  });

  // Test Case 2: GitHub link href is https://github.com/yetone/mirdb
  it('has correct GitHub link href', () => {
    render(<ResourcesSection />);

    const githubLink = screen.getByTestId('resource-link-github');
    expect(githubLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb');
  });

  // Test Case 3: All external links have target='_blank' attribute
  it('all external links have target="_blank" attribute', () => {
    render(<ResourcesSection />);

    const githubLink = screen.getByTestId('resource-link-github');
    const docsLink = screen.getByTestId('resource-link-documentation');
    const communityLink = screen.getByTestId('resource-link-community');

    expect(githubLink).toHaveAttribute('target', '_blank');
    expect(docsLink).toHaveAttribute('target', '_blank');
    expect(communityLink).toHaveAttribute('target', '_blank');
  });

  // Test Case 4: All external links have rel='noopener noreferrer' attribute
  it('all external links have rel="noopener noreferrer" attribute', () => {
    render(<ResourcesSection />);

    const githubLink = screen.getByTestId('resource-link-github');
    const docsLink = screen.getByTestId('resource-link-documentation');
    const communityLink = screen.getByTestId('resource-link-community');

    expect(githubLink).toHaveAttribute('rel', 'noopener noreferrer');
    expect(docsLink).toHaveAttribute('rel', 'noopener noreferrer');
    expect(communityLink).toHaveAttribute('rel', 'noopener noreferrer');
  });

  // Test: Section has proper accessibility attributes
  it('has proper accessibility attributes', () => {
    render(<ResourcesSection />);

    const section = screen.getByTestId('resources-section');
    expect(section).toHaveAttribute('aria-labelledby', 'resources-heading');
    expect(section).toHaveAttribute('id', 'resources');
  });

  // Test: Section renders heading
  it('renders heading', () => {
    render(<ResourcesSection />);

    const heading = screen.getByTestId('resources-heading');
    expect(heading).toBeInTheDocument();
    expect(heading).toHaveTextContent('Resources');
  });

  // Test: Section renders resource grid
  it('renders resource grid with all links', () => {
    render(<ResourcesSection />);

    const grid = screen.getByTestId('resources-grid');
    expect(grid).toBeInTheDocument();

    // Check all resource links are present
    expect(screen.getByTestId('resource-link-github')).toBeInTheDocument();
    expect(screen.getByTestId('resource-link-documentation')).toBeInTheDocument();
    expect(screen.getByTestId('resource-link-community')).toBeInTheDocument();
  });

  // Test: Documentation link has correct href
  it('documentation link has correct href', () => {
    render(<ResourcesSection />);

    const docsLink = screen.getByTestId('resource-link-documentation');
    expect(docsLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb#readme');
  });

  // Test: Community link has correct href
  it('community link has correct href', () => {
    render(<ResourcesSection />);

    const communityLink = screen.getByTestId('resource-link-community');
    expect(communityLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb/discussions');
  });
});

describe('Footer', () => {
  // Test Case 6: Footer displays at bottom of page
  it('footer displays with proper structure', () => {
    render(<Footer />);

    const footer = screen.getByTestId('footer');
    expect(footer).toBeInTheDocument();
    expect(footer).toHaveAttribute('role', 'contentinfo');
  });

  // Test Case 7: Footer includes license information
  it('includes license information', () => {
    render(<Footer />);

    const license = screen.getByTestId('footer-license');
    expect(license).toBeInTheDocument();
    expect(license).toHaveTextContent('MIT License');
  });

  // Test Case 8: Footer includes CircleCI build link
  it('includes CircleCI build link', () => {
    render(<Footer />);

    const circleCiLink = screen.getByTestId('footer-circleci-link');
    expect(circleCiLink).toBeInTheDocument();
    expect(circleCiLink).toHaveAttribute('href', 'https://circleci.com/gh/yetone/mirdb');
    expect(circleCiLink).toHaveTextContent('CircleCI Build');
  });

  // Test: Footer GitHub link has correct attributes
  it('GitHub link has correct attributes', () => {
    render(<Footer />);

    const githubLink = screen.getByTestId('footer-github-link');
    expect(githubLink).toBeInTheDocument();
    expect(githubLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb');
    expect(githubLink).toHaveAttribute('target', '_blank');
    expect(githubLink).toHaveAttribute('rel', 'noopener noreferrer');
  });

  // Test: Footer brand name is displayed
  it('displays brand name', () => {
    render(<Footer />);

    const brand = screen.getByTestId('footer-brand');
    expect(brand).toBeInTheDocument();
    expect(brand).toHaveTextContent('MirDB');
  });

  // Test: Footer documentation link
  it('includes documentation link', () => {
    render(<Footer />);

    const docsLink = screen.getByTestId('footer-docs-link');
    expect(docsLink).toBeInTheDocument();
    expect(docsLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb#readme');
    expect(docsLink).toHaveAttribute('target', '_blank');
    expect(docsLink).toHaveAttribute('rel', 'noopener noreferrer');
  });

  // Test: Footer issues link
  it('includes issues link', () => {
    render(<Footer />);

    const issuesLink = screen.getByTestId('footer-issues-link');
    expect(issuesLink).toBeInTheDocument();
    expect(issuesLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb/issues');
    expect(issuesLink).toHaveAttribute('target', '_blank');
    expect(issuesLink).toHaveAttribute('rel', 'noopener noreferrer');
  });

  // Test: CircleCI link has correct security attributes
  it('CircleCI link has correct security attributes', () => {
    render(<Footer />);

    const circleCiLink = screen.getByTestId('footer-circleci-link');
    expect(circleCiLink).toHaveAttribute('target', '_blank');
    expect(circleCiLink).toHaveAttribute('rel', 'noopener noreferrer');
  });
});
