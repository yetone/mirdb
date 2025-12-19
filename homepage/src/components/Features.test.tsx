import React from 'react';
import { describe, it, expect } from 'vitest';
import { render, screen, within } from '@testing-library/react';
import { Features, featuresData } from './Features';

describe('Features Section - E2E Tests', () => {
  describe('Test Case 1: Memcached Compatibility Feature', () => {
    it('displays Memcached protocol compatibility as a key feature', () => {
      render(<Features />);

      // Check for the features section
      const featuresSection = screen.getByTestId('features-section');
      expect(featuresSection).toBeInTheDocument();

      // Check for Memcached feature card
      const memcachedFeature = screen.getByTestId('feature-memcached');
      expect(memcachedFeature).toBeInTheDocument();

      // Verify it mentions Memcached protocol support
      expect(screen.getByText('Memcached Protocol Compatible')).toBeInTheDocument();
      expect(screen.getByText(/memcached clients/i)).toBeInTheDocument();
      expect(screen.getByText(/drop-in replacement/i)).toBeInTheDocument();
    });
  });

  describe('Test Case 2: Persistence Capability Feature', () => {
    it('displays data persistence as a key differentiator', () => {
      render(<Features />);

      // Check for persistence feature card
      const persistenceFeature = screen.getByTestId('feature-persistence');
      expect(persistenceFeature).toBeInTheDocument();

      // Verify it describes data persistence to disk
      expect(screen.getByText('Durable Persistence')).toBeInTheDocument();
      expect(screen.getByText(/persists/i)).toBeInTheDocument();
      expect(screen.getByText(/data survives restarts/i)).toBeInTheDocument();
    });
  });

  describe('Test Case 3: LSM-Tree Architecture Feature', () => {
    it('displays LSM-tree or SSTable architecture details', () => {
      render(<Features />);

      // Check for LSM-tree feature card
      const lsmTreeFeature = screen.getByTestId('feature-lsm-tree');
      expect(lsmTreeFeature).toBeInTheDocument();

      // Verify it mentions LSM-tree architecture
      expect(screen.getByText('LSM-Tree Architecture')).toBeInTheDocument();

      // Use getAllByText since LSM-tree appears in both title and description
      const lsmTreeMentions = screen.getAllByText(/LSM-tree/i);
      expect(lsmTreeMentions.length).toBeGreaterThanOrEqual(1);

      // Verify SSTable and compaction are mentioned (may appear in multiple features)
      const ssTableMentions = screen.getAllByText(/SSTable/i);
      expect(ssTableMentions.length).toBeGreaterThanOrEqual(1);

      const compactionMentions = screen.getAllByText(/compaction/i);
      expect(compactionMentions.length).toBeGreaterThanOrEqual(1);
    });
  });

  describe('Test Case 4: Feature Cards Have Icons', () => {
    it('each feature has an associated icon or visual indicator', () => {
      render(<Features />);

      // Check that each feature has an icon
      const memcachedIcon = screen.getByTestId('feature-memcached-icon');
      const persistenceIcon = screen.getByTestId('feature-persistence-icon');
      const lsmTreeIcon = screen.getByTestId('feature-lsm-tree-icon');

      expect(memcachedIcon).toBeInTheDocument();
      expect(persistenceIcon).toBeInTheDocument();
      expect(lsmTreeIcon).toBeInTheDocument();

      // Verify there are SVG icons present
      expect(screen.getByTestId('memcached-icon')).toBeInTheDocument();
      expect(screen.getByTestId('persistence-icon')).toBeInTheDocument();
      expect(screen.getByTestId('lsm-tree-icon')).toBeInTheDocument();
    });
  });

  describe('Features Section Structure', () => {
    it('has a features grid containing all feature cards', () => {
      render(<Features />);

      const featuresGrid = screen.getByTestId('features-grid');
      expect(featuresGrid).toBeInTheDocument();

      // Verify all three features are present
      const features = within(featuresGrid).getAllByText(/Compatible|Persistence|Architecture/);
      expect(features.length).toBeGreaterThanOrEqual(3);
    });

    it('has a section title', () => {
      render(<Features />);

      expect(screen.getByText('Key Features')).toBeInTheDocument();
    });

    it('features data contains correct number of features', () => {
      expect(featuresData).toHaveLength(3);
      expect(featuresData.map(f => f.id)).toEqual(['memcached', 'persistence', 'lsm-tree']);
    });
  });
});
