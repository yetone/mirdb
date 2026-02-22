/**
 * Unit tests for Features section component.
 * Owner: Scenario 3 - Features Section
 */
import { render, screen } from '@testing-library/svelte';
import { describe, it, expect } from 'vitest';
import Features from '$lib/components/sections/Features.svelte';

describe('Features Section', () => {
	it('renders section with Features heading', () => {
		render(Features);

		const heading = screen.getByRole('heading', { name: /features/i });
		expect(heading).toBeInTheDocument();
		expect(heading).toHaveTextContent('Key Features');
	});

	it('renders exactly 6 FeatureCard components', () => {
		render(Features);

		const featureCards = screen.getAllByTestId('feature-card');
		expect(featureCards).toHaveLength(6);
	});

	it('contains Memcached Compatible feature', () => {
		render(Features);

		const memcachedTitle = screen.getByRole('heading', { name: /memcached/i });
		expect(memcachedTitle).toBeInTheDocument();
	});

	it('contains Persistent Storage feature explaining disk persistence', () => {
		render(Features);

		const persistentTitle = screen.getByRole('heading', { name: /persistent storage/i });
		expect(persistentTitle).toBeInTheDocument();

		// Check for disk persistence explanation
		expect(screen.getByText(/persists data to disk/i)).toBeInTheDocument();
	});

	it('contains LSM-Tree Architecture feature', () => {
		render(Features);

		const lsmTitle = screen.getByRole('heading', { name: /lsm-tree/i });
		expect(lsmTitle).toBeInTheDocument();

		// Check for LSM-tree explanation
		expect(screen.getByText(/log-structured merge-tree/i)).toBeInTheDocument();
	});

	it('contains Rust Implementation feature mentioning safety and performance', () => {
		render(Features);

		const rustTitle = screen.getByRole('heading', { name: /rust/i });
		expect(rustTitle).toBeInTheDocument();

		// Check for safety/performance mention
		expect(screen.getByText(/memory safety/i)).toBeInTheDocument();
	});

	it('contains Write-Ahead Logging feature explaining WAL for durability', () => {
		render(Features);

		const walTitle = screen.getByRole('heading', { name: /write-ahead logging/i });
		expect(walTitle).toBeInTheDocument();

		// Check for WAL/durability explanation
		expect(screen.getByText(/durability/i)).toBeInTheDocument();
	});

	it('contains Compaction Support feature explaining minor/major compaction', () => {
		render(Features);

		const compactionTitle = screen.getByRole('heading', { name: /compaction support/i });
		expect(compactionTitle).toBeInTheDocument();

		// Check for minor/major compaction explanation
		expect(screen.getByText(/minor and major compaction/i)).toBeInTheDocument();
	});

	it('has proper section with aria-labelledby for accessibility', () => {
		render(Features);

		const section = document.querySelector('section#features');
		expect(section).toBeInTheDocument();
		expect(section).toHaveAttribute('aria-labelledby', 'features-heading');
	});

	it('has features grid container', () => {
		render(Features);

		const gridContainer = document.querySelector('.features-grid');
		expect(gridContainer).toBeInTheDocument();
	});
});
