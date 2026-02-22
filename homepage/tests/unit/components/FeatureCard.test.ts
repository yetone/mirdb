/**
 * Unit tests for FeatureCard component.
 * Owner: Scenario 3 - Features Section
 */
import { render, screen } from '@testing-library/svelte';
import { describe, it, expect } from 'vitest';
import FeatureCard from '$lib/components/ui/FeatureCard.svelte';

describe('FeatureCard', () => {
	it('renders icon, title, and description passed as props', () => {
		const props = {
			icon: '🚀',
			title: 'Test Feature',
			description: 'This is a test description for the feature.'
		};

		render(FeatureCard, { props });

		// Check icon is rendered
		expect(screen.getByText('🚀')).toBeInTheDocument();

		// Check title is rendered
		expect(screen.getByRole('heading', { name: 'Test Feature' })).toBeInTheDocument();

		// Check description is rendered
		expect(screen.getByText('This is a test description for the feature.')).toBeInTheDocument();
	});

	it('has proper semantic structure with article element', () => {
		const props = {
			icon: '💾',
			title: 'Storage Feature',
			description: 'Persistent storage description.'
		};

		render(FeatureCard, { props });

		const article = screen.getByRole('article');
		expect(article).toBeInTheDocument();
		expect(article).toHaveAttribute('data-testid', 'feature-card');
	});

	it('renders heading as h3 element', () => {
		const props = {
			icon: '🔄',
			title: 'Compaction Feature',
			description: 'Compaction description.'
		};

		render(FeatureCard, { props });

		const heading = screen.getByRole('heading', { level: 3 });
		expect(heading).toHaveTextContent('Compaction Feature');
	});

	it('renders different icons correctly', () => {
		const props = {
			icon: '🦀',
			title: 'Rust Feature',
			description: 'Rust implementation details.'
		};

		render(FeatureCard, { props });

		expect(screen.getByText('🦀')).toBeInTheDocument();
	});
});
