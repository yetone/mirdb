/**
 * Roadmap Section Component Tests
 * Owner: Scenario 6 - Roadmap Section
 *
 * Test Cases:
 * 1. Section contains 'Roadmap' heading
 * 2. Completed items for 'tokio', 'memtable/skiplist', 'minor compaction', 'major compaction' are marked complete
 * 3. Item for 'Raft consensus' exists and is not marked complete
 * 4. Completed items have checkmark/strikethrough, planned have different styling
 */

import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/svelte';
import Roadmap from '$lib/components/sections/Roadmap.svelte';

describe('Roadmap Component', () => {
	// Test Case 1: Section contains 'Roadmap' heading
	describe('Section Heading', () => {
		it('should render with Roadmap heading', () => {
			render(Roadmap);
			const heading = screen.getByRole('heading', { level: 2, name: 'Roadmap' });
			expect(heading).toBeDefined();
			expect(heading.textContent).toBe('Roadmap');
		});

		it('should have roadmap section with proper aria-label', () => {
			render(Roadmap);
			const roadmapSection = screen.getByRole('region', { name: 'Roadmap section' });
			expect(roadmapSection).toBeDefined();
		});

		it('should have a subtitle describing the section', () => {
			render(Roadmap);
			const subtitle = screen.getByText(/Track our progress/i);
			expect(subtitle).toBeDefined();
		});
	});

	// Test Case 2: Completed items are marked complete
	describe('Completed Roadmap Items', () => {
		it('should display Tokio with Memcached protocol as completed', () => {
			render(Roadmap);
			const item = screen.getByText('Tokio with Memcached protocol');
			expect(item).toBeDefined();
			const listItem = item.closest('li');
			expect(listItem?.classList.contains('completed')).toBe(true);
		});

		it('should display Memtable with skiplist as completed', () => {
			render(Roadmap);
			const item = screen.getByText('Memtable with skiplist');
			expect(item).toBeDefined();
			const listItem = item.closest('li');
			expect(listItem?.classList.contains('completed')).toBe(true);
		});

		it('should display Minor compaction as completed', () => {
			render(Roadmap);
			const item = screen.getByText('Minor compaction');
			expect(item).toBeDefined();
			const listItem = item.closest('li');
			expect(listItem?.classList.contains('completed')).toBe(true);
		});

		it('should display Major compaction as completed', () => {
			render(Roadmap);
			const item = screen.getByText('Major compaction');
			expect(item).toBeDefined();
			const listItem = item.closest('li');
			expect(listItem?.classList.contains('completed')).toBe(true);
		});

		it('should have checkmarks for completed items', () => {
			render(Roadmap);
			const checkmarks = document.querySelectorAll('.checkmark');
			expect(checkmarks.length).toBe(4);
		});
	});

	// Test Case 3: Raft consensus exists and is not marked complete
	describe('Planned Roadmap Items', () => {
		it('should display Raft consensus item', () => {
			render(Roadmap);
			const item = screen.getByText('Raft consensus');
			expect(item).toBeDefined();
		});

		it('should mark Raft consensus as planned (not completed)', () => {
			render(Roadmap);
			const item = screen.getByText('Raft consensus');
			const listItem = item.closest('li');
			expect(listItem?.classList.contains('planned')).toBe(true);
			expect(listItem?.classList.contains('completed')).toBe(false);
		});

		it('should have pending indicator for Raft consensus', () => {
			render(Roadmap);
			const pendingIndicators = document.querySelectorAll('.pending-indicator');
			expect(pendingIndicators.length).toBe(1);
		});
	});

	// Test Case 4: Visual distinction between completed and planned items
	describe('Visual Styling', () => {
		it('should have completed items with strikethrough styling class', () => {
			render(Roadmap);
			const completedItems = document.querySelectorAll('.completed');
			expect(completedItems.length).toBe(4);
		});

		it('should have planned items with different styling class', () => {
			render(Roadmap);
			const plannedItems = document.querySelectorAll('.planned');
			expect(plannedItems.length).toBe(1);
		});

		it('should display all 5 roadmap items', () => {
			render(Roadmap);
			const allItems = document.querySelectorAll('.roadmap-item');
			expect(allItems.length).toBe(5);
		});

		it('completed items should have text-decoration line-through class', () => {
			render(Roadmap);
			const completedItem = screen.getByText('Tokio with Memcached protocol');
			const listItem = completedItem.closest('li');
			expect(listItem?.classList.contains('completed')).toBe(true);
		});

		it('planned items should have different visual appearance', () => {
			render(Roadmap);
			const plannedItem = screen.getByText('Raft consensus');
			const listItem = plannedItem.closest('li');
			expect(listItem?.classList.contains('planned')).toBe(true);
		});
	});

	// Accessibility tests
	describe('Accessibility', () => {
		it('should have screen reader text indicating completion status', () => {
			render(Roadmap);
			const completedSrText = screen.getAllByText('(completed)');
			const plannedSrText = screen.getAllByText('(planned)');
			expect(completedSrText.length).toBe(4);
			expect(plannedSrText.length).toBe(1);
		});

		it('should use semantic list structure', () => {
			render(Roadmap);
			const list = screen.getByRole('list');
			expect(list).toBeDefined();
			const items = screen.getAllByRole('listitem');
			expect(items.length).toBe(5);
		});
	});
});
