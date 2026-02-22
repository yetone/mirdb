/**
 * Shared type definitions for MirDB Homepage.
 *
 * This file is created by the first scenario builder and
 * should contain types used across multiple modules.
 */

export interface Feature {
	icon: string;
	title: string;
	description: string;
}

export interface RoadmapItem {
	title: string;
	completed: boolean;
}

export type Theme = 'light' | 'dark';

export interface CodeExample {
	language: string;
	code: string;
	description: string;
}

export interface NavItem {
	label: string;
	href: string;
	external?: boolean;
}
