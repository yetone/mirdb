import { sveltekit } from '@sveltejs/kit/vite';
import { defineConfig } from 'vitest/config';

export default defineConfig({
	plugins: [sveltekit()],
	test: {
		include: ['tests/unit/**/*.{test,spec}.{js,ts}', 'tests/integration/**/*.{test,spec}.{js,ts}'],
		exclude: ['tests/e2e/**/*'],
		environment: 'jsdom',
		globals: true,
		setupFiles: ['./tests/setup.ts'],
		alias: [
			{ find: /^svelte$/, replacement: 'svelte' }
		],
		server: {
			deps: {
				inline: [/^svelte/]
			}
		}
	},
	resolve: {
		conditions: ['browser']
	}
});
