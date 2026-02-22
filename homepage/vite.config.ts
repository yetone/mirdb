import { sveltekit } from '@sveltejs/kit/vite';
import { svelteTesting } from '@testing-library/svelte/vite';
import { defineConfig } from 'vitest/config';

export default defineConfig({
	plugins: [sveltekit(), svelteTesting()],
	build: {
		cssMinify: 'lightningcss',
		minify: 'esbuild',
		target: 'es2020',
		rollupOptions: {
			output: {
				manualChunks: undefined
			}
		}
	},
	test: {
		include: ['tests/unit/**/*.test.ts', 'tests/integration/**/*.test.ts'],
		exclude: ['tests/e2e/**'],
		environment: 'jsdom',
		globals: true,
		setupFiles: ['./tests/setup.ts']
	}
});
