<script lang="ts">
	import { GITHUB_URL } from '$lib/utils/constants';

	const currentYear = new Date().getFullYear();
</script>

<footer class="footer">
	<div class="footer-container container">
		<div class="footer-content">
			<div class="footer-brand">
				<span class="footer-logo">MirDB</span>
				<p class="footer-tagline">A Persistent Key-Value Store with Memcached protocol</p>
			</div>

			<div class="footer-links">
				<a
					href={GITHUB_URL}
					target="_blank"
					rel="noopener noreferrer"
					class="footer-link"
					aria-label="View MirDB on GitHub (opens in new tab)"
				>
					<svg
						class="github-icon"
						viewBox="0 0 24 24"
						width="24"
						height="24"
						fill="currentColor"
						aria-hidden="true"
					>
						<path d="M12 0c-6.626 0-12 5.373-12 12 0 5.302 3.438 9.8 8.207 11.387.599.111.793-.261.793-.577v-2.234c-3.338.726-4.033-1.416-4.033-1.416-.546-1.387-1.333-1.756-1.333-1.756-1.089-.745.083-.729.083-.729 1.205.084 1.839 1.237 1.839 1.237 1.07 1.834 2.807 1.304 3.492.997.107-.775.418-1.305.762-1.604-2.665-.305-5.467-1.334-5.467-5.931 0-1.311.469-2.381 1.236-3.221-.124-.303-.535-1.524.117-3.176 0 0 1.008-.322 3.301 1.23.957-.266 1.983-.399 3.003-.404 1.02.005 2.047.138 3.006.404 2.291-1.552 3.297-1.23 3.297-1.23.653 1.653.242 2.874.118 3.176.77.84 1.235 1.911 1.235 3.221 0 4.609-2.807 5.624-5.479 5.921.43.372.823 1.102.823 2.222v3.293c0 .319.192.694.801.576 4.765-1.589 8.199-6.086 8.199-11.386 0-6.627-5.373-12-12-12z"/>
					</svg>
					<span>GitHub</span>
				</a>
			</div>
		</div>

		<div class="footer-bottom">
			<p class="copyright">
				&copy; {currentYear} MirDB. All rights reserved.
			</p>
		</div>
	</div>
</footer>

<style>
	.footer {
		background-color: var(--color-surface);
		border-top: 1px solid var(--color-border);
		padding: var(--spacing-2xl) 0;
		margin-top: auto;
	}

	.footer-container {
		display: flex;
		flex-direction: column;
		gap: var(--spacing-xl);
	}

	.footer-content {
		display: flex;
		justify-content: space-between;
		align-items: flex-start;
		flex-wrap: wrap;
		gap: var(--spacing-xl);
	}

	.footer-brand {
		max-width: 300px;
	}

	.footer-logo {
		font-size: var(--font-size-xl);
		font-weight: 700;
		color: var(--color-primary);
	}

	.footer-tagline {
		margin-top: var(--spacing-sm);
		color: var(--color-text-secondary);
		font-size: var(--font-size-sm);
	}

	.footer-links {
		display: flex;
		gap: var(--spacing-lg);
	}

	.footer-link {
		display: flex;
		align-items: center;
		gap: var(--spacing-sm);
		color: var(--color-text);
		text-decoration: none;
		font-weight: 500;
		transition: color var(--transition-fast);
	}

	.footer-link:hover {
		color: var(--color-primary);
	}

	.github-icon {
		width: 20px;
		height: 20px;
	}

	.footer-bottom {
		padding-top: var(--spacing-lg);
		border-top: 1px solid var(--color-border);
	}

	.copyright {
		margin: 0;
		color: var(--color-text-secondary);
		font-size: var(--font-size-sm);
		text-align: center;
	}

	@media (max-width: 768px) {
		.footer-content {
			flex-direction: column;
			align-items: center;
			text-align: center;
		}

		.footer-brand {
			max-width: 100%;
		}
	}
</style>
