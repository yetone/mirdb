<!--
  Usage Demo Section Component
  Owner: Scenario 10 - Usage Demo Section

  Displays:
  - Animated GIF or static demonstration of MirDB usage
  - Lazy-loaded for performance
  - Accessible description for screen readers

  Props:
  - animated: boolean (default true)

  REQ-8 traceability
-->
<script lang="ts">
	export let animated: boolean = true;
</script>

<section id="usage-demo" class="section usage-demo" aria-labelledby="usage-demo-heading">
	<div class="container">
		<h2 id="usage-demo-heading">Usage Demo</h2>
		<p class="demo-description">See MirDB in action with the Memcached protocol</p>
		<div class="demo-container">
			{#if animated}
				<img
					src="/assets/usage.gif"
					alt="Demonstration of MirDB usage showing Memcached protocol commands including set, get, and delete operations in a terminal session"
					loading="lazy"
					class="demo-media"
					aria-describedby="demo-description"
				/>
			{:else}
				<img
					src="/assets/usage.gif"
					alt="Demonstration of MirDB usage showing Memcached protocol commands including set, get, and delete operations in a terminal session"
					loading="lazy"
					class="demo-media demo-static"
					aria-describedby="demo-description"
				/>
			{/if}
			<p id="demo-description" class="sr-only">
				This demonstration shows a terminal session where MirDB is running as a server.
				The user connects using netcat and executes Memcached protocol commands:
				SET to store key-value pairs, GET to retrieve values, and DELETE to remove entries.
				The demo illustrates MirDB's full compatibility with the Memcached protocol.
			</p>
		</div>
	</div>
</section>

<style>
	.usage-demo {
		background-color: var(--color-surface);
		text-align: center;
	}

	.usage-demo h2 {
		margin-bottom: var(--spacing-md);
	}

	.demo-description {
		color: var(--color-text-secondary);
		margin-bottom: var(--spacing-xl);
		font-size: var(--font-size-lg);
	}

	.demo-container {
		max-width: 800px;
		margin: 0 auto;
		border-radius: var(--border-radius-lg);
		overflow: hidden;
		box-shadow: 0 4px 20px rgba(0, 0, 0, 0.15);
		background-color: var(--color-code-bg);
	}

	.demo-media {
		width: 100%;
		height: auto;
		display: block;
	}

	.demo-static {
		animation: none;
	}

	.sr-only {
		position: absolute;
		width: 1px;
		height: 1px;
		padding: 0;
		margin: -1px;
		overflow: hidden;
		clip: rect(0, 0, 0, 0);
		white-space: nowrap;
		border: 0;
	}

	@media (max-width: 768px) {
		.demo-container {
			border-radius: var(--border-radius-md);
		}

		.demo-description {
			font-size: var(--font-size-base);
		}
	}
</style>
