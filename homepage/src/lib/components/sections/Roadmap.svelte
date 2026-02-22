<!--
  Roadmap Section Component
  Owner: Scenario 6 - Roadmap Section

  Displays planned and completed features:
  - [x] Tokio with Memcached protocol
  - [x] Memtable with skiplist
  - [x] Minor compaction
  - [x] Major compaction
  - [ ] Raft consensus

  Props: none (uses constants)

  REQ-10 traceability
-->
<script lang="ts">
	import { ROADMAP_ITEMS } from '$lib/utils/constants';
</script>

<section id="roadmap" class="section roadmap" aria-label="Roadmap section">
	<div class="container">
		<h2>Roadmap</h2>
		<p class="roadmap-subtitle">Track our progress and upcoming features</p>
		<ul class="roadmap-list" role="list">
			{#each ROADMAP_ITEMS as item}
				<li class="roadmap-item" class:completed={item.completed} class:planned={!item.completed}>
					<span class="roadmap-status" aria-hidden="true">
						{#if item.completed}
							<svg class="checkmark" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="3">
								<polyline points="20 6 9 17 4 12"></polyline>
							</svg>
						{:else}
							<span class="pending-indicator"></span>
						{/if}
					</span>
					<span class="roadmap-title">
						{item.title}
					</span>
					<span class="sr-only">
						{item.completed ? '(completed)' : '(planned)'}
					</span>
				</li>
			{/each}
		</ul>
	</div>
</section>

<style>
	.roadmap {
		background-color: var(--color-surface);
	}

	.roadmap h2 {
		text-align: center;
		margin-bottom: var(--spacing-sm);
	}

	.roadmap-subtitle {
		text-align: center;
		color: var(--color-text-secondary);
		margin-bottom: var(--spacing-2xl);
	}

	.roadmap-list {
		list-style: none;
		max-width: 600px;
		margin: 0 auto;
		padding: 0;
	}

	.roadmap-item {
		display: flex;
		align-items: center;
		gap: var(--spacing-md);
		padding: var(--spacing-md) var(--spacing-lg);
		margin-bottom: var(--spacing-sm);
		background-color: var(--color-background);
		border-radius: var(--border-radius-lg);
		border: 1px solid var(--color-border);
		transition: all var(--transition-fast);
	}

	.roadmap-item:hover {
		box-shadow: var(--shadow-sm);
	}

	.roadmap-status {
		display: flex;
		align-items: center;
		justify-content: center;
		width: 24px;
		height: 24px;
		flex-shrink: 0;
	}

	.checkmark {
		width: 20px;
		height: 20px;
		color: var(--color-success);
	}

	.pending-indicator {
		width: 12px;
		height: 12px;
		border-radius: 50%;
		border: 2px solid var(--color-primary);
		background-color: transparent;
	}

	.roadmap-title {
		flex: 1;
		font-size: var(--font-size-lg);
	}

	.completed .roadmap-title {
		color: var(--color-text-secondary);
		text-decoration: line-through;
	}

	.planned .roadmap-title {
		color: var(--color-text);
		font-weight: 500;
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
		.roadmap-item {
			padding: var(--spacing-sm) var(--spacing-md);
		}

		.roadmap-title {
			font-size: var(--font-size-base);
		}
	}
</style>
