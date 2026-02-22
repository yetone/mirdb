<!--
  Quick Start Section Component
  Owner: Scenario 4 - Quick Start Section

  Contains:
  - Installation instructions
  - Code examples for set, get, delete operations
  - Terminal-style code blocks with copy buttons

  Uses: CodeBlock, CopyButton components

  REQ-3 traceability
-->
<script lang="ts">
	import CodeBlock from '$lib/components/ui/CodeBlock.svelte';
</script>

<section id="quickstart" class="quickstart-section" data-testid="quickstart-section">
	<div class="container">
		<h2 class="section-title">Quick Start</h2>
		<p class="section-subtitle">Get MirDB running in minutes with these simple steps.</p>

		<div class="quickstart-content">
			<!-- Installation -->
			<div class="quickstart-step">
				<h3 class="step-title">1. Install MirDB</h3>
				<p class="step-description">Install using Cargo or clone from source:</p>
				<CodeBlock language="bash" code="cargo install mirdb" />
				<p class="step-description">Or clone the repository:</p>
				<CodeBlock language="bash" code="git clone https://github.com/yetone/mirdb.git && cd mirdb && cargo build --release" />
			</div>

			<!-- Start Server -->
			<div class="quickstart-step">
				<h3 class="step-title">2. Start the Server</h3>
				<p class="step-description">Launch the MirDB server:</p>
				<CodeBlock language="bash" code="mirdb-server" />
			</div>

			<!-- Usage Examples -->
			<div class="quickstart-step">
				<h3 class="step-title">3. Basic Operations</h3>
				<p class="step-description">
					MirDB uses the Memcached protocol, so you can use any Memcached client. Here are examples
					using netcat:
				</p>

				<!-- Set operation -->
				<h4 class="operation-title">Set a value</h4>
				<CodeBlock
					language="bash"
					code={`echo -e "set mykey 0 0 5\\r\\nvalue\\r\\n" | nc localhost 12333`}
				/>

				<!-- Get operation -->
				<h4 class="operation-title">Get a value</h4>
				<CodeBlock language="bash" code={`echo -e "get mykey\\r\\n" | nc localhost 12333`} />

				<!-- Delete operation -->
				<h4 class="operation-title">Delete a value</h4>
				<CodeBlock language="bash" code={`echo -e "delete mykey\\r\\n" | nc localhost 12333`} />
			</div>
		</div>
	</div>
</section>

<style>
	.quickstart-section {
		padding: var(--spacing-16) 0;
		background-color: var(--color-background);
	}

	.container {
		max-width: var(--container-max-width);
		margin: 0 auto;
		padding: 0 var(--spacing-4);
	}

	.section-title {
		font-size: var(--font-size-3xl);
		font-weight: 700;
		color: var(--color-text);
		text-align: center;
		margin-bottom: var(--spacing-4);
	}

	.section-subtitle {
		font-size: var(--font-size-lg);
		color: var(--color-text-muted);
		text-align: center;
		margin-bottom: var(--spacing-12);
		max-width: 600px;
		margin-left: auto;
		margin-right: auto;
	}

	.quickstart-content {
		max-width: 800px;
		margin: 0 auto;
	}

	.quickstart-step {
		margin-bottom: var(--spacing-12);
	}

	.step-title {
		font-size: var(--font-size-xl);
		font-weight: 600;
		color: var(--color-text);
		margin-bottom: var(--spacing-3);
	}

	.step-description {
		font-size: var(--font-size-base);
		color: var(--color-text-muted);
		margin-bottom: var(--spacing-4);
		line-height: 1.6;
	}

	.operation-title {
		font-size: var(--font-size-base);
		font-weight: 600;
		color: var(--color-text);
		margin-top: var(--spacing-6);
		margin-bottom: var(--spacing-2);
	}
</style>
