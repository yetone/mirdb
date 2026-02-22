<!--
  Code Block Component
  Owner: Scenario 4 - Quick Start Section

  Props:
  - code: string
  - language: string
  - showCopy: boolean (default true)

  Displays syntax-highlighted code with copy button
-->
<script lang="ts">
	import CopyButton from './CopyButton.svelte';

	export let code: string;
	export let language: string = 'bash';
	export let showCopy: boolean = true;
</script>

<div class="code-block" data-testid="code-block" data-language={language}>
	<div class="code-header">
		<span class="code-language">{language}</span>
		{#if showCopy}
			<CopyButton text={code} />
		{/if}
	</div>
	<pre class="code-pre"><code class="code-content language-{language}">{code}</code></pre>
</div>

<style>
	.code-block {
		background-color: #1e1e1e;
		border-radius: var(--radius-lg);
		overflow: hidden;
		font-family: var(--font-family-mono);
		margin: var(--spacing-4) 0;
	}

	.code-header {
		display: flex;
		justify-content: space-between;
		align-items: center;
		padding: var(--spacing-3) var(--spacing-4);
		background-color: #2d2d2d;
		border-bottom: 1px solid #3d3d3d;
	}

	.code-language {
		font-size: var(--font-size-xs);
		color: #808080;
		text-transform: uppercase;
		letter-spacing: 0.05em;
	}

	.code-header :global(.copy-button) {
		background-color: transparent;
		border-color: #4d4d4d;
		color: #a0a0a0;
	}

	.code-header :global(.copy-button:hover) {
		background-color: #3d3d3d;
		color: #ffffff;
		border-color: #6d6d6d;
	}

	.code-header :global(.copy-button.copied) {
		color: var(--color-success);
		border-color: var(--color-success);
	}

	.code-pre {
		margin: 0;
		padding: var(--spacing-4);
		overflow-x: auto;
	}

	.code-content {
		color: #d4d4d4;
		font-size: var(--font-size-sm);
		line-height: 1.6;
		white-space: pre;
		display: block;
	}

	/* Basic syntax highlighting for bash */
	.code-content.language-bash {
		color: #9cdcfe;
	}
</style>
