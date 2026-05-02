import { SerialFrameSelector, RunOptions, AxeResults } from 'axe-core';
import { Page } from 'playwright-core';

interface AxePlaywrightParams {
    page: Page;
    axeSource?: string;
}

declare class AxeBuilder {
    private page;
    private includes;
    private excludes;
    private option;
    private axeSource;
    private legacyMode;
    private errorUrl;
    constructor({ page, axeSource }: AxePlaywrightParams);
    include(selector: SerialFrameSelector): this;
    exclude(selector: SerialFrameSelector): this;
    options(options: RunOptions): this;
    withRules(rules: string | string[]): this;
    withTags(tags: string | string[]): this;
    disableRules(rules: string | string[]): this;
    setLegacyMode(legacyMode?: boolean): this;
    analyze(): Promise<AxeResults>;
    private inject;
    private script;
    private runLegacy;
    private runPartialRecursive;
    private finishRun;
    private axeConfigure;
}

export { AxeBuilder, AxeBuilder as default };
