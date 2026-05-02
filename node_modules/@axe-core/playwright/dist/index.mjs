// src/index.ts
import assert from "assert";
import axe from "axe-core";

// src/utils.ts
var normalizeContext = (includes, excludes) => {
  const base = {
    exclude: [],
    include: []
  };
  if (excludes.length && Array.isArray(base.exclude)) {
    base.exclude.push(...excludes);
  }
  if (includes.length) {
    base.include = includes;
  }
  return base;
};
var analyzePage = ({
  context,
  options
}) => {
  const axeCore = window.axe;
  return axeCore.run(context || document, options || {}).then((results) => {
    return { error: null, results };
  }).catch((err) => {
    return { error: err.message, results: null };
  });
};

// src/browser.ts
var axeGetFrameContexts = ({
  context
}) => {
  return window.axe.utils.getFrameContexts(context);
};
var axeShadowSelect = ({
  frameSelector
}) => {
  return window.axe.utils.shadowSelect(frameSelector);
};
var axeRunPartial = ({
  context,
  options
}) => {
  return window.axe.runPartial(context, options);
};
var axeFinishRun = ({
  options
}) => {
  return window.axe.finishRun(JSON.parse(window.partialResults), options);
};
function chunkResultString(chunk) {
  if (!window.partialResults) {
    window.partialResults = "";
  }
  window.partialResults += chunk;
}

// src/AxePartialRunner.ts
var AxePartialRunner = class {
  constructor(partialPromise, initiator = false) {
    this.initiator = initiator;
    this.partialPromise = caught(partialPromise);
  }
  partialPromise;
  childRunners = [];
  addChildResults(childResultRunner) {
    this.childRunners.push(childResultRunner);
  }
  async getPartials() {
    try {
      const parentPartial = await this.partialPromise;
      const childPromises = this.childRunners.map((childRunner) => {
        return childRunner ? caught(childRunner.getPartials()) : [null];
      });
      const childPartials = (await Promise.all(childPromises)).flat(1);
      return [parentPartial, ...childPartials];
    } catch (e) {
      if (this.initiator) {
        throw e;
      }
      return [null];
    }
  }
};
var caught = /* @__PURE__ */ ((f) => {
  return (p) => (p.catch(f), p);
})(() => {
});

// src/index.ts
var { source } = axe;
var AxeBuilder = class {
  page;
  includes;
  excludes;
  option;
  axeSource;
  legacyMode = false;
  errorUrl;
  constructor({ page, axeSource }) {
    this.page = page;
    this.includes = [];
    this.excludes = [];
    this.option = {};
    this.axeSource = axeSource;
    this.errorUrl = "https://github.com/dequelabs/axe-core-npm/blob/develop/packages/playwright/error-handling.md";
  }
  /**
   * Selector to include in analysis.
   * This may be called any number of times.
   * @param String selector
   * @returns this
   */
  include(selector) {
    this.includes.push(selector);
    return this;
  }
  /**
   * Selector to exclude in analysis.
   * This may be called any number of times.
   * @param String selector
   * @returns this
   */
  exclude(selector) {
    this.excludes.push(selector);
    return this;
  }
  /**
   * Set options to be passed into axe-core
   * @param RunOptions options
   * @returns AxeBuilder
   */
  options(options) {
    this.option = options;
    return this;
  }
  /**
   * Limit analysis to only the specified rules.
   * Cannot be used with `AxeBuilder#withTags`
   * @param String|Array rules
   * @returns this
   */
  withRules(rules) {
    rules = Array.isArray(rules) ? rules : [rules];
    this.option = this.option || {};
    this.option.runOnly = {
      type: "rule",
      values: rules
    };
    return this;
  }
  /**
   * Limit analysis to only specified tags.
   * Cannot be used with `AxeBuilder#withRules`
   * @param String|Array tags
   * @returns this
   */
  withTags(tags) {
    tags = Array.isArray(tags) ? tags : [tags];
    this.option = this.option || {};
    this.option.runOnly = {
      type: "tag",
      values: tags
    };
    return this;
  }
  /**
   * Set the list of rules to skip when running an analysis.
   * @param String|Array rules
   * @returns this
   */
  disableRules(rules) {
    rules = Array.isArray(rules) ? rules : [rules];
    this.option = this.option || {};
    this.option.rules = {};
    for (const rule of rules) {
      this.option.rules[rule] = { enabled: false };
    }
    return this;
  }
  /**
   * Use frameMessenger with <same_origin_only>
   *
   * This disables use of axe.runPartial() which is called in each frame, and
   * axe.finishRun() which is called in a blank page. This uses axe.run() instead,
   * but with the restriction that cross-origin frames will not be tested.
   */
  setLegacyMode(legacyMode = true) {
    this.legacyMode = legacyMode;
    return this;
  }
  /**
   * Perform analysis and retrieve results. *Does not chain.*
   * @return Promise<Result | Error>
   */
  async analyze() {
    const context = normalizeContext(this.includes, this.excludes);
    const { page } = this;
    await page.evaluate(this.script());
    const runPartialDefined = await page.evaluate(
      'typeof window.axe.runPartial === "function"'
    );
    let results;
    if (!runPartialDefined || this.legacyMode) {
      results = await this.runLegacy(context);
      return results;
    }
    const partialResults = await this.runPartialRecursive(
      page.mainFrame(),
      context
    );
    const partials = await partialResults.getPartials();
    try {
      return await this.finishRun(partials);
    } catch (error) {
      throw new Error(
        `${error.message}
 Please check out ${this.errorUrl}`
      );
    }
  }
  /**
   * Injects `axe-core` into all frames.
   * @param Page - playwright page object
   * @returns Promise<void>
   */
  async inject(frames, shouldThrow) {
    for (const iframe of frames) {
      const race = new Promise((_, reject) => {
        setTimeout(() => {
          reject(new Error("Script Timeout"));
        }, 1e3);
      });
      const evaluate = iframe.evaluate(this.script());
      try {
        await Promise.race([evaluate, race]);
        await iframe.evaluate(await this.axeConfigure());
      } catch (err) {
        if (shouldThrow) {
          throw err;
        }
      }
    }
  }
  /**
   * Get axe-core source and configurations
   * @returns String
   */
  script() {
    return this.axeSource || source;
  }
  async runLegacy(context) {
    const frames = this.page.frames();
    await this.inject(frames);
    const axeResults = await this.page.evaluate(analyzePage, {
      context,
      options: this.option
    });
    if (axeResults.error) {
      throw new Error(axeResults.error);
    }
    return axeResults.results;
  }
  /**
   * Inject `axe-core` into each frame and run `axe.runPartial`.
   * Because we need to inject axe into all frames all at once
   * (to avoid any potential problems with the DOM becoming out-of-sync)
   * but also need to not process results for any child frames if the parent
   * frame throws an error (requirements of the data structure for `axe.finishRun`),
   *  we have to return a deeply nested array of Promises and then flatten
   * the array once all Promises have finished, throwing out any nested Promises
   * if the parent Promise is not fulfilled.
   * @param frame - playwright frame object
   * @param context - axe-core context object
   * @returns Promise<AxePartialRunner>
   */
  async runPartialRecursive(frame, context) {
    const frameContexts = await frame.evaluate(axeGetFrameContexts, {
      context
    });
    const partialPromise = frame.evaluate(axeRunPartial, {
      context,
      options: this.option
    });
    const initiator = frame === this.page.mainFrame();
    const axePartialRunner = new AxePartialRunner(partialPromise, initiator);
    for (const { frameSelector, frameContext } of frameContexts) {
      let childResults = null;
      try {
        const iframeHandle = await frame.evaluateHandle(axeShadowSelect, {
          frameSelector
        });
        const iframeElement = iframeHandle.asElement();
        const childFrame = await iframeElement.contentFrame();
        if (childFrame) {
          await this.inject([childFrame], true);
          childResults = await this.runPartialRecursive(
            childFrame,
            frameContext
          );
        }
      } catch {
      }
      axePartialRunner.addChildResults(childResults);
    }
    return axePartialRunner;
  }
  async finishRun(partialResults) {
    const { page, option: options } = this;
    const context = page.context();
    const blankPage = await context.newPage();
    assert(
      blankPage,
      "Please make sure that you have popup blockers disabled."
    );
    await blankPage.evaluate(this.script());
    await blankPage.evaluate(await this.axeConfigure());
    const sizeLimit = 6e7;
    const partialString = JSON.stringify(partialResults);
    async function chunkResults(result) {
      const chunk = result.substring(0, sizeLimit);
      await blankPage.evaluate(chunkResultString, chunk);
      if (result.length > sizeLimit) {
        return await chunkResults(result.substr(sizeLimit));
      }
      return;
    }
    await chunkResults(partialString);
    return await blankPage.evaluate(axeFinishRun, {
      options
    }).finally(async () => {
      await blankPage.close();
    });
  }
  async axeConfigure() {
    const hasRunPartial = await this.page.evaluate(
      'typeof window.axe?.runPartial === "function"'
    );
    return `
    ;axe.configure({
      ${!this.legacyMode && !hasRunPartial ? 'allowedOrigins: ["<unsafe_all_origins>"],' : 'allowedOrigins: ["<same_origin>"],'}
      branding: { application: 'playwright' }
    })
    `;
  }
};
export {
  AxeBuilder,
  AxeBuilder as default
};
