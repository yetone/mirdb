/* PrismJS - Minimal syntax highlighting */
/* This is a lightweight implementation for basic syntax highlighting */

(function() {
  'use strict';

  var Prism = {
    languages: {
      bash: {
        comment: /#.*/,
        string: /(["'])(?:\\.|(?!\1)[^\\\r\n])*\1/,
        keyword: /\b(?:if|then|else|elif|fi|for|while|do|done|case|esac|function|return|in)\b/,
        builtin: /\b(?:echo|cd|pwd|exit|export|source|alias|unalias|set|unset|read|declare|local|readonly|shift|wait|kill|jobs|bg|fg|disown|trap|eval|exec|ulimit|umask|getopts|hash|type|bind|help|let|shopt|caller|command|compgen|complete|compopt|mapfile|readarray|printf|test)\b/,
        variable: /\$(?:\w+|\{[^}]+\})/,
        function: /\b(?:cargo|git|npm|pip|telnet|curl|wget|make|sudo|apt|yum|brew)\b/
      },
      python: {
        comment: /#.*/,
        string: {
          pattern: /(?:[rub]|rb|br)?(?:"""[\s\S]*?"""|'''[\s\S]*?'''|"(?:\\.|[^"\\\r\n])*"|'(?:\\.|[^'\\\r\n])*')/i,
          greedy: true
        },
        keyword: /\b(?:and|as|assert|async|await|break|class|continue|def|del|elif|else|except|finally|for|from|global|if|import|in|is|lambda|nonlocal|not|or|pass|raise|return|try|while|with|yield)\b/,
        builtin: /\b(?:__import__|abs|all|any|apply|ascii|basestring|bin|bool|buffer|bytearray|bytes|callable|chr|classmethod|cmp|coerce|compile|complex|delattr|dict|dir|divmod|enumerate|eval|execfile|file|filter|float|format|frozenset|getattr|globals|hasattr|hash|help|hex|id|input|int|intern|isinstance|issubclass|iter|len|list|locals|long|map|max|memoryview|min|next|object|oct|open|ord|pow|print|property|range|raw_input|reduce|reload|repr|reversed|round|set|setattr|slice|sorted|staticmethod|str|sum|super|tuple|type|unichr|unicode|vars|xrange|zip)\b/,
        function: /\b\w+(?=\s*\()/,
        number: /\b(?:0[xX][\dA-Fa-f]+|0[oO][0-7]+|0[bB][01]+|\d+\.?\d*(?:[eE][+-]?\d+)?)\b/,
        operator: /[-+%=]=?|!=|\*\*?=?|\/\/?=?|<[<=>]?|>[=>]?|[&|^~]/,
        punctuation: /[{}[\];(),.:]/
      },
      toml: {
        comment: /#.*/,
        string: /"(?:\\.|[^"\\\r\n])*"|'[^'\r\n]*'/,
        property: /\b\w+(?=\s*=)/,
        number: /\b\d+\.?\d*\b/,
        boolean: /\b(?:true|false)\b/,
        punctuation: /[=\[\].,]/
      }
    },

    highlight: function(text, grammar) {
      var tokens = this.tokenize(text, grammar);
      return this.stringify(tokens);
    },

    tokenize: function(text, grammar) {
      var tokens = [text];

      for (var token in grammar) {
        if (!grammar.hasOwnProperty(token)) continue;

        var pattern = grammar[token];
        var regex = pattern.pattern || pattern;

        for (var i = 0; i < tokens.length; i++) {
          var str = tokens[i];

          if (typeof str !== 'string') continue;

          var match = regex.exec(str);
          if (!match) continue;

          var before = str.slice(0, match.index);
          var matched = match[0];
          var after = str.slice(match.index + matched.length);

          var newTokens = [];
          if (before) newTokens.push(before);
          newTokens.push({ type: token, content: matched });
          if (after) newTokens.push(after);

          tokens.splice.apply(tokens, [i, 1].concat(newTokens));
          i += newTokens.length - 1;
        }
      }

      return tokens;
    },

    stringify: function(tokens) {
      if (typeof tokens === 'string') {
        return this.escape(tokens);
      }

      if (Array.isArray(tokens)) {
        return tokens.map(this.stringify.bind(this)).join('');
      }

      return '<span class="token ' + tokens.type + '">' +
             this.escape(tokens.content) +
             '</span>';
    },

    escape: function(text) {
      return text
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;');
    },

    highlightAll: function() {
      var elements = document.querySelectorAll('code[class*="language-"], pre[class*="language-"]');

      elements.forEach(function(element) {
        var code = element.textContent;
        var language = (element.className.match(/language-(\w+)/) || [, 'bash'])[1];
        var grammar = Prism.languages[language] || Prism.languages.bash;

        element.innerHTML = Prism.highlight(code, grammar);
      });
    }
  };

  // Auto-run on DOMContentLoaded
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', Prism.highlightAll.bind(Prism));
  } else {
    Prism.highlightAll();
  }

  // Expose globally
  window.Prism = Prism;
})();
