/* PrismJS - Minimal syntax highlighting for bash, toml, and rust */
(function() {
  var Prism = {
    languages: {
      bash: {
        'comment': {
          pattern: /(^|[^\\])#.*/,
          lookbehind: true
        },
        'string': {
          pattern: /("|')(?:\\[\s\S]|(?!\1)[^\\])*\1/,
          greedy: true
        },
        'variable': /\$(?:\w+|[!@#$*?_-]|\{[^}]+\})/,
        'keyword': /\b(?:if|then|else|elif|fi|for|while|do|done|case|esac|function|in|select|until|return|exit|break|continue|readonly|export|unset|local|declare|typeset|source|alias)\b/,
        'builtin': /\b(?:cd|echo|printf|read|set|unset|export|alias|source|exit|return|true|false|test|eval|exec|shift|wait|trap|kill|type|hash|help|let|pwd|time|times|ulimit|umask|jobs|bg|fg|disown|suspend|logout|history|fc|bind|builtin|caller|command|compgen|complete|compopt|dirs|enable|getopts|mapfile|popd|pushd|readarray|shopt)\b/,
        'function': /\b(?:git|cargo|npm|node|python|pip|make|cmake|gcc|clang|rustc|go|java|mvn|gradle|docker|kubectl|aws|gcloud|curl|wget|tar|zip|unzip|ssh|scp|rsync|grep|sed|awk|find|cat|head|tail|less|more|vim|nano|emacs|ls|cp|mv|rm|mkdir|rmdir|chmod|chown|chgrp|ln|touch|stat|file|du|df|mount|umount|ps|top|htop|kill|pkill|killall|service|systemctl|journalctl|crontab|at|nohup|screen|tmux|man|info|apropos|which|whereis|locate|updatedb|xargs|tee|sort|uniq|wc|cut|paste|join|split|tr|nl|pr|fmt|fold|column|rev|comm|diff|patch|cmp|md5sum|sha1sum|sha256sum|base64|od|xxd|hexdump|strings|objdump|nm|readelf|ldd|strace|ltrace|gdb|valgrind|perf|telnet|nc|netcat|nmap|ping|traceroute|dig|nslookup|host|whois|ifconfig|ip|route|netstat|ss|iptables|firewalld|ufw|tcpdump|wireshark)\b/,
        'operator': /&&|\|\||[<>]=?|[!=]=|[-+*\/%]=?|[&|^~]|\b(?:and|or|not)\b/,
        'number': /\b\d+(?:\.\d+)?\b/,
        'punctuation': /[{}[\];(),.:]/
      },
      shell: 'bash',
      sh: 'bash',
      console: 'bash',
      toml: {
        'comment': {
          pattern: /#.*/,
          greedy: true
        },
        'string': {
          pattern: /"""[\s\S]*?"""|'''[\s\S]*?'''|"(?:\\.|[^\\"\r\n])*"|'[^'\r\n]*'/,
          greedy: true
        },
        'number': /(?:\b\d+(?:\.\d*)?|\B\.\d+)(?:[eE][+-]?\d+)?\b/,
        'boolean': /\b(?:true|false)\b/,
        'datetime': /\d{4}-\d{2}-\d{2}(?:[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})?)?/,
        'key': {
          pattern: /(?:[\w-]+|"(?:\\.|[^\\"\r\n])*"|'[^'\r\n]*')(?=\s*=)/,
          greedy: true
        },
        'table': {
          pattern: /\[+[^\[\]]+\]+/,
          inside: {
            'punctuation': /\[+|\]+/,
            'table-name': /[^\[\]]+/
          }
        },
        'punctuation': /[.,=[\]{}]/
      },
      rust: {
        'comment': [
          {
            pattern: /\/\*[\s\S]*?\*\//,
            greedy: true
          },
          {
            pattern: /\/\/.*/,
            greedy: true
          }
        ],
        'string': {
          pattern: /b?"(?:\\[\s\S]|[^\\"])*"|b?r#*"[\s\S]*?"#*/,
          greedy: true
        },
        'char': {
          pattern: /b?'(?:\\(?:x[0-7][\da-fA-F]|u\{[\da-fA-F]{1,6}\}|.)|[^\\\r\n\t'])'/,
          greedy: true
        },
        'attribute': {
          pattern: /#!?\[[\s\S]*?\]/,
          greedy: true,
          inside: {
            'string': null
          }
        },
        'keyword': /\b(?:abstract|as|async|await|become|box|break|const|continue|crate|do|dyn|else|enum|extern|false|final|fn|for|if|impl|in|let|loop|macro|match|mod|move|mut|override|priv|pub|ref|return|self|Self|static|struct|super|trait|true|try|type|typeof|union|unsafe|unsized|use|virtual|where|while|yield)\b/,
        'function': /\b[a-z_]\w*(?=\s*(?:::\s*<|\())/,
        'macro': {
          pattern: /\b\w+!/,
          alias: 'property'
        },
        'number': /\b(?:0x[\da-fA-F](?:_?[\da-fA-F])*|0o[0-7](?:_?[0-7])*|0b[01](?:_?[01])*|(?:\d(?:_?\d)*)?\.?\d(?:_?\d)*(?:[Ee][+-]?\d+)?)(?:_?(?:f32|f64|[iu](?:8|16|32|64|128|size)?))?\b/,
        'boolean': /\b(?:false|true)\b/,
        'punctuation': /->|\.\.=|\.{1,3}|::|[{}[\];(),:]/,
        'operator': /[-+*\/%!^]=?|=[=>]?|&[&=]?|\|[|=]?|<<?=?|>>?=?|[@?]/
      }
    },
    highlightAll: function() {
      var elements = document.querySelectorAll('code[class*="language-"], [class*="language-"] code');
      for (var i = 0; i < elements.length; i++) {
        this.highlightElement(elements[i]);
      }
    },
    highlightElement: function(element) {
      var language = this.getLanguage(element);
      if (!language || !this.languages[language]) return;

      var grammar = this.languages[language];
      if (typeof grammar === 'string') {
        grammar = this.languages[grammar];
      }
      if (!grammar) return;

      var code = element.textContent;
      var highlighted = this.highlight(code, grammar, language);
      element.innerHTML = highlighted;
    },
    getLanguage: function(element) {
      var match = element.className.match(/(?:^|\s)lang(?:uage)?-(\w+)(?:\s|$)/i);
      return match ? match[1].toLowerCase() : null;
    },
    highlight: function(code, grammar, language) {
      var tokens = this.tokenize(code, grammar);
      return this.stringify(tokens, language);
    },
    tokenize: function(code, grammar) {
      var tokens = [code];
      for (var token in grammar) {
        if (!grammar.hasOwnProperty(token)) continue;
        var pattern = grammar[token];
        var patterns = Array.isArray(pattern) ? pattern : [pattern];

        for (var j = 0; j < patterns.length; j++) {
          var patternObj = patterns[j];
          var regex = patternObj.pattern || patternObj;
          if (!(regex instanceof RegExp)) continue;

          var flags = regex.flags || '';
          if (flags.indexOf('g') === -1) {
            regex = new RegExp(regex.source, flags + 'g');
          }

          for (var i = 0; i < tokens.length; i++) {
            var str = tokens[i];
            if (typeof str !== 'string') continue;

            var match;
            var newTokens = [];
            var lastIndex = 0;
            regex.lastIndex = 0;

            while ((match = regex.exec(str)) !== null) {
              if (match.index > lastIndex) {
                newTokens.push(str.slice(lastIndex, match.index));
              }
              newTokens.push({
                type: token,
                content: match[0],
                alias: patternObj.alias
              });
              lastIndex = regex.lastIndex;
            }

            if (lastIndex < str.length) {
              newTokens.push(str.slice(lastIndex));
            }

            if (newTokens.length > 0) {
              tokens.splice.apply(tokens, [i, 1].concat(newTokens));
              i += newTokens.length - 1;
            }
          }
        }
      }
      return tokens;
    },
    stringify: function(tokens, language) {
      var result = '';
      for (var i = 0; i < tokens.length; i++) {
        var token = tokens[i];
        if (typeof token === 'string') {
          result += this.encode(token);
        } else {
          var classes = 'token ' + token.type;
          if (token.alias) {
            classes += ' ' + token.alias;
          }
          result += '<span class="' + classes + '">' + this.encode(token.content) + '</span>';
        }
      }
      return result;
    },
    encode: function(str) {
      return str.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
    }
  };

  // Auto-highlight on DOMContentLoaded
  if (typeof document !== 'undefined') {
    if (document.readyState === 'loading') {
      document.addEventListener('DOMContentLoaded', function() {
        Prism.highlightAll();
      });
    } else {
      Prism.highlightAll();
    }
  }

  // Export for use in other scripts
  if (typeof window !== 'undefined') {
    window.Prism = Prism;
  }
})();
