window.pdocSearch = (function () {
  /** elasticlunr - http://weixsong.github.io * Copyright (C) 2017 Oliver Nightingale * Copyright (C) 2017 Wei Song * MIT Licensed */ !(function () {
    function e(e) {
      if (null === e || "object" != typeof e) return e;
      var t = e.constructor();
      for (var n in e) e.hasOwnProperty(n) && (t[n] = e[n]);
      return t;
    }
    var t = function (e) {
      var n = new t.Index();
      return (
        n.pipeline.add(t.trimmer, t.stopWordFilter, t.stemmer),
        e && e.call(n, n),
        n
      );
    };
    (t.version = "0.9.5"),
      (lunr = t),
      (t.utils = {}),
      (t.utils.warn = (function (e) {
        return function (t) {
          e.console && console.warn && console.warn(t);
        };
      })(this)),
      (t.utils.toString = function (e) {
        return void 0 === e || null === e ? "" : e.toString();
      }),
      (t.EventEmitter = function () {
        this.events = {};
      }),
      (t.EventEmitter.prototype.addListener = function () {
        var e = Array.prototype.slice.call(arguments),
          t = e.pop(),
          n = e;
        if ("function" != typeof t)
          throw new TypeError("last argument must be a function");
        n.forEach(function (e) {
          this.hasHandler(e) || (this.events[e] = []), this.events[e].push(t);
        }, this);
      }),
      (t.EventEmitter.prototype.removeListener = function (e, t) {
        if (this.hasHandler(e)) {
          var n = this.events[e].indexOf(t);
          -1 !== n &&
            (this.events[e].splice(n, 1),
            0 == this.events[e].length && delete this.events[e]);
        }
      }),
      (t.EventEmitter.prototype.emit = function (e) {
        if (this.hasHandler(e)) {
          var t = Array.prototype.slice.call(arguments, 1);
          this.events[e].forEach(function (e) {
            e.apply(void 0, t);
          }, this);
        }
      }),
      (t.EventEmitter.prototype.hasHandler = function (e) {
        return e in this.events;
      }),
      (t.tokenizer = function (e) {
        if (!arguments.length || null === e || void 0 === e) return [];
        if (Array.isArray(e)) {
          var n = e.filter(function (e) {
            return null === e || void 0 === e ? !1 : !0;
          });
          n = n.map(function (e) {
            return t.utils.toString(e).toLowerCase();
          });
          var i = [];
          return (
            n.forEach(function (e) {
              var n = e.split(t.tokenizer.seperator);
              i = i.concat(n);
            }, this),
            i
          );
        }
        return e.toString().trim().toLowerCase().split(t.tokenizer.seperator);
      }),
      (t.tokenizer.defaultSeperator = /[\s\-]+/),
      (t.tokenizer.seperator = t.tokenizer.defaultSeperator),
      (t.tokenizer.setSeperator = function (e) {
        null !== e &&
          void 0 !== e &&
          "object" == typeof e &&
          (t.tokenizer.seperator = e);
      }),
      (t.tokenizer.resetSeperator = function () {
        t.tokenizer.seperator = t.tokenizer.defaultSeperator;
      }),
      (t.tokenizer.getSeperator = function () {
        return t.tokenizer.seperator;
      }),
      (t.Pipeline = function () {
        this._queue = [];
      }),
      (t.Pipeline.registeredFunctions = {}),
      (t.Pipeline.registerFunction = function (e, n) {
        n in t.Pipeline.registeredFunctions &&
          t.utils.warn("Overwriting existing registered function: " + n),
          (e.label = n),
          (t.Pipeline.registeredFunctions[n] = e);
      }),
      (t.Pipeline.getRegisteredFunction = function (e) {
        return e in t.Pipeline.registeredFunctions != !0
          ? null
          : t.Pipeline.registeredFunctions[e];
      }),
      (t.Pipeline.warnIfFunctionNotRegistered = function (e) {
        var n = e.label && e.label in this.registeredFunctions;
        n ||
          t.utils.warn(
            "Function is not registered with pipeline. This may cause problems when serialising the index.\n",
            e
          );
      }),
      (t.Pipeline.load = function (e) {
        var n = new t.Pipeline();
        return (
          e.forEach(function (e) {
            var i = t.Pipeline.getRegisteredFunction(e);
            if (!i) throw new Error("Cannot load un-registered function: " + e);
            n.add(i);
          }),
          n
        );
      }),
      (t.Pipeline.prototype.add = function () {
        var e = Array.prototype.slice.call(arguments);
        e.forEach(function (e) {
          t.Pipeline.warnIfFunctionNotRegistered(e), this._queue.push(e);
        }, this);
      }),
      (t.Pipeline.prototype.after = function (e, n) {
        t.Pipeline.warnIfFunctionNotRegistered(n);
        var i = this._queue.indexOf(e);
        if (-1 === i) throw new Error("Cannot find existingFn");
        this._queue.splice(i + 1, 0, n);
      }),
      (t.Pipeline.prototype.before = function (e, n) {
        t.Pipeline.warnIfFunctionNotRegistered(n);
        var i = this._queue.indexOf(e);
        if (-1 === i) throw new Error("Cannot find existingFn");
        this._queue.splice(i, 0, n);
      }),
      (t.Pipeline.prototype.remove = function (e) {
        var t = this._queue.indexOf(e);
        -1 !== t && this._queue.splice(t, 1);
      }),
      (t.Pipeline.prototype.run = function (e) {
        for (
          var t = [], n = e.length, i = this._queue.length, o = 0;
          n > o;
          o++
        ) {
          for (
            var r = e[o], s = 0;
            i > s &&
            ((r = this._queue[s](r, o, e)), void 0 !== r && null !== r);
            s++
          );
          void 0 !== r && null !== r && t.push(r);
        }
        return t;
      }),
      (t.Pipeline.prototype.reset = function () {
        this._queue = [];
      }),
      (t.Pipeline.prototype.get = function () {
        return this._queue;
      }),
      (t.Pipeline.prototype.toJSON = function () {
        return this._queue.map(function (e) {
          return t.Pipeline.warnIfFunctionNotRegistered(e), e.label;
        });
      }),
      (t.Index = function () {
        (this._fields = []),
          (this._ref = "id"),
          (this.pipeline = new t.Pipeline()),
          (this.documentStore = new t.DocumentStore()),
          (this.index = {}),
          (this.eventEmitter = new t.EventEmitter()),
          (this._idfCache = {}),
          this.on(
            "add",
            "remove",
            "update",
            function () {
              this._idfCache = {};
            }.bind(this)
          );
      }),
      (t.Index.prototype.on = function () {
        var e = Array.prototype.slice.call(arguments);
        return this.eventEmitter.addListener.apply(this.eventEmitter, e);
      }),
      (t.Index.prototype.off = function (e, t) {
        return this.eventEmitter.removeListener(e, t);
      }),
      (t.Index.load = function (e) {
        e.version !== t.version &&
          t.utils.warn(
            "version mismatch: current " + t.version + " importing " + e.version
          );
        var n = new this();
        (n._fields = e.fields),
          (n._ref = e.ref),
          (n.documentStore = t.DocumentStore.load(e.documentStore)),
          (n.pipeline = t.Pipeline.load(e.pipeline)),
          (n.index = {});
        for (var i in e.index) n.index[i] = t.InvertedIndex.load(e.index[i]);
        return n;
      }),
      (t.Index.prototype.addField = function (e) {
        return (
          this._fields.push(e), (this.index[e] = new t.InvertedIndex()), this
        );
      }),
      (t.Index.prototype.setRef = function (e) {
        return (this._ref = e), this;
      }),
      (t.Index.prototype.saveDocument = function (e) {
        return (this.documentStore = new t.DocumentStore(e)), this;
      }),
      (t.Index.prototype.addDoc = function (e, n) {
        if (e) {
          var n = void 0 === n ? !0 : n,
            i = e[this._ref];
          this.documentStore.addDoc(i, e),
            this._fields.forEach(function (n) {
              var o = this.pipeline.run(t.tokenizer(e[n]));
              this.documentStore.addFieldLength(i, n, o.length);
              var r = {};
              o.forEach(function (e) {
                e in r ? (r[e] += 1) : (r[e] = 1);
              }, this);
              for (var s in r) {
                var u = r[s];
                (u = Math.sqrt(u)),
                  this.index[n].addToken(s, { ref: i, tf: u });
              }
            }, this),
            n && this.eventEmitter.emit("add", e, this);
        }
      }),
      (t.Index.prototype.removeDocByRef = function (e) {
        if (
          e &&
          this.documentStore.isDocStored() !== !1 &&
          this.documentStore.hasDoc(e)
        ) {
          var t = this.documentStore.getDoc(e);
          this.removeDoc(t, !1);
        }
      }),
      (t.Index.prototype.removeDoc = function (e, n) {
        if (e) {
          var n = void 0 === n ? !0 : n,
            i = e[this._ref];
          this.documentStore.hasDoc(i) &&
            (this.documentStore.removeDoc(i),
            this._fields.forEach(function (n) {
              var o = this.pipeline.run(t.tokenizer(e[n]));
              o.forEach(function (e) {
                this.index[n].removeToken(e, i);
              }, this);
            }, this),
            n && this.eventEmitter.emit("remove", e, this));
        }
      }),
      (t.Index.prototype.updateDoc = function (e, t) {
        var t = void 0 === t ? !0 : t;
        this.removeDocByRef(e[this._ref], !1),
          this.addDoc(e, !1),
          t && this.eventEmitter.emit("update", e, this);
      }),
      (t.Index.prototype.idf = function (e, t) {
        var n = "@" + t + "/" + e;
        if (Object.prototype.hasOwnProperty.call(this._idfCache, n))
          return this._idfCache[n];
        var i = this.index[t].getDocFreq(e),
          o = 1 + Math.log(this.documentStore.length / (i + 1));
        return (this._idfCache[n] = o), o;
      }),
      (t.Index.prototype.getFields = function () {
        return this._fields.slice();
      }),
      (t.Index.prototype.search = function (e, n) {
        if (!e) return [];
        e = "string" == typeof e ? { any: e } : JSON.parse(JSON.stringify(e));
        var i = null;
        null != n && (i = JSON.stringify(n));
        for (
          var o = new t.Configuration(i, this.getFields()).get(),
            r = {},
            s = Object.keys(e),
            u = 0;
          u < s.length;
          u++
        ) {
          var a = s[u];
          r[a] = this.pipeline.run(t.tokenizer(e[a]));
        }
        var l = {};
        for (var c in o) {
          var d = r[c] || r.any;
          if (d) {
            var f = this.fieldSearch(d, c, o),
              h = o[c].boost;
            for (var p in f) f[p] = f[p] * h;
            for (var p in f) p in l ? (l[p] += f[p]) : (l[p] = f[p]);
          }
        }
        var v,
          g = [];
        for (var p in l)
          (v = { ref: p, score: l[p] }),
            this.documentStore.hasDoc(p) &&
              (v.doc = this.documentStore.getDoc(p)),
            g.push(v);
        return (
          g.sort(function (e, t) {
            return t.score - e.score;
          }),
          g
        );
      }),
      (t.Index.prototype.fieldSearch = function (e, t, n) {
        var i = n[t].bool,
          o = n[t].expand,
          r = n[t].boost,
          s = null,
          u = {};
        return 0 !== r
          ? (e.forEach(function (e) {
              var n = [e];
              1 == o && (n = this.index[t].expandToken(e));
              var r = {};
              n.forEach(function (n) {
                var o = this.index[t].getDocs(n),
                  a = this.idf(n, t);
                if (s && "AND" == i) {
                  var l = {};
                  for (var c in s) c in o && (l[c] = o[c]);
                  o = l;
                }
                n == e && this.fieldSearchStats(u, n, o);
                for (var c in o) {
                  var d = this.index[t].getTermFrequency(n, c),
                    f = this.documentStore.getFieldLength(c, t),
                    h = 1;
                  0 != f && (h = 1 / Math.sqrt(f));
                  var p = 1;
                  n != e && (p = 0.15 * (1 - (n.length - e.length) / n.length));
                  var v = d * a * h * p;
                  c in r ? (r[c] += v) : (r[c] = v);
                }
              }, this),
                (s = this.mergeScores(s, r, i));
            }, this),
            (s = this.coordNorm(s, u, e.length)))
          : void 0;
      }),
      (t.Index.prototype.mergeScores = function (e, t, n) {
        if (!e) return t;
        if ("AND" == n) {
          var i = {};
          for (var o in t) o in e && (i[o] = e[o] + t[o]);
          return i;
        }
        for (var o in t) o in e ? (e[o] += t[o]) : (e[o] = t[o]);
        return e;
      }),
      (t.Index.prototype.fieldSearchStats = function (e, t, n) {
        for (var i in n) i in e ? e[i].push(t) : (e[i] = [t]);
      }),
      (t.Index.prototype.coordNorm = function (e, t, n) {
        for (var i in e)
          if (i in t) {
            var o = t[i].length;
            e[i] = (e[i] * o) / n;
          }
        return e;
      }),
      (t.Index.prototype.toJSON = function () {
        var e = {};
        return (
          this._fields.forEach(function (t) {
            e[t] = this.index[t].toJSON();
          }, this),
          {
            version: t.version,
            fields: this._fields,
            ref: this._ref,
            documentStore: this.documentStore.toJSON(),
            index: e,
            pipeline: this.pipeline.toJSON(),
          }
        );
      }),
      (t.Index.prototype.use = function (e) {
        var t = Array.prototype.slice.call(arguments, 1);
        t.unshift(this), e.apply(this, t);
      }),
      (t.DocumentStore = function (e) {
        (this._save = null === e || void 0 === e ? !0 : e),
          (this.docs = {}),
          (this.docInfo = {}),
          (this.length = 0);
      }),
      (t.DocumentStore.load = function (e) {
        var t = new this();
        return (
          (t.length = e.length),
          (t.docs = e.docs),
          (t.docInfo = e.docInfo),
          (t._save = e.save),
          t
        );
      }),
      (t.DocumentStore.prototype.isDocStored = function () {
        return this._save;
      }),
      (t.DocumentStore.prototype.addDoc = function (t, n) {
        this.hasDoc(t) || this.length++,
          (this.docs[t] = this._save === !0 ? e(n) : null);
      }),
      (t.DocumentStore.prototype.getDoc = function (e) {
        return this.hasDoc(e) === !1 ? null : this.docs[e];
      }),
      (t.DocumentStore.prototype.hasDoc = function (e) {
        return e in this.docs;
      }),
      (t.DocumentStore.prototype.removeDoc = function (e) {
        this.hasDoc(e) &&
          (delete this.docs[e], delete this.docInfo[e], this.length--);
      }),
      (t.DocumentStore.prototype.addFieldLength = function (e, t, n) {
        null !== e &&
          void 0 !== e &&
          0 != this.hasDoc(e) &&
          (this.docInfo[e] || (this.docInfo[e] = {}), (this.docInfo[e][t] = n));
      }),
      (t.DocumentStore.prototype.updateFieldLength = function (e, t, n) {
        null !== e &&
          void 0 !== e &&
          0 != this.hasDoc(e) &&
          this.addFieldLength(e, t, n);
      }),
      (t.DocumentStore.prototype.getFieldLength = function (e, t) {
        return null === e || void 0 === e
          ? 0
          : e in this.docs && t in this.docInfo[e]
          ? this.docInfo[e][t]
          : 0;
      }),
      (t.DocumentStore.prototype.toJSON = function () {
        return {
          docs: this.docs,
          docInfo: this.docInfo,
          length: this.length,
          save: this._save,
        };
      }),
      (t.stemmer = (function () {
        var e = {
            ational: "ate",
            tional: "tion",
            enci: "ence",
            anci: "ance",
            izer: "ize",
            bli: "ble",
            alli: "al",
            entli: "ent",
            eli: "e",
            ousli: "ous",
            ization: "ize",
            ation: "ate",
            ator: "ate",
            alism: "al",
            iveness: "ive",
            fulness: "ful",
            ousness: "ous",
            aliti: "al",
            iviti: "ive",
            biliti: "ble",
            logi: "log",
          },
          t = {
            icate: "ic",
            ative: "",
            alize: "al",
            iciti: "ic",
            ical: "ic",
            ful: "",
            ness: "",
          },
          n = "[^aeiou]",
          i = "[aeiouy]",
          o = n + "[^aeiouy]*",
          r = i + "[aeiou]*",
          s = "^(" + o + ")?" + r + o,
          u = "^(" + o + ")?" + r + o + "(" + r + ")?$",
          a = "^(" + o + ")?" + r + o + r + o,
          l = "^(" + o + ")?" + i,
          c = new RegExp(s),
          d = new RegExp(a),
          f = new RegExp(u),
          h = new RegExp(l),
          p = /^(.+?)(ss|i)es$/,
          v = /^(.+?)([^s])s$/,
          g = /^(.+?)eed$/,
          m = /^(.+?)(ed|ing)$/,
          y = /.$/,
          S = /(at|bl|iz)$/,
          x = new RegExp("([^aeiouylsz])\\1$"),
          w = new RegExp("^" + o + i + "[^aeiouwxy]$"),
          I = /^(.+?[^aeiou])y$/,
          b =
            /^(.+?)(ational|tional|enci|anci|izer|bli|alli|entli|eli|ousli|ization|ation|ator|alism|iveness|fulness|ousness|aliti|iviti|biliti|logi)$/,
          E = /^(.+?)(icate|ative|alize|iciti|ical|ful|ness)$/,
          D =
            /^(.+?)(al|ance|ence|er|ic|able|ible|ant|ement|ment|ent|ou|ism|ate|iti|ous|ive|ize)$/,
          F = /^(.+?)(s|t)(ion)$/,
          _ = /^(.+?)e$/,
          P = /ll$/,
          k = new RegExp("^" + o + i + "[^aeiouwxy]$"),
          z = function (n) {
            var i, o, r, s, u, a, l;
            if (n.length < 3) return n;
            if (
              ((r = n.substr(0, 1)),
              "y" == r && (n = r.toUpperCase() + n.substr(1)),
              (s = p),
              (u = v),
              s.test(n)
                ? (n = n.replace(s, "$1$2"))
                : u.test(n) && (n = n.replace(u, "$1$2")),
              (s = g),
              (u = m),
              s.test(n))
            ) {
              var z = s.exec(n);
              (s = c), s.test(z[1]) && ((s = y), (n = n.replace(s, "")));
            } else if (u.test(n)) {
              var z = u.exec(n);
              (i = z[1]),
                (u = h),
                u.test(i) &&
                  ((n = i),
                  (u = S),
                  (a = x),
                  (l = w),
                  u.test(n)
                    ? (n += "e")
                    : a.test(n)
                    ? ((s = y), (n = n.replace(s, "")))
                    : l.test(n) && (n += "e"));
            }
            if (((s = I), s.test(n))) {
              var z = s.exec(n);
              (i = z[1]), (n = i + "i");
            }
            if (((s = b), s.test(n))) {
              var z = s.exec(n);
              (i = z[1]), (o = z[2]), (s = c), s.test(i) && (n = i + e[o]);
            }
            if (((s = E), s.test(n))) {
              var z = s.exec(n);
              (i = z[1]), (o = z[2]), (s = c), s.test(i) && (n = i + t[o]);
            }
            if (((s = D), (u = F), s.test(n))) {
              var z = s.exec(n);
              (i = z[1]), (s = d), s.test(i) && (n = i);
            } else if (u.test(n)) {
              var z = u.exec(n);
              (i = z[1] + z[2]), (u = d), u.test(i) && (n = i);
            }
            if (((s = _), s.test(n))) {
              var z = s.exec(n);
              (i = z[1]),
                (s = d),
                (u = f),
                (a = k),
                (s.test(i) || (u.test(i) && !a.test(i))) && (n = i);
            }
            return (
              (s = P),
              (u = d),
              s.test(n) && u.test(n) && ((s = y), (n = n.replace(s, ""))),
              "y" == r && (n = r.toLowerCase() + n.substr(1)),
              n
            );
          };
        return z;
      })()),
      t.Pipeline.registerFunction(t.stemmer, "stemmer"),
      (t.stopWordFilter = function (e) {
        return e && t.stopWordFilter.stopWords[e] !== !0 ? e : void 0;
      }),
      (t.clearStopWords = function () {
        t.stopWordFilter.stopWords = {};
      }),
      (t.addStopWords = function (e) {
        null != e &&
          Array.isArray(e) !== !1 &&
          e.forEach(function (e) {
            t.stopWordFilter.stopWords[e] = !0;
          }, this);
      }),
      (t.resetStopWords = function () {
        t.stopWordFilter.stopWords = t.defaultStopWords;
      }),
      (t.defaultStopWords = {
        "": !0,
        a: !0,
        able: !0,
        about: !0,
        across: !0,
        after: !0,
        all: !0,
        almost: !0,
        also: !0,
        am: !0,
        among: !0,
        an: !0,
        and: !0,
        any: !0,
        are: !0,
        as: !0,
        at: !0,
        be: !0,
        because: !0,
        been: !0,
        but: !0,
        by: !0,
        can: !0,
        cannot: !0,
        could: !0,
        dear: !0,
        did: !0,
        do: !0,
        does: !0,
        either: !0,
        else: !0,
        ever: !0,
        every: !0,
        for: !0,
        from: !0,
        get: !0,
        got: !0,
        had: !0,
        has: !0,
        have: !0,
        he: !0,
        her: !0,
        hers: !0,
        him: !0,
        his: !0,
        how: !0,
        however: !0,
        i: !0,
        if: !0,
        in: !0,
        into: !0,
        is: !0,
        it: !0,
        its: !0,
        just: !0,
        least: !0,
        let: !0,
        like: !0,
        likely: !0,
        may: !0,
        me: !0,
        might: !0,
        most: !0,
        must: !0,
        my: !0,
        neither: !0,
        no: !0,
        nor: !0,
        not: !0,
        of: !0,
        off: !0,
        often: !0,
        on: !0,
        only: !0,
        or: !0,
        other: !0,
        our: !0,
        own: !0,
        rather: !0,
        said: !0,
        say: !0,
        says: !0,
        she: !0,
        should: !0,
        since: !0,
        so: !0,
        some: !0,
        than: !0,
        that: !0,
        the: !0,
        their: !0,
        them: !0,
        then: !0,
        there: !0,
        these: !0,
        they: !0,
        this: !0,
        tis: !0,
        to: !0,
        too: !0,
        twas: !0,
        us: !0,
        wants: !0,
        was: !0,
        we: !0,
        were: !0,
        what: !0,
        when: !0,
        where: !0,
        which: !0,
        while: !0,
        who: !0,
        whom: !0,
        why: !0,
        will: !0,
        with: !0,
        would: !0,
        yet: !0,
        you: !0,
        your: !0,
      }),
      (t.stopWordFilter.stopWords = t.defaultStopWords),
      t.Pipeline.registerFunction(t.stopWordFilter, "stopWordFilter"),
      (t.trimmer = function (e) {
        if (null === e || void 0 === e)
          throw new Error("token should not be undefined");
        return e.replace(/^\W+/, "").replace(/\W+$/, "");
      }),
      t.Pipeline.registerFunction(t.trimmer, "trimmer"),
      (t.InvertedIndex = function () {
        this.root = { docs: {}, df: 0 };
      }),
      (t.InvertedIndex.load = function (e) {
        var t = new this();
        return (t.root = e.root), t;
      }),
      (t.InvertedIndex.prototype.addToken = function (e, t, n) {
        for (var n = n || this.root, i = 0; i <= e.length - 1; ) {
          var o = e[i];
          o in n || (n[o] = { docs: {}, df: 0 }), (i += 1), (n = n[o]);
        }
        var r = t.ref;
        n.docs[r]
          ? (n.docs[r] = { tf: t.tf })
          : ((n.docs[r] = { tf: t.tf }), (n.df += 1));
      }),
      (t.InvertedIndex.prototype.hasToken = function (e) {
        if (!e) return !1;
        for (var t = this.root, n = 0; n < e.length; n++) {
          if (!t[e[n]]) return !1;
          t = t[e[n]];
        }
        return !0;
      }),
      (t.InvertedIndex.prototype.getNode = function (e) {
        if (!e) return null;
        for (var t = this.root, n = 0; n < e.length; n++) {
          if (!t[e[n]]) return null;
          t = t[e[n]];
        }
        return t;
      }),
      (t.InvertedIndex.prototype.getDocs = function (e) {
        var t = this.getNode(e);
        return null == t ? {} : t.docs;
      }),
      (t.InvertedIndex.prototype.getTermFrequency = function (e, t) {
        var n = this.getNode(e);
        return null == n ? 0 : t in n.docs ? n.docs[t].tf : 0;
      }),
      (t.InvertedIndex.prototype.getDocFreq = function (e) {
        var t = this.getNode(e);
        return null == t ? 0 : t.df;
      }),
      (t.InvertedIndex.prototype.removeToken = function (e, t) {
        if (e) {
          var n = this.getNode(e);
          null != n && t in n.docs && (delete n.docs[t], (n.df -= 1));
        }
      }),
      (t.InvertedIndex.prototype.expandToken = function (e, t, n) {
        if (null == e || "" == e) return [];
        var t = t || [];
        if (void 0 == n && ((n = this.getNode(e)), null == n)) return t;
        n.df > 0 && t.push(e);
        for (var i in n)
          "docs" !== i && "df" !== i && this.expandToken(e + i, t, n[i]);
        return t;
      }),
      (t.InvertedIndex.prototype.toJSON = function () {
        return { root: this.root };
      }),
      (t.Configuration = function (e, n) {
        var e = e || "";
        if (void 0 == n || null == n)
          throw new Error("fields should not be null");
        this.config = {};
        var i;
        try {
          (i = JSON.parse(e)), this.buildUserConfig(i, n);
        } catch (o) {
          t.utils.warn(
            "user configuration parse failed, will use default configuration"
          ),
            this.buildDefaultConfig(n);
        }
      }),
      (t.Configuration.prototype.buildDefaultConfig = function (e) {
        this.reset(),
          e.forEach(function (e) {
            this.config[e] = { boost: 1, bool: "OR", expand: !1 };
          }, this);
      }),
      (t.Configuration.prototype.buildUserConfig = function (e, n) {
        var i = "OR",
          o = !1;
        if (
          (this.reset(),
          "bool" in e && (i = e.bool || i),
          "expand" in e && (o = e.expand || o),
          "fields" in e)
        )
          for (var r in e.fields)
            if (n.indexOf(r) > -1) {
              var s = e.fields[r],
                u = o;
              void 0 != s.expand && (u = s.expand),
                (this.config[r] = {
                  boost: s.boost || 0 === s.boost ? s.boost : 1,
                  bool: s.bool || i,
                  expand: u,
                });
            } else
              t.utils.warn(
                "field name in user configuration not found in index instance fields"
              );
        else this.addAllFields2UserConfig(i, o, n);
      }),
      (t.Configuration.prototype.addAllFields2UserConfig = function (e, t, n) {
        n.forEach(function (n) {
          this.config[n] = { boost: 1, bool: e, expand: t };
        }, this);
      }),
      (t.Configuration.prototype.get = function () {
        return this.config;
      }),
      (t.Configuration.prototype.reset = function () {
        this.config = {};
      }),
      (lunr.SortedSet = function () {
        (this.length = 0), (this.elements = []);
      }),
      (lunr.SortedSet.load = function (e) {
        var t = new this();
        return (t.elements = e), (t.length = e.length), t;
      }),
      (lunr.SortedSet.prototype.add = function () {
        var e, t;
        for (e = 0; e < arguments.length; e++)
          (t = arguments[e]),
            ~this.indexOf(t) || this.elements.splice(this.locationFor(t), 0, t);
        this.length = this.elements.length;
      }),
      (lunr.SortedSet.prototype.toArray = function () {
        return this.elements.slice();
      }),
      (lunr.SortedSet.prototype.map = function (e, t) {
        return this.elements.map(e, t);
      }),
      (lunr.SortedSet.prototype.forEach = function (e, t) {
        return this.elements.forEach(e, t);
      }),
      (lunr.SortedSet.prototype.indexOf = function (e) {
        for (
          var t = 0,
            n = this.elements.length,
            i = n - t,
            o = t + Math.floor(i / 2),
            r = this.elements[o];
          i > 1;

        ) {
          if (r === e) return o;
          e > r && (t = o),
            r > e && (n = o),
            (i = n - t),
            (o = t + Math.floor(i / 2)),
            (r = this.elements[o]);
        }
        return r === e ? o : -1;
      }),
      (lunr.SortedSet.prototype.locationFor = function (e) {
        for (
          var t = 0,
            n = this.elements.length,
            i = n - t,
            o = t + Math.floor(i / 2),
            r = this.elements[o];
          i > 1;

        )
          e > r && (t = o),
            r > e && (n = o),
            (i = n - t),
            (o = t + Math.floor(i / 2)),
            (r = this.elements[o]);
        return r > e ? o : e > r ? o + 1 : void 0;
      }),
      (lunr.SortedSet.prototype.intersect = function (e) {
        for (
          var t = new lunr.SortedSet(),
            n = 0,
            i = 0,
            o = this.length,
            r = e.length,
            s = this.elements,
            u = e.elements;
          ;

        ) {
          if (n > o - 1 || i > r - 1) break;
          s[n] !== u[i]
            ? s[n] < u[i]
              ? n++
              : s[n] > u[i] && i++
            : (t.add(s[n]), n++, i++);
        }
        return t;
      }),
      (lunr.SortedSet.prototype.clone = function () {
        var e = new lunr.SortedSet();
        return (e.elements = this.toArray()), (e.length = e.elements.length), e;
      }),
      (lunr.SortedSet.prototype.union = function (e) {
        var t, n, i;
        this.length >= e.length ? ((t = this), (n = e)) : ((t = e), (n = this)),
          (i = t.clone());
        for (var o = 0, r = n.toArray(); o < r.length; o++) i.add(r[o]);
        return i;
      }),
      (lunr.SortedSet.prototype.toJSON = function () {
        return this.toArray();
      }),
      (function (e, t) {
        "function" == typeof define && define.amd
          ? define(t)
          : "object" == typeof exports
          ? (module.exports = t())
          : (e.elasticlunr = t());
      })(this, function () {
        return t;
      });
  })();
  /** pdoc search index */ const docs = [
    {
      fullname: "ncaa_stats_py",
      modulename: "ncaa_stats_py",
      kind: "module",
      doc: '<h1 id="welcome">Welcome!</h1>\n\n<p>This is the official docs page for the <code>ncaa_stats_py</code> python package.</p>\n\n<h1 id="basic-setup">Basic Setup</h1>\n\n<h2 id="how-to-install">How to Install</h2>\n\n<p>This package is is available through the\n<a href="https://en.wikipedia.org/wiki/Pip_(package_manager)"><code>pip</code> package manager</a>,\nand can be installed through one of the following commands\nin your terminal/shell:</p>\n\n<pre><code>pip install ncaa_stats_py\n</code></pre>\n\n<p>OR</p>\n\n<pre><code>python -m pip install ncaa_stats_py\n</code></pre>\n\n<p>If you are using a Linux/Mac instance,\nyou may need to specify <code>python3</code> when installing, as shown below:</p>\n\n<pre><code>python3 -m pip install ncaa_stats_py\n</code></pre>\n\n<p>Alternatively, <code>cfbd-json-py</code> can be installed from\nthis GitHub repository with the following command through pip:</p>\n\n<pre><code>pip install git+https://github.com/armstjc/ncaa_stats_py\n</code></pre>\n\n<p>OR</p>\n\n<pre><code>python -m pip install git+https://github.com/armstjc/ncaa_stats_py\n</code></pre>\n\n<p>OR</p>\n\n<pre><code>python3 -m pip install git+https://github.com/armstjc/ncaa_stats_py\n</code></pre>\n\n<h2 id="how-to-use">How to Use</h2>\n\n<p><code>ncaa_stats_py</code> separates itself by doing the following\nthings when attempting to get data:</p>\n\n<ol>\n<li>Automatically caching any data that is already parsed</li>\n<li>Automatically forcing a 5 second sleep timer for any HTML call,\nto ensure that any function call from this package\nwon\'t result in you getting IP banned\n(you do not <em>need</em> to add sleep timers if you\'re looping through, \nand calling functions in this python package).</li>\n<li>Automatically refreshing any cached data if it\'s stale.</li>\n</ol>\n\n<p>For example, the following code will work as-is,\n    and in the second loop, the code will load in the teams\n    even faster because the data is cached\n    on the device you\'re running this code.</p>\n\n<div class="pdoc-code codehilite">\n<pre><span></span><code><span class="kn">from</span> <span class="nn">timeit</span> <span class="kn">import</span> <span class="n">default_timer</span> <span class="k">as</span> <span class="n">timer</span>\n\n<span class="kn">from</span> <span class="nn">ncaa_stats_py.baseball</span> <span class="kn">import</span> <span class="p">(</span>\n    <span class="n">get_baseball_team_roster</span><span class="p">,</span>\n    <span class="n">load_baseball_teams</span>\n<span class="p">)</span>\n\n<span class="c1"># Loads in a table with every NCAA baseball team from 2008 to present day.</span>\n<span class="n">teams_df</span> <span class="o">=</span> <span class="n">load_baseball_teams</span><span class="p">()</span>\n\n<span class="c1"># Gets 5 random D1 teams from 2023</span>\n<span class="n">teams_df</span> <span class="o">=</span> <span class="n">teams_df</span><span class="o">.</span><span class="n">sample</span><span class="p">(</span><span class="mi">5</span><span class="p">)</span>\n<span class="nb">print</span><span class="p">(</span><span class="n">teams_df</span><span class="p">)</span>\n<span class="nb">print</span><span class="p">()</span>\n\n<span class="c1"># Let&#39;s send this to a list to make the loop slightly faster</span>\n<span class="n">team_ids_list</span> <span class="o">=</span> <span class="n">teams_df</span><span class="p">[</span><span class="s2">&quot;team_id&quot;</span><span class="p">]</span><span class="o">.</span><span class="n">to_list</span><span class="p">()</span>\n\n<span class="c1"># First loop</span>\n<span class="c1"># If the data isn&#39;t cached, it should take 35-40 seconds to do this loop</span>\n<span class="n">start_time</span> <span class="o">=</span> <span class="n">timer</span><span class="p">()</span>\n\n<span class="k">for</span> <span class="n">t_id</span> <span class="ow">in</span> <span class="n">team_ids_list</span><span class="p">:</span>\n    <span class="nb">print</span><span class="p">(</span><span class="sa">f</span><span class="s2">&quot;On Team ID: </span><span class="si">{</span><span class="n">t_id</span><span class="si">}</span><span class="s2">&quot;</span><span class="p">)</span>\n    <span class="n">df</span> <span class="o">=</span> <span class="n">get_baseball_team_roster</span><span class="p">(</span><span class="n">team_id</span><span class="o">=</span><span class="n">t_id</span><span class="p">)</span>\n    <span class="c1"># print(df)</span>\n\n<span class="n">end_time</span> <span class="o">=</span> <span class="n">timer</span><span class="p">()</span>\n\n<span class="n">time_elapsed</span> <span class="o">=</span> <span class="n">end_time</span> <span class="o">-</span> <span class="n">start_time</span>\n<span class="nb">print</span><span class="p">(</span><span class="sa">f</span><span class="s2">&quot;Elapsed time: </span><span class="si">{</span><span class="n">time_elapsed</span><span class="si">:</span><span class="s2">03f</span><span class="si">}</span><span class="s2"> seconds.</span>\n\n<span class="s2">&quot;)</span>\n\n<span class="c1"># Second loop</span>\n<span class="c1"># Because the data has been parsed and cached,</span>\n<span class="c1"># this shouldn&#39;t take that long to loop through</span>\n<span class="n">start_time</span> <span class="o">=</span> <span class="n">timer</span><span class="p">()</span>\n\n<span class="k">for</span> <span class="n">t_id</span> <span class="ow">in</span> <span class="n">team_ids_list</span><span class="p">:</span>\n    <span class="nb">print</span><span class="p">(</span><span class="sa">f</span><span class="s2">&quot;On Team ID: </span><span class="si">{</span><span class="n">t_id</span><span class="si">}</span><span class="s2">&quot;</span><span class="p">)</span>\n    <span class="n">df</span> <span class="o">=</span> <span class="n">get_baseball_team_roster</span><span class="p">(</span><span class="n">team_id</span><span class="o">=</span><span class="n">t_id</span><span class="p">)</span>\n    <span class="c1"># print(df)</span>\n\n<span class="n">end_time</span> <span class="o">=</span> <span class="n">timer</span><span class="p">()</span>\n<span class="n">time_elapsed</span> <span class="o">=</span> <span class="n">end_time</span> <span class="o">-</span> <span class="n">start_time</span>\n<span class="nb">print</span><span class="p">(</span><span class="sa">f</span><span class="s2">&quot;Elapsed time: </span><span class="si">{</span><span class="n">time_elapsed</span><span class="si">:</span><span class="s2">03f</span><span class="si">}</span><span class="s2"> seconds.</span>\n\n<span class="s2">&quot;)</span>\n</code></pre>\n</div>\n',
    },
    {
      fullname: "ncaa_stats_py.baseball",
      modulename: "ncaa_stats_py.baseball",
      kind: "module",
      doc: "<p></p>\n",
    },
    {
      fullname: "ncaa_stats_py.baseball.get_baseball_teams",
      modulename: "ncaa_stats_py.baseball",
      qualname: "get_baseball_teams",
      kind: "function",
      doc: '<p>Retrieves a list of baseball teams from the NCAA.</p>\n\n<h2 id="parameters">Parameters</h2>\n\n<p><code>season</code> (int, mandatory):\n    Required argument.\n    Specifies the season you want NCAA baseball team information from.</p>\n\n<p><code>level</code> (int, mandatory):\n    Required argument.\n    Specifies the level/division you want\n    NCAA baseball team information from.\n    This can either be an integer (1-3) or a string ("I"-"III").</p>\n\n<h2 id="usage">Usage</h2>\n\n<div class="pdoc-code codehilite">\n<pre><span></span><code><span class="kn">from</span> <span class="nn">ncaa_stats_py.baseball</span> <span class="kn">import</span> <span class="n">get_baseball_teams</span>\n\n<span class="c1"># Get all D1 baseball teams for the 2024 season.</span>\n<span class="nb">print</span><span class="p">(</span><span class="s2">&quot;Get all D1 baseball teams for the 2024 season.&quot;</span><span class="p">)</span>\n<span class="n">df</span> <span class="o">=</span> <span class="n">get_baseball_teams</span><span class="p">(</span><span class="mi">2024</span><span class="p">,</span> <span class="mi">1</span><span class="p">)</span>\n<span class="nb">print</span><span class="p">(</span><span class="n">df</span><span class="p">)</span>\n\n<span class="c1"># Get all D2 baseball teams for the 2023 season.</span>\n<span class="nb">print</span><span class="p">(</span><span class="s2">&quot;Get all D2 baseball teams for the 2023 season.&quot;</span><span class="p">)</span>\n<span class="n">df</span> <span class="o">=</span> <span class="n">get_baseball_teams</span><span class="p">(</span><span class="mi">2023</span><span class="p">,</span> <span class="mi">2</span><span class="p">)</span>\n<span class="nb">print</span><span class="p">(</span><span class="n">df</span><span class="p">)</span>\n\n<span class="c1"># Get all D3 baseball teams for the 2022 season.</span>\n<span class="nb">print</span><span class="p">(</span><span class="s2">&quot;Get all D3 baseball teams for the 2022 season.&quot;</span><span class="p">)</span>\n<span class="n">df</span> <span class="o">=</span> <span class="n">get_baseball_teams</span><span class="p">(</span><span class="mi">2022</span><span class="p">,</span> <span class="mi">3</span><span class="p">)</span>\n<span class="nb">print</span><span class="p">(</span><span class="n">df</span><span class="p">)</span>\n\n<span class="c1"># Get all D1 baseball teams for the 2021 season.</span>\n<span class="nb">print</span><span class="p">(</span><span class="s2">&quot;Get all D1 baseball teams for the 2021 season.&quot;</span><span class="p">)</span>\n<span class="n">df</span> <span class="o">=</span> <span class="n">get_baseball_teams</span><span class="p">(</span><span class="mi">2021</span><span class="p">,</span> <span class="s2">&quot;I&quot;</span><span class="p">)</span>\n<span class="nb">print</span><span class="p">(</span><span class="n">df</span><span class="p">)</span>\n\n<span class="c1"># Get all D2 baseball teams for the 2020 season.</span>\n<span class="nb">print</span><span class="p">(</span><span class="s2">&quot;Get all D2 baseball teams for the 2020 season.&quot;</span><span class="p">)</span>\n<span class="n">df</span> <span class="o">=</span> <span class="n">get_baseball_teams</span><span class="p">(</span><span class="mi">2020</span><span class="p">,</span> <span class="s2">&quot;II&quot;</span><span class="p">)</span>\n<span class="nb">print</span><span class="p">(</span><span class="n">df</span><span class="p">)</span>\n\n<span class="c1"># Get all D3 baseball teams for the 2019 season.</span>\n<span class="nb">print</span><span class="p">(</span><span class="s2">&quot;Get all D3 baseball teams for the 2019 season.&quot;</span><span class="p">)</span>\n<span class="n">df</span> <span class="o">=</span> <span class="n">get_baseball_teams</span><span class="p">(</span><span class="mi">2019</span><span class="p">,</span> <span class="s2">&quot;III&quot;</span><span class="p">)</span>\n<span class="nb">print</span><span class="p">(</span><span class="n">df</span><span class="p">)</span>\n</code></pre>\n</div>\n\n<h2 id="returns">Returns</h2>\n\n<p>A pandas <code>DataFrame</code> object with a list of college baseball teams\nin that season and NCAA level.</p>\n',
      signature:
        '<span class="signature pdoc-code condensed">(<span class="param"><span class="n">season</span><span class="p">:</span> <span class="nb">int</span>, </span><span class="param"><span class="n">level</span><span class="p">:</span> <span class="nb">str</span> <span class="o">|</span> <span class="nb">int</span></span><span class="return-annotation">) -> <span class="n">pandas</span><span class="o">.</span><span class="n">core</span><span class="o">.</span><span class="n">frame</span><span class="o">.</span><span class="n">DataFrame</span>:</span></span>',
      funcdef: "def",
    },
    {
      fullname: "ncaa_stats_py.baseball.load_baseball_teams",
      modulename: "ncaa_stats_py.baseball",
      qualname: "load_baseball_teams",
      kind: "function",
      doc: '<p>Compiles a list of known NCAA baseball teams in NCAA baseball history.</p>\n\n<h2 id="parameters">Parameters</h2>\n\n<p><code>start_year</code> (int, optional):\n    Optional argument.\n    Specifies the first season you want\n    NCAA baseball team information from.</p>\n\n<h2 id="usage">Usage</h2>\n\n<div class="pdoc-code codehilite">\n<pre><span></span><code><span class="kn">from</span> <span class="nn">ncaa_stats_py.baseball</span> <span class="kn">import</span> <span class="n">load_baseball_teams</span>\n\n<span class="c1"># Compile a list of known baseball teams</span>\n<span class="c1"># in NCAA baseball history.</span>\n<span class="nb">print</span><span class="p">(</span>\n    <span class="s2">&quot;Compile a list of known baseball teams &quot;</span> <span class="o">+</span>\n    <span class="s2">&quot;in NCAA baseball history.&quot;</span>\n<span class="p">)</span>\n<span class="n">df</span> <span class="o">=</span> <span class="n">load_baseball_teams</span><span class="p">()</span>\n<span class="nb">print</span><span class="p">(</span><span class="n">df</span><span class="p">)</span>\n</code></pre>\n</div>\n\n<h2 id="returns">Returns</h2>\n\n<p>A pandas <code>DataFrame</code> object with a list of\nall known college baseball teams.</p>\n',
      signature:
        '<span class="signature pdoc-code condensed">(<span class="param"><span class="n">start_year</span><span class="p">:</span> <span class="nb">int</span> <span class="o">=</span> <span class="mi">2008</span></span><span class="return-annotation">) -> <span class="n">pandas</span><span class="o">.</span><span class="n">core</span><span class="o">.</span><span class="n">frame</span><span class="o">.</span><span class="n">DataFrame</span>:</span></span>',
      funcdef: "def",
    },
    {
      fullname: "ncaa_stats_py.baseball.get_baseball_team_schedule",
      modulename: "ncaa_stats_py.baseball",
      qualname: "get_baseball_team_schedule",
      kind: "function",
      doc: '<p>Retrieves a team schedule, from a valid NCAA baseball team ID.</p>\n\n<h2 id="parameters">Parameters</h2>\n\n<p><code>team_id</code> (int, mandatory):\n    Required argument.\n    Specifies the team you want a schedule from.\n    This is separate from a school ID, which identifies the institution.\n    A team ID should be unique to a school, and a season.</p>\n\n<h2 id="usage">Usage</h2>\n\n<div class="pdoc-code codehilite">\n<pre><span></span><code><span class="kn">from</span> <span class="nn">ncaa_stats_py.baseball</span> <span class="kn">import</span> <span class="n">get_baseball_team_schedule</span>\n\n<span class="c1"># Get the team schedule for the</span>\n<span class="c1"># 2024 Texas Tech baseball team (D1, ID: 573916).</span>\n<span class="nb">print</span><span class="p">(</span>\n    <span class="s2">&quot;Get the team schedule for the &quot;</span> <span class="o">+</span>\n    <span class="s2">&quot;2024 Texas Tech baseball team (ID: 573916).&quot;</span>\n<span class="p">)</span>\n<span class="n">df</span> <span class="o">=</span> <span class="n">get_baseball_team_schedule</span><span class="p">(</span><span class="mi">573916</span><span class="p">)</span>\n<span class="nb">print</span><span class="p">(</span><span class="n">df</span><span class="p">)</span>\n\n<span class="c1"># Get the team schedule for the</span>\n<span class="c1"># 2023 Emporia St. baseball team (D2, ID: 548561).</span>\n<span class="nb">print</span><span class="p">(</span>\n    <span class="s2">&quot;Get the team schedule for the &quot;</span> <span class="o">+</span>\n    <span class="s2">&quot;2023 Emporia St. baseball team (D2, ID: 548561).&quot;</span>\n<span class="p">)</span>\n<span class="n">df</span> <span class="o">=</span> <span class="n">get_baseball_team_schedule</span><span class="p">(</span><span class="mi">548561</span><span class="p">)</span>\n<span class="nb">print</span><span class="p">(</span><span class="n">df</span><span class="p">)</span>\n\n<span class="c1"># Get the team schedule for the</span>\n<span class="c1"># 2022 Pfeiffer baseball team (D3, ID: 526836).</span>\n<span class="nb">print</span><span class="p">(</span>\n    <span class="s2">&quot;Get the team schedule for the &quot;</span> <span class="o">+</span>\n    <span class="s2">&quot;2022 Pfeiffer baseball team (D3, ID: 526836).&quot;</span>\n<span class="p">)</span>\n<span class="n">df</span> <span class="o">=</span> <span class="n">get_baseball_team_schedule</span><span class="p">(</span><span class="mi">526836</span><span class="p">)</span>\n<span class="nb">print</span><span class="p">(</span><span class="n">df</span><span class="p">)</span>\n</code></pre>\n</div>\n\n<h2 id="returns">Returns</h2>\n\n<p>A pandas <code>DataFrame</code> object with an NCAA baseball team\'s schedule.</p>\n',
      signature:
        '<span class="signature pdoc-code condensed">(<span class="param"><span class="n">team_id</span><span class="p">:</span> <span class="nb">int</span></span><span class="return-annotation">) -> <span class="n">pandas</span><span class="o">.</span><span class="n">core</span><span class="o">.</span><span class="n">frame</span><span class="o">.</span><span class="n">DataFrame</span>:</span></span>',
      funcdef: "def",
    },
    {
      fullname: "ncaa_stats_py.baseball.get_baseball_day_schedule",
      modulename: "ncaa_stats_py.baseball",
      qualname: "get_baseball_day_schedule",
      kind: "function",
      doc: "<p></p>\n",
      signature:
        '<span class="signature pdoc-code condensed">(<span class="param"><span class="n">game_date</span><span class="p">:</span> <span class="nb">str</span> <span class="o">|</span> <span class="n">datetime</span><span class="o">.</span><span class="n">date</span> <span class="o">|</span> <span class="n">datetime</span><span class="o">.</span><span class="n">datetime</span></span><span class="return-annotation">):</span></span>',
      funcdef: "def",
    },
    {
      fullname: "ncaa_stats_py.baseball.get_full_baseball_schedule",
      modulename: "ncaa_stats_py.baseball",
      qualname: "get_full_baseball_schedule",
      kind: "function",
      doc: '<p>Retrieves a full baseball schedule, from an NCAA level ("I", "II", "IIIs").\nThe way this is done is by going through every team in a division,\nand parsing the schedules of every team in a division.</p>\n\n<p>This function will take time when first run (30-60 minutes)!\nYou have been warned.</p>\n\n<h2 id="parameters">Parameters</h2>\n\n<p><code>season</code> (int, mandatory):\n    Specifies the season you want a schedule from.</p>\n\n<p><code>level</code> (int | str, mandatory):\n    Specifies the team you want a schedule from.</p>\n\n<h2 id="usage">Usage</h2>\n\n<div class="pdoc-code codehilite">\n<pre><span></span><code><span class="kn">from</span> <span class="nn">ncaa_stats_py.baseball</span> <span class="kn">import</span> <span class="n">get_full_baseball_schedule</span>\n\n<span class="c1"># Get the entire 2024 schedule for the 2024 D1 baseball season.</span>\n<span class="nb">print</span><span class="p">(</span><span class="s2">&quot;Get the entire 2024 schedule for the 2024 D1 baseball season.&quot;</span><span class="p">)</span>\n<span class="n">df</span> <span class="o">=</span> <span class="n">get_full_baseball_schedule</span><span class="p">(</span><span class="n">season</span><span class="o">=</span><span class="mi">2024</span><span class="p">,</span> <span class="n">level</span><span class="o">=</span><span class="s2">&quot;I&quot;</span><span class="p">)</span>\n<span class="nb">print</span><span class="p">(</span><span class="n">df</span><span class="p">)</span>\n\n<span class="c1"># You can also input `level` as an integer.</span>\n<span class="c1"># In addition, this and other functions cache data,</span>\n<span class="c1"># so this should load very quickly</span>\n<span class="c1"># compared to the first run of this function.</span>\n<span class="nb">print</span><span class="p">(</span><span class="s2">&quot;You can also input `level` as an integer.&quot;</span><span class="p">)</span>\n<span class="nb">print</span><span class="p">(</span>\n    <span class="s2">&quot;In addition, this and other functions cache data, &quot;</span>\n    <span class="o">+</span> <span class="s2">&quot;so this should load very quickly &quot;</span>\n    <span class="o">+</span> <span class="s2">&quot;compared to the first run of this function.&quot;</span>\n<span class="p">)</span>\n<span class="n">df</span> <span class="o">=</span> <span class="n">get_full_baseball_schedule</span><span class="p">(</span><span class="n">season</span><span class="o">=</span><span class="mi">2024</span><span class="p">,</span> <span class="n">level</span><span class="o">=</span><span class="mi">1</span><span class="p">)</span>\n<span class="nb">print</span><span class="p">(</span><span class="n">df</span><span class="p">)</span>\n</code></pre>\n</div>\n\n<h2 id="returns">Returns</h2>\n\n<p>A pandas <code>DataFrame</code> object with an NCAA baseball\nschedule for a specific season and level.</p>\n',
      signature:
        '<span class="signature pdoc-code condensed">(<span class="param"><span class="n">season</span><span class="p">:</span> <span class="nb">int</span>, </span><span class="param"><span class="n">level</span><span class="p">:</span> <span class="nb">str</span> <span class="o">|</span> <span class="nb">int</span> <span class="o">=</span> <span class="s1">&#39;I&#39;</span></span><span class="return-annotation">) -> <span class="n">pandas</span><span class="o">.</span><span class="n">core</span><span class="o">.</span><span class="n">frame</span><span class="o">.</span><span class="n">DataFrame</span>:</span></span>',
      funcdef: "def",
    },
    {
      fullname: "ncaa_stats_py.baseball.get_baseball_team_roster",
      modulename: "ncaa_stats_py.baseball",
      qualname: "get_baseball_team_roster",
      kind: "function",
      doc: '<p>Retrieves a baseball team\'s roster from a given team ID.</p>\n\n<h2 id="parameters">Parameters</h2>\n\n<p><code>team_id</code> (int, mandatory):\n    Required argument.\n    Specifies the team you want a roster from.\n    This is separate from a school ID, which identifies the institution.\n    A team ID should be unique to a school, and a season.</p>\n\n<h2 id="usage">Usage</h2>\n\n<div class="pdoc-code codehilite">\n<pre><span></span><code><span class="kn">from</span> <span class="nn">ncaa_stats_py.baseball</span> <span class="kn">import</span> <span class="n">get_baseball_team_roster</span>\n\n<span class="c1"># Get the roster of the 2024 Stetson baseball team (D1, ID: 574223).</span>\n<span class="nb">print</span><span class="p">(</span>\n    <span class="s2">&quot;Get the roster of the 2024 Stetson baseball team (D1, ID: 574223).&quot;</span>\n<span class="p">)</span>\n<span class="n">df</span> <span class="o">=</span> <span class="n">get_baseball_team_roster</span><span class="p">(</span><span class="n">team_id</span><span class="o">=</span><span class="mi">574223</span><span class="p">)</span>\n<span class="nb">print</span><span class="p">(</span><span class="n">df</span><span class="p">)</span>\n\n<span class="c1"># Get the roster of the 2023 Findlay baseball team (D2, ID: 548310).</span>\n<span class="nb">print</span><span class="p">(</span>\n    <span class="s2">&quot;Get the roster of the 2023 Findlay baseball team (D2, ID: 548310).&quot;</span>\n<span class="p">)</span>\n<span class="n">df</span> <span class="o">=</span> <span class="n">get_baseball_team_roster</span><span class="p">(</span><span class="n">team_id</span><span class="o">=</span><span class="mi">548310</span><span class="p">)</span>\n<span class="nb">print</span><span class="p">(</span><span class="n">df</span><span class="p">)</span>\n\n\n<span class="c1"># Get the roster of the 2022 Mary Baldwin baseball team (D3, ID: 534084).</span>\n<span class="nb">print</span><span class="p">(</span>\n    <span class="s2">&quot;Get the roster of the 2022 Mary Baldwin &quot;</span> <span class="o">+</span>\n    <span class="s2">&quot;baseball team (D3, ID: 534084).&quot;</span>\n<span class="p">)</span>\n<span class="n">df</span> <span class="o">=</span> <span class="n">get_baseball_team_roster</span><span class="p">(</span><span class="n">team_id</span><span class="o">=</span><span class="mi">534084</span><span class="p">)</span>\n<span class="nb">print</span><span class="p">(</span><span class="n">df</span><span class="p">)</span>\n</code></pre>\n</div>\n\n<h2 id="returns">Returns</h2>\n\n<p>A pandas <code>DataFrame</code> object with\nan NCAA baseball team\'s roster for that season.</p>\n',
      signature:
        '<span class="signature pdoc-code condensed">(<span class="param"><span class="n">team_id</span><span class="p">:</span> <span class="nb">int</span></span><span class="return-annotation">) -> <span class="n">pandas</span><span class="o">.</span><span class="n">core</span><span class="o">.</span><span class="n">frame</span><span class="o">.</span><span class="n">DataFrame</span>:</span></span>',
      funcdef: "def",
    },
    {
      fullname:
        "ncaa_stats_py.baseball.get_baseball_player_season_batting_stats",
      modulename: "ncaa_stats_py.baseball",
      qualname: "get_baseball_player_season_batting_stats",
      kind: "function",
      doc: '<p>Given a team ID, this function retrieves and parses\nthe season batting stats for all of the players in a given baseball team.</p>\n\n<h2 id="parameters">Parameters</h2>\n\n<p><code>team_id</code> (int, mandatory):\n    Required argument.\n    Specifies the team you want batting stats from.\n    This is separate from a school ID, which identifies the institution.\n    A team ID should be unique to a school, and a season.</p>\n\n<h2 id="usage">Usage</h2>\n\n<div class="pdoc-code codehilite">\n<pre><span></span><code><span class="kn">from</span> <span class="nn">ncaa_stats_py.baseball</span> <span class="kn">import</span> <span class="n">get_baseball_player_season_batting_stats</span>\n\n<span class="c1"># Get the season batting stats of</span>\n<span class="c1"># the 2024 Stetson baseball team (D1, ID: 574077).</span>\n<span class="nb">print</span><span class="p">(</span>\n    <span class="s2">&quot;Get the season batting stats of the &quot;</span> <span class="o">+</span>\n    <span class="s2">&quot;2024 Gonzaga baseball team (D1, ID: 574077).&quot;</span>\n<span class="p">)</span>\n<span class="n">df</span> <span class="o">=</span> <span class="n">get_baseball_player_season_batting_stats</span><span class="p">(</span><span class="n">team_id</span><span class="o">=</span><span class="mi">574077</span><span class="p">)</span>\n<span class="nb">print</span><span class="p">(</span><span class="n">df</span><span class="p">)</span>\n\n<span class="c1"># Get the season batting stats of</span>\n<span class="c1"># the 2023 Lock Haven baseball team (D2, ID: 548493).</span>\n<span class="nb">print</span><span class="p">(</span>\n    <span class="s2">&quot;Get the season batting stats of the &quot;</span> <span class="o">+</span>\n    <span class="s2">&quot;2023 Findlay baseball team (D2, ID: 548493).&quot;</span>\n<span class="p">)</span>\n<span class="n">df</span> <span class="o">=</span> <span class="n">get_baseball_player_season_batting_stats</span><span class="p">(</span><span class="n">team_id</span><span class="o">=</span><span class="mi">548493</span><span class="p">)</span>\n<span class="nb">print</span><span class="p">(</span><span class="n">df</span><span class="p">)</span>\n\n<span class="c1"># Get the season batting stats of</span>\n<span class="c1"># the 2022 Moravian baseball team (D3, ID: 526838).</span>\n<span class="nb">print</span><span class="p">(</span>\n    <span class="s2">&quot;Get the season batting stats of the &quot;</span> <span class="o">+</span>\n    <span class="s2">&quot;2022 Findlay baseball team (D2, ID: 526838).&quot;</span>\n<span class="p">)</span>\n<span class="n">df</span> <span class="o">=</span> <span class="n">get_baseball_player_season_batting_stats</span><span class="p">(</span><span class="n">team_id</span><span class="o">=</span><span class="mi">526838</span><span class="p">)</span>\n<span class="nb">print</span><span class="p">(</span><span class="n">df</span><span class="p">)</span>\n</code></pre>\n</div>\n\n<h2 id="returns">Returns</h2>\n\n<p>A pandas <code>DataFrame</code> object with the season batting stats for\nall players with a given NCAA baseball team.</p>\n',
      signature:
        '<span class="signature pdoc-code condensed">(<span class="param"><span class="n">team_id</span><span class="p">:</span> <span class="nb">int</span></span><span class="return-annotation">) -> <span class="n">pandas</span><span class="o">.</span><span class="n">core</span><span class="o">.</span><span class="n">frame</span><span class="o">.</span><span class="n">DataFrame</span>:</span></span>',
      funcdef: "def",
    },
    {
      fullname:
        "ncaa_stats_py.baseball.get_baseball_player_season_pitching_stats",
      modulename: "ncaa_stats_py.baseball",
      qualname: "get_baseball_player_season_pitching_stats",
      kind: "function",
      doc: '<p>Given a team ID, this function retrieves and parses\nthe season pitching stats for all of the players in a given baseball team.</p>\n\n<h2 id="parameters">Parameters</h2>\n\n<p><code>team_id</code> (int, mandatory):\n    Required argument.\n    Specifies the team you want pitching stats from.\n    This is separate from a school ID, which identifies the institution.\n    A team ID should be unique to a school, and a season.</p>\n\n<h2 id="usage">Usage</h2>\n\n<div class="pdoc-code codehilite">\n<pre><span></span><code><span class="kn">from</span> <span class="nn">ncaa_stats_py.baseball</span> <span class="kn">import</span> <span class="p">(</span>\n    <span class="n">get_baseball_player_season_pitching_stats</span>\n<span class="p">)</span>\n\n<span class="c1"># Get the season pitching stats of</span>\n<span class="c1"># the 2024 Minnesota baseball team (D1, ID: 574129).</span>\n<span class="nb">print</span><span class="p">(</span>\n    <span class="s2">&quot;Get the season pitching stats of the &quot;</span> <span class="o">+</span>\n    <span class="s2">&quot;2024 Minnesota baseball team (D1, ID: 574129).&quot;</span>\n<span class="p">)</span>\n<span class="n">df</span> <span class="o">=</span> <span class="n">get_baseball_player_season_pitching_stats</span><span class="p">(</span><span class="n">team_id</span><span class="o">=</span><span class="mi">574129</span><span class="p">)</span>\n<span class="nb">print</span><span class="p">(</span><span class="n">df</span><span class="p">)</span>\n<span class="mi">1</span>\n<span class="c1"># Get the season pitching stats of</span>\n<span class="c1"># the 2023 Missouri S&amp;T baseball team (D2, ID: 548504).</span>\n<span class="nb">print</span><span class="p">(</span>\n    <span class="s2">&quot;Get the season pitching stats of the &quot;</span> <span class="o">+</span>\n    <span class="s2">&quot;2023 Missouri S&amp;T baseball team (D2, ID: 548504).&quot;</span>\n<span class="p">)</span>\n<span class="n">df</span> <span class="o">=</span> <span class="n">get_baseball_player_season_pitching_stats</span><span class="p">(</span><span class="n">team_id</span><span class="o">=</span><span class="mi">548504</span><span class="p">)</span>\n<span class="nb">print</span><span class="p">(</span><span class="n">df</span><span class="p">)</span>\n\n<span class="c1"># Get the season pitching stats of</span>\n<span class="c1"># the 2022 Rowan baseball team (D3, ID: 527161).</span>\n<span class="nb">print</span><span class="p">(</span>\n    <span class="s2">&quot;Get the season pitching stats of the &quot;</span> <span class="o">+</span>\n    <span class="s2">&quot;2022 Rowan baseball team (D2, ID: 527161).&quot;</span>\n<span class="p">)</span>\n<span class="n">df</span> <span class="o">=</span> <span class="n">get_baseball_player_season_pitching_stats</span><span class="p">(</span><span class="n">team_id</span><span class="o">=</span><span class="mi">527161</span><span class="p">)</span>\n<span class="nb">print</span><span class="p">(</span><span class="n">df</span><span class="p">)</span>\n</code></pre>\n</div>\n\n<h2 id="returns">Returns</h2>\n\n<p>A pandas <code>DataFrame</code> object with the season pitching stats for\nall players with a given NCAA baseball team.</p>\n',
      signature:
        '<span class="signature pdoc-code condensed">(<span class="param"><span class="n">team_id</span><span class="p">:</span> <span class="nb">int</span></span><span class="return-annotation">) -> <span class="n">pandas</span><span class="o">.</span><span class="n">core</span><span class="o">.</span><span class="n">frame</span><span class="o">.</span><span class="n">DataFrame</span>:</span></span>',
      funcdef: "def",
    },
    {
      fullname:
        "ncaa_stats_py.baseball.get_baseball_player_season_fielding_stats",
      modulename: "ncaa_stats_py.baseball",
      qualname: "get_baseball_player_season_fielding_stats",
      kind: "function",
      doc: '<p>Given a team ID, this function retrieves and parses\nthe season fielding stats for all of the players in a given baseball team.</p>\n\n<h2 id="parameters">Parameters</h2>\n\n<p><code>team_id</code> (int, mandatory):\n    Required argument.\n    Specifies the team you want fielding stats from.\n    This is separate from a school ID, which identifies the institution.\n    A team ID should be unique to a school, and a season.</p>\n\n<h2 id="usage">Usage</h2>\n\n<div class="pdoc-code codehilite">\n<pre><span></span><code><span class="kn">from</span> <span class="nn">ncaa_stats_py.baseball</span> <span class="kn">import</span> <span class="p">(</span>\n    <span class="n">get_baseball_player_season_fielding_stats</span>\n<span class="p">)</span>\n\n<span class="c1"># Get the season fielding stats of</span>\n<span class="c1"># the 2024 South Florida (USF) baseball team (D1, ID: 574210).</span>\n<span class="nb">print</span><span class="p">(</span>\n    <span class="s2">&quot;Get the season fielding stats of the &quot;</span> <span class="o">+</span>\n    <span class="s2">&quot;2024 South Florida (USF) baseball team (D1, ID: 574210).&quot;</span>\n<span class="p">)</span>\n<span class="n">df</span> <span class="o">=</span> <span class="n">get_baseball_player_season_fielding_stats</span><span class="p">(</span><span class="n">team_id</span><span class="o">=</span><span class="mi">574210</span><span class="p">)</span>\n<span class="nb">print</span><span class="p">(</span><span class="n">df</span><span class="p">)</span>\n\n<span class="c1"># Get the season fielding stats of</span>\n<span class="c1"># the 2023 Wingate baseball team (D2, ID: 548345).</span>\n<span class="nb">print</span><span class="p">(</span>\n    <span class="s2">&quot;Get the season fielding stats of the &quot;</span> <span class="o">+</span>\n    <span class="s2">&quot;2023 Wingate baseball team (D2, ID: 548345).&quot;</span>\n<span class="p">)</span>\n<span class="n">df</span> <span class="o">=</span> <span class="n">get_baseball_player_season_fielding_stats</span><span class="p">(</span><span class="n">team_id</span><span class="o">=</span><span class="mi">548345</span><span class="p">)</span>\n<span class="nb">print</span><span class="p">(</span><span class="n">df</span><span class="p">)</span>\n\n<span class="c1"># Get the season fielding stats of</span>\n<span class="c1"># the 2022 Texas-Dallas baseball team (D3, ID: 527042).</span>\n<span class="nb">print</span><span class="p">(</span>\n    <span class="s2">&quot;Get the season fielding stats of the &quot;</span> <span class="o">+</span>\n    <span class="s2">&quot;2022 Texas-Dallas baseball team (D2, ID: 527042).&quot;</span>\n<span class="p">)</span>\n<span class="n">df</span> <span class="o">=</span> <span class="n">get_baseball_player_season_fielding_stats</span><span class="p">(</span><span class="n">team_id</span><span class="o">=</span><span class="mi">527042</span><span class="p">)</span>\n<span class="nb">print</span><span class="p">(</span><span class="n">df</span><span class="p">)</span>\n</code></pre>\n</div>\n\n<h2 id="returns">Returns</h2>\n\n<p>A pandas <code>DataFrame</code> object with the season fielding stats for\nall players with a given NCAA baseball team.</p>\n',
      signature:
        '<span class="signature pdoc-code condensed">(<span class="param"><span class="n">team_id</span><span class="p">:</span> <span class="nb">int</span></span><span class="return-annotation">) -> <span class="n">pandas</span><span class="o">.</span><span class="n">core</span><span class="o">.</span><span class="n">frame</span><span class="o">.</span><span class="n">DataFrame</span>:</span></span>',
      funcdef: "def",
    },
    {
      fullname: "ncaa_stats_py.baseball.get_baseball_player_game_batting_stats",
      modulename: "ncaa_stats_py.baseball",
      qualname: "get_baseball_player_game_batting_stats",
      kind: "function",
      doc: '<p>Given a valid player ID and season,\nthis function retrieves the batting stats for this player at a game level.</p>\n\n<h2 id="parameters">Parameters</h2>\n\n<p><code>player_id</code> (int, mandatory):\n    Required argument.\n    Specifies the player you want batting stats from.</p>\n\n<p><code>season</code> (int, mandatory):\n    Required argument.\n    Specifies the season you want batting stats from.</p>\n\n<h2 id="usage">Usage</h2>\n\n<div class="pdoc-code codehilite">\n<pre><span></span><code><span class="kn">from</span> <span class="nn">ncaa_stats_py.baseball</span> <span class="kn">import</span> <span class="p">(</span>\n    <span class="n">get_baseball_player_game_batting_stats</span>\n<span class="p">)</span>\n\n<span class="c1"># Get the batting stats of Jacob Berry in 2022 (LSU).</span>\n<span class="nb">print</span><span class="p">(</span>\n    <span class="s2">&quot;Get the batting stats of Jacob Berry in 2022 (LSU).&quot;</span>\n<span class="p">)</span>\n<span class="n">df</span> <span class="o">=</span> <span class="n">get_baseball_player_game_batting_stats</span><span class="p">(</span><span class="n">player_id</span><span class="o">=</span><span class="mi">7579336</span><span class="p">,</span> <span class="n">season</span><span class="o">=</span><span class="mi">2022</span><span class="p">)</span>\n<span class="nb">print</span><span class="p">(</span><span class="n">df</span><span class="p">)</span>\n\n<span class="c1"># Get the batting stats of Alec Burleson in 2019 (ECU).</span>\n<span class="nb">print</span><span class="p">(</span>\n    <span class="s2">&quot;Get the batting stats of Alec Burleson in 2019 (ECU).&quot;</span>\n<span class="p">)</span>\n<span class="n">df</span> <span class="o">=</span> <span class="n">get_baseball_player_game_batting_stats</span><span class="p">(</span><span class="n">player_id</span><span class="o">=</span><span class="mi">6015715</span><span class="p">,</span> <span class="n">season</span><span class="o">=</span><span class="mi">2019</span><span class="p">)</span>\n<span class="nb">print</span><span class="p">(</span><span class="n">df</span><span class="p">)</span>\n\n<span class="c1"># Get the batting stats of Hunter Bishop in 2018 (Arizona St.).</span>\n<span class="nb">print</span><span class="p">(</span>\n    <span class="s2">&quot;Get the batting stats of Hunter Bishop in 2018 (Arizona St.).&quot;</span>\n<span class="p">)</span>\n<span class="n">df</span> <span class="o">=</span> <span class="n">get_baseball_player_game_batting_stats</span><span class="p">(</span><span class="n">player_id</span><span class="o">=</span><span class="mi">6014052</span><span class="p">,</span> <span class="n">season</span><span class="o">=</span><span class="mi">2019</span><span class="p">)</span>\n<span class="nb">print</span><span class="p">(</span><span class="n">df</span><span class="p">)</span>\n</code></pre>\n</div>\n\n<h2 id="returns">Returns</h2>\n\n<p>A pandas <code>DataFrame</code> object with a player\'s batting game logs\nin a given season.</p>\n',
      signature:
        '<span class="signature pdoc-code condensed">(<span class="param"><span class="n">player_id</span><span class="p">:</span> <span class="nb">int</span>, </span><span class="param"><span class="n">season</span><span class="p">:</span> <span class="nb">int</span></span><span class="return-annotation">) -> <span class="n">pandas</span><span class="o">.</span><span class="n">core</span><span class="o">.</span><span class="n">frame</span><span class="o">.</span><span class="n">DataFrame</span>:</span></span>',
      funcdef: "def",
    },
    {
      fullname:
        "ncaa_stats_py.baseball.get_baseball_player_game_pitching_stats",
      modulename: "ncaa_stats_py.baseball",
      qualname: "get_baseball_player_game_pitching_stats",
      kind: "function",
      doc: '<p>Given a valid player ID and season,\nthis function retrieves the pitching stats for this player at a game level.</p>\n\n<h2 id="parameters">Parameters</h2>\n\n<p><code>player_id</code> (int, mandatory):\n    Required argument.\n    Specifies the player you want pitching stats from.</p>\n\n<p><code>season</code> (int, mandatory):\n    Required argument.\n    Specifies the season you want pitching stats from.</p>\n\n<h2 id="usage">Usage</h2>\n\n<div class="pdoc-code codehilite">\n<pre><span></span><code><span class="kn">from</span> <span class="nn">ncaa_stats_py.baseball</span> <span class="kn">import</span> <span class="p">(</span>\n    <span class="n">get_baseball_player_game_pitching_stats</span>\n<span class="p">)</span>\n\n<span class="c1"># Get the pitching stats of Jack Leiter in 2021 (Vanderbilt).</span>\n<span class="nb">print</span><span class="p">(</span>\n    <span class="s2">&quot;Get the pitching stats of Jack Leiter in 2021 (Vanderbilt).&quot;</span>\n<span class="p">)</span>\n<span class="n">df</span> <span class="o">=</span> <span class="n">get_baseball_player_game_pitching_stats</span><span class="p">(</span>\n    <span class="n">player_id</span><span class="o">=</span><span class="mi">6611721</span><span class="p">,</span>\n    <span class="n">season</span><span class="o">=</span><span class="mi">2021</span>\n<span class="p">)</span>\n<span class="nb">print</span><span class="p">(</span><span class="n">df</span><span class="p">)</span>\n\n<span class="c1"># Get the pitching stats of Kumar Rocker in 2020 (Vanderbilt).</span>\n<span class="nb">print</span><span class="p">(</span>\n    <span class="s2">&quot;Get the pitching stats of Kumar Rocker in 2020 (Vanderbilt).&quot;</span>\n<span class="p">)</span>\n<span class="n">df</span> <span class="o">=</span> <span class="n">get_baseball_player_game_pitching_stats</span><span class="p">(</span>\n    <span class="n">player_id</span><span class="o">=</span><span class="mi">6552352</span><span class="p">,</span>\n    <span class="n">season</span><span class="o">=</span><span class="mi">2020</span>\n<span class="p">)</span>\n<span class="nb">print</span><span class="p">(</span><span class="n">df</span><span class="p">)</span>\n\n<span class="c1"># Get the pitching stats of Garrett Crochet in 2018 (Tennessee).</span>\n<span class="nb">print</span><span class="p">(</span>\n    <span class="s2">&quot;Get the pitching stats of Garrett Crochet in 2018 (Tennessee).&quot;</span>\n<span class="p">)</span>\n<span class="n">df</span> <span class="o">=</span> <span class="n">get_baseball_player_game_pitching_stats</span><span class="p">(</span>\n    <span class="n">player_id</span><span class="o">=</span><span class="mi">5641886</span><span class="p">,</span>\n    <span class="n">season</span><span class="o">=</span><span class="mi">2018</span>\n<span class="p">)</span>\n<span class="nb">print</span><span class="p">(</span><span class="n">df</span><span class="p">)</span>\n</code></pre>\n</div>\n\n<h2 id="returns">Returns</h2>\n\n<p>A pandas <code>DataFrame</code> object with a player\'s pitching game logs\nin a given season.</p>\n',
      signature:
        '<span class="signature pdoc-code condensed">(<span class="param"><span class="n">player_id</span><span class="p">:</span> <span class="nb">int</span>, </span><span class="param"><span class="n">season</span><span class="p">:</span> <span class="nb">int</span></span><span class="return-annotation">) -> <span class="n">pandas</span><span class="o">.</span><span class="n">core</span><span class="o">.</span><span class="n">frame</span><span class="o">.</span><span class="n">DataFrame</span>:</span></span>',
      funcdef: "def",
    },
    {
      fullname:
        "ncaa_stats_py.baseball.get_baseball_player_game_fielding_stats",
      modulename: "ncaa_stats_py.baseball",
      qualname: "get_baseball_player_game_fielding_stats",
      kind: "function",
      doc: '<p>Given a valid player ID and season,\nthis function retrieves the fielding stats for this player at a game level.</p>\n\n<h2 id="parameters">Parameters</h2>\n\n<p><code>player_id</code> (int, mandatory):\n    Required argument.\n    Specifies the player you want fielding stats from.</p>\n\n<p><code>season</code> (int, mandatory):\n    Required argument.\n    Specifies the season you want fielding stats from.</p>\n\n<h2 id="usage">Usage</h2>\n\n<div class="pdoc-code codehilite">\n<pre><span></span><code><span class="kn">from</span> <span class="nn">ncaa_stats_py.baseball</span> <span class="kn">import</span> <span class="p">(</span>\n    <span class="n">get_baseball_player_game_fielding_stats</span>\n<span class="p">)</span>\n\n<span class="c1"># Get the fielding stats of Hunter Dorraugh in 2024 (San Jose St.).</span>\n<span class="nb">print</span><span class="p">(</span>\n    <span class="s2">&quot;Get the fielding stats of Hunter Dorraugh in 2024 (San Jose St.).&quot;</span>\n<span class="p">)</span>\n<span class="n">df</span> <span class="o">=</span> <span class="n">get_baseball_player_game_fielding_stats</span><span class="p">(</span>\n    <span class="n">player_id</span><span class="o">=</span><span class="mi">8271037</span><span class="p">,</span>\n    <span class="n">season</span><span class="o">=</span><span class="mi">2024</span>\n<span class="p">)</span>\n<span class="nb">print</span><span class="p">(</span><span class="n">df</span><span class="p">)</span>\n\n<span class="c1"># Get the fielding stats of Matt Bathauer in 2023 (Adams St., DII).</span>\n<span class="nb">print</span><span class="p">(</span>\n    <span class="s2">&quot;Get the fielding stats of Matt Bathauer in 2023 (Adams St., DII).&quot;</span>\n<span class="p">)</span>\n<span class="n">df</span> <span class="o">=</span> <span class="n">get_baseball_player_game_fielding_stats</span><span class="p">(</span>\n    <span class="n">player_id</span><span class="o">=</span><span class="mi">7833458</span><span class="p">,</span>\n    <span class="n">season</span><span class="o">=</span><span class="mi">2023</span>\n<span class="p">)</span>\n<span class="nb">print</span><span class="p">(</span><span class="n">df</span><span class="p">)</span>\n\n<span class="c1"># Get the fielding stats of Paul Hamilton in 2022 (Saint Elizabeth, DIII).</span>\n<span class="nb">print</span><span class="p">(</span>\n    <span class="s2">&quot;Get the fielding stats of Paul Hamilton in 2022 &quot;</span> <span class="o">+</span>\n    <span class="s2">&quot;(Saint Elizabeth, DIII).&quot;</span>\n<span class="p">)</span>\n<span class="n">df</span> <span class="o">=</span> <span class="n">get_baseball_player_game_fielding_stats</span><span class="p">(</span>\n    <span class="n">player_id</span><span class="o">=</span><span class="mi">7581440</span><span class="p">,</span>\n    <span class="n">season</span><span class="o">=</span><span class="mi">2022</span>\n<span class="p">)</span>\n<span class="nb">print</span><span class="p">(</span><span class="n">df</span><span class="p">)</span>\n</code></pre>\n</div>\n\n<h2 id="returns">Returns</h2>\n\n<p>A pandas <code>DataFrame</code> object with a player\'s fielding game logs\nin a given season.</p>\n',
      signature:
        '<span class="signature pdoc-code condensed">(<span class="param"><span class="n">player_id</span><span class="p">:</span> <span class="nb">int</span>, </span><span class="param"><span class="n">season</span><span class="p">:</span> <span class="nb">int</span></span><span class="return-annotation">) -> <span class="n">pandas</span><span class="o">.</span><span class="n">core</span><span class="o">.</span><span class="n">frame</span><span class="o">.</span><span class="n">DataFrame</span>:</span></span>',
      funcdef: "def",
    },
    {
      fullname: "ncaa_stats_py.baseball.get_baseball_game_player_stats",
      modulename: "ncaa_stats_py.baseball",
      qualname: "get_baseball_game_player_stats",
      kind: "function",
      doc: '<p>Given a valid game ID,\nthis function will attempt to get all player game stats, if possible.</p>\n\n<p>NOTE: Due to an issue with <a href="stats.ncaa.org">stats.ncaa.org</a>,\nfull player game stats may not be loaded in through this function.\nThis is a known issue, however you should be able to get position\ndata and starters information through this function</p>\n\n<h2 id="parameters">Parameters</h2>\n\n<p><code>game_id</code> (int, mandatory):\n    Required argument.\n    Specifies the game you want player game stats from.</p>\n\n<h2 id="usage">Usage</h2>\n\n<div class="pdoc-code codehilite">\n<pre><span></span><code><span class="kn">from</span> <span class="nn">ncaa_stats_py.baseball</span> <span class="kn">import</span> <span class="p">(</span>\n    <span class="n">get_baseball_game_player_stats</span>\n<span class="p">)</span>\n\n<span class="c1"># Get the player game stats of the series winning game</span>\n<span class="c1"># of the 2024 NCAA D1 Baseball Championship.</span>\n<span class="nb">print</span><span class="p">(</span>\n    <span class="s2">&quot;Get the player game stats of the series winning game &quot;</span> <span class="o">+</span>\n    <span class="s2">&quot;of the 2024 NCAA D1 Baseball Championship.&quot;</span>\n<span class="p">)</span>\n<span class="n">df</span> <span class="o">=</span> <span class="n">get_baseball_game_player_stats</span><span class="p">(</span>\n    <span class="n">game_id</span><span class="o">=</span><span class="mi">5336815</span>\n<span class="p">)</span>\n<span class="nb">print</span><span class="p">(</span><span class="n">df</span><span class="p">)</span>\n\n\n<span class="c1"># Get the player game stats of a game that occurred between Ball St.</span>\n<span class="c1"># and Lehigh on February 16th, 2024.</span>\n<span class="nb">print</span><span class="p">(</span>\n    <span class="s2">&quot;Get the player game stats of a game that occurred between Ball St. &quot;</span> <span class="o">+</span>\n    <span class="s2">&quot;and Lehigh on February 16th, 2024.&quot;</span>\n<span class="p">)</span>\n<span class="n">df</span> <span class="o">=</span> <span class="n">get_baseball_game_player_stats</span><span class="p">(</span>\n    <span class="n">game_id</span><span class="o">=</span><span class="mi">4525569</span>\n<span class="p">)</span>\n<span class="nb">print</span><span class="p">(</span><span class="n">df</span><span class="p">)</span>\n\n<span class="c1"># Get the player game stats of a game that occurred between Findlay</span>\n<span class="c1"># and Tiffin on April 10th, 2024.</span>\n<span class="nb">print</span><span class="p">(</span>\n    <span class="s2">&quot;Get the player game stats of a game that occurred between Findlay &quot;</span> <span class="o">+</span>\n    <span class="s2">&quot;and Tiffin on April 10th, 2024.&quot;</span>\n<span class="p">)</span>\n<span class="n">df</span> <span class="o">=</span> <span class="n">get_baseball_game_player_stats</span><span class="p">(</span>\n    <span class="n">game_id</span><span class="o">=</span><span class="mi">4546074</span>\n<span class="p">)</span>\n<span class="nb">print</span><span class="p">(</span><span class="n">df</span><span class="p">)</span>\n\n<span class="c1"># Get the player game stats of a game that occurred between Dean</span>\n<span class="c1"># and Anna Maria on March 30th, 2024.</span>\n<span class="nb">print</span><span class="p">(</span>\n    <span class="s2">&quot;Get the player game stats of a game that occurred between Dean &quot;</span> <span class="o">+</span>\n    <span class="s2">&quot;and Anna Maria on March 30th, 2024.&quot;</span>\n<span class="p">)</span>\n<span class="n">df</span> <span class="o">=</span> <span class="n">get_baseball_game_player_stats</span><span class="p">(</span>\n    <span class="n">game_id</span><span class="o">=</span><span class="mi">4543103</span>\n<span class="p">)</span>\n<span class="nb">print</span><span class="p">(</span><span class="n">df</span><span class="p">)</span>\n</code></pre>\n</div>\n\n<h2 id="returns">Returns</h2>\n\n<p>A pandas <code>DataFrame</code> object with a player game stats in a given game.</p>\n',
      signature:
        '<span class="signature pdoc-code condensed">(<span class="param"><span class="n">game_id</span><span class="p">:</span> <span class="nb">int</span></span><span class="return-annotation">) -> <span class="n">pandas</span><span class="o">.</span><span class="n">core</span><span class="o">.</span><span class="n">frame</span><span class="o">.</span><span class="n">DataFrame</span>:</span></span>',
      funcdef: "def",
    },
    {
      fullname: "ncaa_stats_py.baseball.get_baseball_game_team_stats",
      modulename: "ncaa_stats_py.baseball",
      qualname: "get_baseball_game_team_stats",
      kind: "function",
      doc: '<p>Given a valid game ID,\nthis function will attempt to get all team game stats, if possible.</p>\n\n<p>NOTE: Due to an issue with <a href="stats.ncaa.org">stats.ncaa.org</a>,\nfull team game stats may not be loaded in through this function.\nThis is a known issue.</p>\n\n<h2 id="parameters">Parameters</h2>\n\n<p><code>game_id</code> (int, mandatory):\n    Required argument.\n    Specifies the game you want team game stats from.</p>\n\n<h2 id="usage">Usage</h2>\n\n<div class="pdoc-code codehilite">\n<pre><span></span><code><span class="kn">from</span> <span class="nn">ncaa_stats_py.baseball</span> <span class="kn">import</span> <span class="p">(</span>\n    <span class="n">get_baseball_game_team_stats</span>\n<span class="p">)</span>\n\n<span class="c1"># Get the team game stats of the series winning game</span>\n<span class="c1"># of the 2024 NCAA D1 Baseball Championship.</span>\n<span class="nb">print</span><span class="p">(</span>\n    <span class="s2">&quot;Get the team game stats of the series winning game &quot;</span> <span class="o">+</span>\n    <span class="s2">&quot;of the 2024 NCAA D1 Baseball Championship.&quot;</span>\n<span class="p">)</span>\n<span class="n">df</span> <span class="o">=</span> <span class="n">get_baseball_game_team_stats</span><span class="p">(</span>\n    <span class="n">game_id</span><span class="o">=</span><span class="mi">5336815</span>\n<span class="p">)</span>\n<span class="nb">print</span><span class="p">(</span><span class="n">df</span><span class="p">)</span>\n\n<span class="c1"># Get the team game stats of a game</span>\n<span class="c1"># that occurred between Ball St.</span>\n<span class="c1"># and Lehigh on February 16th, 2024.</span>\n<span class="nb">print</span><span class="p">(</span>\n    <span class="s2">&quot;Get the team game stats of a game &quot;</span> <span class="o">+</span>\n    <span class="s2">&quot;that occurred between Ball St. &quot;</span> <span class="o">+</span>\n    <span class="s2">&quot;and Lehigh on February 16th, 2024.&quot;</span>\n<span class="p">)</span>\n<span class="n">df</span> <span class="o">=</span> <span class="n">get_baseball_game_team_stats</span><span class="p">(</span>\n    <span class="n">game_id</span><span class="o">=</span><span class="mi">4525569</span>\n<span class="p">)</span>\n<span class="nb">print</span><span class="p">(</span><span class="n">df</span><span class="p">)</span>\n\n<span class="c1"># Get the team game stats of a game</span>\n<span class="c1"># that occurred between Findlay</span>\n<span class="c1"># and Tiffin on April 10th, 2024.</span>\n<span class="nb">print</span><span class="p">(</span>\n    <span class="s2">&quot;Get the team game stats of a game &quot;</span> <span class="o">+</span>\n    <span class="s2">&quot;that occurred between Findlay &quot;</span> <span class="o">+</span>\n    <span class="s2">&quot;and Tiffin on April 10th, 2024.&quot;</span>\n<span class="p">)</span>\n<span class="n">df</span> <span class="o">=</span> <span class="n">get_baseball_game_team_stats</span><span class="p">(</span>\n    <span class="n">game_id</span><span class="o">=</span><span class="mi">4546074</span>\n<span class="p">)</span>\n<span class="nb">print</span><span class="p">(</span><span class="n">df</span><span class="p">)</span>\n\n<span class="c1"># Get the team game stats of a game</span>\n<span class="c1"># that occurred between Dean</span>\n<span class="c1"># and Anna Maria on March 30th, 2024.</span>\n<span class="nb">print</span><span class="p">(</span>\n    <span class="s2">&quot;Get the team game stats of a game &quot;</span> <span class="o">+</span>\n    <span class="s2">&quot;that occurred between Dean &quot;</span> <span class="o">+</span>\n    <span class="s2">&quot;and Anna Maria on March 30th, 2024.&quot;</span>\n<span class="p">)</span>\n<span class="n">df</span> <span class="o">=</span> <span class="n">get_baseball_game_team_stats</span><span class="p">(</span>\n    <span class="n">game_id</span><span class="o">=</span><span class="mi">4543103</span>\n<span class="p">)</span>\n<span class="nb">print</span><span class="p">(</span><span class="n">df</span><span class="p">)</span>\n</code></pre>\n</div>\n\n<h2 id="returns">Returns</h2>\n\n<p>A pandas <code>DataFrame</code> object with a team game stats in a given game.</p>\n',
      signature:
        '<span class="signature pdoc-code condensed">(<span class="param"><span class="n">game_id</span><span class="p">:</span> <span class="nb">int</span></span><span class="return-annotation">) -> <span class="n">pandas</span><span class="o">.</span><span class="n">core</span><span class="o">.</span><span class="n">frame</span><span class="o">.</span><span class="n">DataFrame</span>:</span></span>',
      funcdef: "def",
    },
    {
      fullname: "ncaa_stats_py.baseball.get_raw_baseball_game_pbp",
      modulename: "ncaa_stats_py.baseball",
      qualname: "get_raw_baseball_game_pbp",
      kind: "function",
      doc: '<p>Given a valid game ID,\nthis function will attempt to get the raw play-by-play (PBP)\ndata for that game.</p>\n\n<p>Long term goal is to get this, but for college baseball:\n<a href="https://www.retrosheet.org/datause.html">https://www.retrosheet.org/datause.html</a></p>\n\n<h2 id="parameters">Parameters</h2>\n\n<p><code>game_id</code> (int, mandatory):\n    Required argument.\n    Specifies the game you want play-by-play data (PBP) from.</p>\n\n<h2 id="usage">Usage</h2>\n\n<div class="pdoc-code codehilite">\n<pre><span></span><code><span class="kn">from</span> <span class="nn">ncaa_stats_py.baseball</span> <span class="kn">import</span> <span class="p">(</span>\n    <span class="n">get_raw_baseball_game_pbp</span>\n<span class="p">)</span>\n\n<span class="c1"># Get the raw play-by-play data of the series winning game</span>\n<span class="c1"># of the 2024 NCAA D1 Baseball Championship.</span>\n<span class="nb">print</span><span class="p">(</span>\n    <span class="s2">&quot;Get the raw play-by-play data &quot;</span> <span class="o">+</span>\n    <span class="s2">&quot;of the series winning game &quot;</span> <span class="o">+</span>\n    <span class="s2">&quot;in the 2024 NCAA D1 Baseball Championship.&quot;</span>\n<span class="p">)</span>\n<span class="n">df</span> <span class="o">=</span> <span class="n">get_raw_baseball_game_pbp</span><span class="p">(</span>\n    <span class="n">game_id</span><span class="o">=</span><span class="mi">5336815</span>\n<span class="p">)</span>\n<span class="nb">print</span><span class="p">(</span><span class="n">df</span><span class="p">)</span>\n\n<span class="c1"># Get the raw play-by-play data of a game</span>\n<span class="c1"># that occurred between Ball St.</span>\n<span class="c1"># and Lehigh on February 16th, 2024.</span>\n<span class="nb">print</span><span class="p">(</span>\n    <span class="s2">&quot;Get the raw play-by-play data of a game &quot;</span> <span class="o">+</span>\n    <span class="s2">&quot;that occurred between Ball St. &quot;</span> <span class="o">+</span>\n    <span class="s2">&quot;and Lehigh on February 16th, 2024.&quot;</span>\n<span class="p">)</span>\n<span class="n">df</span> <span class="o">=</span> <span class="n">get_raw_baseball_game_pbp</span><span class="p">(</span>\n    <span class="n">game_id</span><span class="o">=</span><span class="mi">4525569</span>\n<span class="p">)</span>\n<span class="nb">print</span><span class="p">(</span><span class="n">df</span><span class="p">)</span>\n\n<span class="c1"># Get the raw play-by-play data of a game</span>\n<span class="c1"># that occurred between Findlay</span>\n<span class="c1"># and Tiffin on April 10th, 2024.</span>\n<span class="nb">print</span><span class="p">(</span>\n    <span class="s2">&quot;Get the raw play-by-play data of a game &quot;</span> <span class="o">+</span>\n    <span class="s2">&quot;that occurred between Findlay &quot;</span> <span class="o">+</span>\n    <span class="s2">&quot;and Tiffin on April 10th, 2024.&quot;</span>\n<span class="p">)</span>\n<span class="n">df</span> <span class="o">=</span> <span class="n">get_raw_baseball_game_pbp</span><span class="p">(</span>\n    <span class="n">game_id</span><span class="o">=</span><span class="mi">4546074</span>\n<span class="p">)</span>\n<span class="nb">print</span><span class="p">(</span><span class="n">df</span><span class="p">)</span>\n\n<span class="c1"># Get the raw play-by-play data of a game</span>\n<span class="c1"># that occurred between Dean</span>\n<span class="c1"># and Anna Maria on March 30th, 2024.</span>\n<span class="nb">print</span><span class="p">(</span>\n    <span class="s2">&quot;Get the raw play-by-play data of a game &quot;</span> <span class="o">+</span>\n    <span class="s2">&quot;that occurred between Dean &quot;</span> <span class="o">+</span>\n    <span class="s2">&quot;and Anna Maria on March 30th, 2024.&quot;</span>\n<span class="p">)</span>\n<span class="n">df</span> <span class="o">=</span> <span class="n">get_raw_baseball_game_pbp</span><span class="p">(</span>\n    <span class="n">game_id</span><span class="o">=</span><span class="mi">4543103</span>\n<span class="p">)</span>\n<span class="nb">print</span><span class="p">(</span><span class="n">df</span><span class="p">)</span>\n</code></pre>\n</div>\n\n<h2 id="returns">Returns</h2>\n\n<p>A pandas <code>DataFrame</code> object with a play-by-play (PBP) data in a given game.</p>\n',
      signature:
        '<span class="signature pdoc-code condensed">(<span class="param"><span class="n">game_id</span><span class="p">:</span> <span class="nb">int</span></span><span class="return-annotation">):</span></span>',
      funcdef: "def",
    },
    {
      fullname: "ncaa_stats_py.basketball",
      modulename: "ncaa_stats_py.basketball",
      kind: "module",
      doc: "<p></p>\n",
    },
    {
      fullname: "ncaa_stats_py.field_hockey",
      modulename: "ncaa_stats_py.field_hockey",
      kind: "module",
      doc: "<p></p>\n",
    },
    {
      fullname: "ncaa_stats_py.football",
      modulename: "ncaa_stats_py.football",
      kind: "module",
      doc: "<p></p>\n",
    },
    {
      fullname: "ncaa_stats_py.hockey",
      modulename: "ncaa_stats_py.hockey",
      kind: "module",
      doc: "<p></p>\n",
    },
    {
      fullname: "ncaa_stats_py.lacrosse",
      modulename: "ncaa_stats_py.lacrosse",
      kind: "module",
      doc: "<p></p>\n",
    },
    {
      fullname: "ncaa_stats_py.soccer",
      modulename: "ncaa_stats_py.soccer",
      kind: "module",
      doc: "<p></p>\n",
    },
    {
      fullname: "ncaa_stats_py.softball",
      modulename: "ncaa_stats_py.softball",
      kind: "module",
      doc: "<p></p>\n",
    },
    {
      fullname: "ncaa_stats_py.utls",
      modulename: "ncaa_stats_py.utls",
      kind: "module",
      doc: "<p></p>\n",
    },
    {
      fullname: "ncaa_stats_py.volleyball",
      modulename: "ncaa_stats_py.volleyball",
      kind: "module",
      doc: "<p></p>\n",
    },
  ];

  // mirrored in build-search-index.js (part 1)
  // Also split on html tags. this is a cheap heuristic, but good enough.
  elasticlunr.tokenizer.setSeperator(/[\s\-.;&_'"=,()]+|<[^>]*>/);

  let searchIndex;
  if (docs._isPrebuiltIndex) {
    console.info("using precompiled search index");
    searchIndex = elasticlunr.Index.load(docs);
  } else {
    console.time("building search index");
    // mirrored in build-search-index.js (part 2)
    searchIndex = elasticlunr(function () {
      this.pipeline.remove(elasticlunr.stemmer);
      this.pipeline.remove(elasticlunr.stopWordFilter);
      this.addField("qualname");
      this.addField("fullname");
      this.addField("annotation");
      this.addField("default_value");
      this.addField("signature");
      this.addField("bases");
      this.addField("doc");
      this.setRef("fullname");
    });
    for (let doc of docs) {
      searchIndex.addDoc(doc);
    }
    console.timeEnd("building search index");
  }

  return (term) =>
    searchIndex.search(term, {
      fields: {
        qualname: { boost: 4 },
        fullname: { boost: 2 },
        annotation: { boost: 2 },
        default_value: { boost: 2 },
        signature: { boost: 2 },
        bases: { boost: 2 },
        doc: { boost: 1 },
      },
      expand: true,
    });
})();
