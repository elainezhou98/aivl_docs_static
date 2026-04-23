(function () {
  var C = window.MANUAL_CONFIG;
  if (!C || typeof C !== "object") C = {};

  var PARAM_DEFAULTS = {
    balanceInitial: 150,
    balancePerListing: 50,
    experimentRounds: 5,
    roundTimeMinutes: 20,
  };

  var CONTROL_CENTER_IMG_DEFAULTS = {
    performanceWithMarket:
      "assets/images/中控中心_平台市场_绩效表现.png",
    withoutMarket: "assets/images/中控中心_无平台市场组.png",
  };

  var PROCUREMENT_IMG_DEFAULTS = {
    categoryWithIntelliselect:
      "assets/images/进货中心_品类筛选_AI组.png",
    mainWithIntelliselect: "assets/images/进货中心_AI组.png",
    /** 同时有平台市场 + AI 时：左主界面、右品类筛选 */
    categoryWithIntelliselectPlatformMarket:
      "assets/images/进货中心_品类筛选_AI组_平台市场.png",
    mainWithIntelliselectPlatformMarket:
      "assets/images/进货中心_AI组.png",
    withoutIntelliselect:
      "assets/images/进货中心_无AI_平台市场组.png",
  };

  function strOrDefault(val, fallback) {
    if (val === undefined || val === null) return fallback;
    var s = String(val).trim();
    return s === "" ? fallback : s;
  }

  /** 是否展示 AI / Intelliselect；优先 includeAI，其次兼容旧键 includeIntelliselect */
  function includeAiEnabled() {
    if (C && Object.prototype.hasOwnProperty.call(C, "includeAI")) {
      return !!C.includeAI;
    }
    if (C && Object.prototype.hasOwnProperty.call(C, "includeIntelliselect")) {
      return !!C.includeIntelliselect;
    }
    return true;
  }

  function flag(key, defaultOn) {
    if (Object.prototype.hasOwnProperty.call(C, key)) return !!C[key];
    return defaultOn;
  }

  /** 中控单图：有平台市场为绩效表现；无平台市场为无平台市场主界面 */
  function controlCenterImageSrc() {
    var useMarket = flag("platformMarket", true);
    if (useMarket) {
      return strOrDefault(
        C.imageControlCenterPlatformMarketPerformance,
        CONTROL_CENTER_IMG_DEFAULTS.performanceWithMarket
      );
    }
    return strOrDefault(
      C.imageControlCenterWithoutPlatformMarket,
      CONTROL_CENTER_IMG_DEFAULTS.withoutMarket
    );
  }

  function applyControlCenterImage() {
    var img = document.getElementById("manualControlCenterImg");
    if (!img || img.tagName !== "IMG") return;
    img.src = controlCenterImageSrc();
  }

  /**
   * 进货中心双图 / 单图（与 MANUAL_CONFIG 一致）。
   * @param {string} dualId
   * @param {string} singleFigId
   * @param {string} mainImgId
   * @param {string} catImgId
   * @param {string} singleImgId
   */
  function applyProcurementBlock(
    dualId,
    singleFigId,
    mainImgId,
    catImgId,
    singleImgId
  ) {
    var dual = document.getElementById(dualId);
    var single = document.getElementById(singleFigId);
    var imgCat = document.getElementById(catImgId);
    var imgMain = document.getElementById(mainImgId);
    var imgSingle = document.getElementById(singleImgId);
    var hasAi = includeAiEnabled();

    if (dual && single) {
      if (hasAi) {
        dual.removeAttribute("hidden");
        single.setAttribute("hidden", "");
      } else {
        dual.setAttribute("hidden", "");
        single.removeAttribute("hidden");
      }
    }

    if (hasAi) {
      var usePlatformMarketProcurement = flag("platformMarket", true);
      var catDefault = usePlatformMarketProcurement
        ? PROCUREMENT_IMG_DEFAULTS.categoryWithIntelliselectPlatformMarket
        : PROCUREMENT_IMG_DEFAULTS.categoryWithIntelliselect;
      var mainDefault = usePlatformMarketProcurement
        ? PROCUREMENT_IMG_DEFAULTS.mainWithIntelliselectPlatformMarket
        : PROCUREMENT_IMG_DEFAULTS.mainWithIntelliselect;
      var catKey = usePlatformMarketProcurement
        ? "imageProcurementCategoryWithIntelliselectPlatformMarket"
        : "imageProcurementCategoryWithIntelliselect";
      var mainKey = usePlatformMarketProcurement
        ? "imageProcurementMainWithIntelliselectPlatformMarket"
        : "imageProcurementMainWithIntelliselect";
      if (imgCat && imgCat.tagName === "IMG") {
        imgCat.src = strOrDefault(C[catKey], catDefault);
      }
      if (imgMain && imgMain.tagName === "IMG") {
        imgMain.src = strOrDefault(C[mainKey], mainDefault);
      }
    } else if (imgSingle && imgSingle.tagName === "IMG") {
      imgSingle.src = strOrDefault(
        C.imageProcurementWithoutIntelliselect,
        PROCUREMENT_IMG_DEFAULTS.withoutIntelliselect
      );
    }
  }

  function applyProcurementImage() {
    applyProcurementBlock(
      "procurementDual",
      "procurementSingle",
      "manualProcurementImgMain",
      "manualProcurementImgCategory",
      "manualProcurementImgSingle"
    );
  }

  function paramNumber(key) {
    var def = PARAM_DEFAULTS[key];
    if (def === undefined) return null;
    if (Object.prototype.hasOwnProperty.call(C, key)) {
      var n = Number(C[key]);
      if (!isNaN(n)) return n;
    }
    return def;
  }

  function applyVisibility(key, visible) {
    document.querySelectorAll('[data-manual="' + key + '"]').forEach(function (el) {
      if (visible) el.removeAttribute("hidden");
      else el.setAttribute("hidden", "");
    });
  }

  /** 反向显示：当指定 key 为 false 时展示（如无 AI 组） */
  function applyInverseVisibility(key, visible) {
    document.querySelectorAll('[data-manual-not="' + key + '"]').forEach(function (el) {
      if (visible) el.setAttribute("hidden", "");
      else el.removeAttribute("hidden");
    });
  }

  function applyParamValues() {
    document.querySelectorAll("[data-manual-value]").forEach(function (el) {
      var key = el.getAttribute("data-manual-value");
      if (!key) return;
      var n = paramNumber(key);
      if (n === null) return;
      el.textContent = String(n);
    });
  }

  /** 左右双栏：图片加载失败或缺少 src 时隐藏图片；若有 .figure-pair__text 则同时显示备用说明 */
  function applyFigurePairFallbacks() {
    document.querySelectorAll("img.figure-pair__img").forEach(function (img) {
      var media = img.closest(".figure-pair__media");
      if (!media || media.classList.contains("figure-pair__media--text-only")) return;
      var textEl = media.querySelector(".figure-pair__text");

      function onBroken() {
        img.setAttribute("hidden", "");
        if (textEl) textEl.removeAttribute("hidden");
      }

      var src = img.getAttribute("src");
      if (!src || String(src).trim() === "") {
        onBroken();
        return;
      }

      function checkFailed() {
        if (img.naturalWidth === 0) onBroken();
      }

      if (img.complete) checkFailed();
      else
        img.addEventListener(
          "load",
          function () {
            checkFailed();
          },
          { once: true }
        );

      img.addEventListener(
        "error",
        function () {
          onBroken();
        },
        { once: true }
      );
    });
  }

  /** 侧栏目录：main 下顶级 section[id]；若某节内另有直接子 section[id]，则作为缩进子项；子节内还可再嵌套一层 section（如进货中心 · 仪表盘 → 三个工具） */
  function buildTocFromHeadings() {
    var ul = document.getElementById("navList");
    if (!ul) return;

    ul.innerHTML = "";

    var main = document.querySelector("main");
    if (!main) return;

    var sections = main.querySelectorAll(":scope > section[id]");
    sections.forEach(function (sec) {
      if (sec.getAttribute("data-toc") === "exclude") return;
      if (sec.hasAttribute("hidden")) return;
      var h2 = sec.querySelector(":scope > h2");
      if (!h2) return;
      var id = sec.id;
      if (!id) return;

      var li = document.createElement("li");
      li.className = "nav-sub";
      var dm = sec.getAttribute("data-manual");
      if (dm) li.setAttribute("data-manual", dm);

      var a = document.createElement("a");
      a.href = "#" + id;
      a.textContent = h2.textContent.trim();
      li.appendChild(a);

      var nested = sec.querySelectorAll(":scope > section[id]");
      if (nested.length) {
        var subUl = document.createElement("ul");
        subUl.className = "nav-list nav-list--nested";
        nested.forEach(function (childSec) {
          if (childSec.getAttribute("data-toc") === "exclude") return;
          if (childSec.hasAttribute("hidden")) return;
          var head = childSec.querySelector(":scope > h2, :scope > h3");
          if (!head) return;
          var cid = childSec.id;
          if (!cid) return;

          var subLi = document.createElement("li");
          subLi.className = "nav-sub nav-sub--nested";
          var dmc = childSec.getAttribute("data-manual");
          if (dmc) subLi.setAttribute("data-manual", dmc);

          var subA = document.createElement("a");
          subA.href = "#" + cid;
          subA.textContent = head.textContent.trim();
          subLi.appendChild(subA);

          var deepNested = childSec.querySelectorAll(":scope > section[id]");
          if (deepNested.length) {
            var deepUl = document.createElement("ul");
            deepUl.className = "nav-list nav-list--nested";
            deepNested.forEach(function (deepSec) {
              if (deepSec.getAttribute("data-toc") === "exclude") return;
              if (deepSec.hasAttribute("hidden")) return;
              var deepHead = deepSec.querySelector(":scope > h2, :scope > h3");
              if (!deepHead) return;
              var did = deepSec.id;
              if (!did) return;
              var deepLi = document.createElement("li");
              deepLi.className = "nav-sub nav-sub--nested";
              var dmd = deepSec.getAttribute("data-manual");
              if (dmd) deepLi.setAttribute("data-manual", dmd);
              var deepA = document.createElement("a");
              deepA.href = "#" + did;
              deepA.textContent = deepHead.textContent.trim();
              deepLi.appendChild(deepA);
              deepUl.appendChild(deepLi);
            });
            if (deepUl.children.length) subLi.appendChild(deepUl);
          }

          subUl.appendChild(subLi);
        });
        if (subUl.children.length) li.appendChild(subUl);
      }

      ul.appendChild(li);
    });
  }

  function run() {
    var hasPlatformMarket = flag("platformMarket", true);
    var hasAi = includeAiEnabled();
    var hasProcurementAnalytics = flag("procurementAnalyticsTools", false);
    applyVisibility("platformMarket", hasPlatformMarket);
    applyVisibility("includeAI", hasAi);
    applyInverseVisibility("includeAI", hasAi);
    applyVisibility("procurementAnalytics", hasProcurementAnalytics);
    applyVisibility("controlNavTaskHelp", flag("controlNavTaskHelp", true));
    applyControlCenterImage();
    applyProcurementImage();
    applyParamValues();
    applyFigurePairFallbacks();
    buildTocFromHeadings();
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", run);
  } else {
    run();
  }
})();
