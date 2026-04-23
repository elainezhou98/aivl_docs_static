/**
 * 在 manual-config.js 之后、manual-init.js 之前执行。
 * 根据当前文档 <html data-manual-variant="…"> 合并 MANUAL_CONFIG 预设。
 */
(function () {
  var C = window.MANUAL_CONFIG;
  if (!C || typeof C !== "object") return;

  var raw = document.documentElement.getAttribute("data-manual-variant");
  var name = raw && String(raw).trim();

  var presets = {
    /** 版本 1：有 AI、有平台市场、无市场分析/选品对比/仪表盘 */
    full: {},
    /** 版本 2：有 AI、有平台市场（进货中心双图使用 ai-market 指定配图） */
    "ai-market": {
      imageProcurementMainWithIntelliselectPlatformMarket:
        "assets/images/进货中心_AI组_平台市场.png",
      imageProcurementCategoryWithIntelliselectPlatformMarket:
        "assets/images/进货中心_品类筛选_AI组_平台市场.png",
    },
    /** 版本 3：有 AI、无平台市场、无市场分析/选品对比/仪表盘 */
    "ai-no-market": { platformMarket: false },
    /** 版本 4：无 AI、无平台市场、有市场分析/选品对比/仪表盘 */
    "no-ai-no-market": {
      includeAI: false,
      platformMarket: false,
      procurementAnalyticsTools: true,
    },
    /** 版本 5：无 AI、有平台市场、有市场分析/选品对比/仪表盘 */
    "no-ai-market": { includeAI: false, procurementAnalyticsTools: true },

    // 兼容旧命名
    "no-ai": { includeAI: false, procurementAnalyticsTools: true },
    "no-platform-market": { platformMarket: false },
    minimal: {
      includeAI: false,
      platformMarket: false,
      procurementAnalyticsTools: true,
    },
  };

  if (!name || name === "full") return;
  var patch = presets[name];
  if (!patch) return;

  Object.assign(C, patch);
})();
