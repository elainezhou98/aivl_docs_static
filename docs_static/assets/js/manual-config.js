/**
 * 操作指南参数（仅电脑端静态页）
 *
 * 将某项设为 false 后，页面上所有带相同 data-manual 标记的内容会隐藏。
 *
 * includeAI — 是否有 AI（Intelliselect）
 *   - false：隐藏「进货中心 · Intelliselect」整块、快捷入口与侧栏对应项；进货中心顶部改为单张主界面图（无品类筛选并列图）。
 *   - 兼容旧键名 includeIntelliselect（若未设置 includeAI 则读此项）。
 *
 * platformMarket — 是否有平台市场
 *   - false：隐藏「平台市场」整节（含嵌套的「平台市场 · 市场汇总」）、相关快捷入口与侧栏条目；中控「平台市场」导航说明；中控配图切换为无平台市场版本。
 *
 * controlNavTaskHelp
 *   - 仅中控「顶部导航栏」里「任务与帮助：…」那一行
 *
 * balanceInitial / balancePerListing
 *   - 数值参数，见正文 data-manual-value
 * experimentRounds / roundTimeMinutes
 *   - 实验轮次与每轮时长（分钟），见正文 data-manual-value
 *
 * procurementAnalyticsTools — 是否展示进货中心内「市场分析 / 选品对比 / 仪表盘」及仪表盘下属工具
 *   - 与 includeAI 独立：可同时开启 Intelliselect 与上述板块。
 *   - 正文与导航中带 data-manual="procurementAnalytics" 的内容会随此项显示或隐藏。
 *
 * 配图路径：imageControlCenter*、imageProcurement*（见文件内注释）
 *   中控单图：有平台市场时用 imageControlCenterPlatformMarketPerformance（绩效表现）；
 *   无平台市场时用 imageControlCenterWithoutPlatformMarket。
 *   当 includeAI 与 platformMarket 均为 true 时，进货中心双图使用
 *   imageProcurement*WithIntelliselectPlatformMarket（左主界面、右品类筛选）；
 *   仅有 AI、无平台市场时，仍用 imageProcurementMain/CategoryWithIntelliselect。
 *
 * 多版本 HTML（同一正文、不同参数）：
 *   - 见项目根目录 versions.html 列表；各文件在 <html data-manual-variant="…"> 指定预设，
 *     由 assets/js/manual-apply-variant.js 在加载时合并到本配置。
 *   - 修改正文请优先改 full.html，再同步到其他版本（ai-market / ai-no-market / no-ai-market / no-ai-no-market）。
 */
window.MANUAL_CONFIG = {
  /** 是否有 AI（Intelliselect）；各 manual-*.html 由 manual-apply-variant 覆盖 */
  includeAI: true,

  /** 是否有平台市场（为 false 时整节「平台市场」不出现） */
  platformMarket: true,

  /**
   * 是否展示进货中心 · 市场分析 / 选品对比 / 仪表盘（含下属小节）。
   * full / ai-market 默认为 false；no-ai-market / no-ai-no-market 由预设改为 true。
   */
  procurementAnalyticsTools: false,

  controlNavTaskHelp: true,
  balanceInitial: 150,
  balancePerListing: 50,
  experimentRounds: 8,
  roundTimeMinutes: 15,
  imageControlCenterWithoutPlatformMarket:
    "assets/images/中控中心_无平台市场组.png",
  /** 有平台市场时中控配图：绩效表现 */
  imageControlCenterPlatformMarketPerformance:
    "assets/images/中控中心_平台市场_绩效表现.png",
  imageProcurementCategoryWithIntelliselect:
    "assets/images/进货中心_品类筛选_AI组.png",
  imageProcurementMainWithIntelliselect:
    "assets/images/进货中心_AI组.png",
  /** 同时有平台市场 + AI 时双图左栏（主界面 / 平台市场） */
  imageProcurementMainWithIntelliselectPlatformMarket:
    "assets/images/进货中心_AI组.png",
  /** 同时有平台市场 + AI 时双图右栏（品类筛选） */
  imageProcurementCategoryWithIntelliselectPlatformMarket:
    "assets/images/进货中心_品类筛选_AI组_平台市场.png",
  imageProcurementWithoutIntelliselect:
    "assets/images/进货中心_无AI_平台市场组.png",
};
