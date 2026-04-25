# DV Performance Score Report

## 1. Current Stage
- Stage 2 descriptive statistics for `1_DP_feedback`.

## 2. Input and Scope
- Input file: `code/participant_evaluation/eval_runs/20260411_OnlyAI&Control/3_DP_feedback/2_feedback_detail.csv`
- Output root: `code/participant_evaluation/eval_runs/20260411_OnlyAI&Control/3_DP_feedback/dv_performance_score`
- Raw rows: 1278 (source is feedback-item level; final table is user-round level)
- User-round observations: 427
- Unique users: 61
- Rounds covered: 3, 4, 5, 6, 7, 8, 9

## 3. Ranking Rule
- Current script creates a reusable numeric `ranking` column from `0` to `8`, where `0` is the best segment.
- Because the original request listed 9 codes (`0-8`) but the textual percentile bands were not fully consistent, this script stores:
  - exact `percentile_from_top`
  - numeric `ranking`
  - text `ranking_label`
- Current label set is: top10%, 10%-20%, 20%-30%, 30%-40%, 40%-50%, 50%-60%, 60%-70%, 70%-80%, 80%-100%
- `cumulative_score_to_round`: per user, running sum of `round_score` from the earliest observed round through the current round (inclusive).
- `rank_cumulative_score_within_round`: within each round, rank all users by `cumulative_score_to_round` (1 = highest cumulative score in that round; ties broken by `username`).

## 4. Round-Level Group Summary
- Summary file: `code/participant_evaluation/eval_runs/20260411_OnlyAI&Control/3_DP_feedback/dv_performance_score/analysis/round_group_score_summary.csv`
- Significance file: `code/participant_evaluation/eval_runs/20260411_OnlyAI&Control/3_DP_feedback/dv_performance_score/analysis/round_group_significance_tests.csv`
- Per-round results:
- Round 3: Control mean=-7.302, OnlyAI mean=-4.563, Welch p=0.3829, MW p=0.3560, Levene p=0.9424
- Round 4: Control mean=0.562, OnlyAI mean=-1.828, Welch p=0.4640, MW p=0.2686, Levene p=0.2696
- Round 5: Control mean=3.318, OnlyAI mean=2.443, Welch p=0.8300, MW p=0.9079, Levene p=0.5567
- Round 6: Control mean=6.177, OnlyAI mean=7.770, Welch p=0.7535, MW p=0.6384, Levene p=0.7964
- Round 7: Control mean=6.146, OnlyAI mean=8.253, Welch p=0.6449, MW p=0.5059, Levene p=0.7163
- Round 8: Control mean=6.422, OnlyAI mean=7.103, Welch p=0.8707, MW p=0.7072, Levene p=0.9794
- Round 9: Control mean=10.901, OnlyAI mean=13.925, Welch p=0.5237, MW p=0.6233, Levene p=0.0130

## 5. Cumulative User Summary
- Per user-round cumulative score and rank-by-cumulative-score are in the last columns of `code/participant_evaluation/eval_runs/20260411_OnlyAI&Control/3_DP_feedback/dv_performance_score/derived/user_round_performance_score.csv` (`cumulative_score_to_round`, `rank_cumulative_score_within_round`).
- Separate user-level cumulative CSV: `code/participant_evaluation/eval_runs/20260411_OnlyAI&Control/3_DP_feedback/dv_performance_score/derived/user_cumulative_performance_summary.csv`
- Group summary file: `code/participant_evaluation/eval_runs/20260411_OnlyAI&Control/3_DP_feedback/dv_performance_score/analysis/cumulative_group_score_summary.csv`
- Group test file: `code/participant_evaluation/eval_runs/20260411_OnlyAI&Control/3_DP_feedback/dv_performance_score/analysis/cumulative_group_significance_tests.csv`
- Cumulative: Control mean=26.224, OnlyAI mean=33.103, Welch p=0.7509, MW p=0.5019, Levene p=0.8510

## 6. Visualization Outputs
- Run `participant_evaluation_runner.py --config participant_evaluation_config.xlsx` after this tables step to generate PNGs under `figures/`.
- Expected paths:
- `code/participant_evaluation/eval_runs/20260411_OnlyAI&Control/3_DP_feedback/dv_performance_score/figures/round_mean_score_by_group.png`
- `code/participant_evaluation/eval_runs/20260411_OnlyAI&Control/3_DP_feedback/dv_performance_score/figures/round_variance_score_by_group.png`
- `code/participant_evaluation/eval_runs/20260411_OnlyAI&Control/3_DP_feedback/dv_performance_score/figures/round_score_boxplot_by_group.png`
- `code/participant_evaluation/eval_runs/20260411_OnlyAI&Control/3_DP_feedback/dv_performance_score/figures/round_ranking_heatmap_by_group.png`
- `code/participant_evaluation/eval_runs/20260411_OnlyAI&Control/3_DP_feedback/dv_performance_score/figures/cumulative_score_boxplot_by_group.png`
- `code/participant_evaluation/eval_runs/20260411_OnlyAI&Control/3_DP_feedback/dv_performance_score/figures/cumulative_mean_variance_by_group.png`
