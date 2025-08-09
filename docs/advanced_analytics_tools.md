# Advanced NBA Analytics Tools

このドキュメントでは、NBA 分析エージェントで使用可能な高度な分析ツールについて説明します。

## 概要

新しく実装された 8 つの高度な分析ツールにより、NBA のプロ分析チームが使用するような精密で包括的な分析が可能になりました。

さらに、選手とチームデータを組み合わせた 5 つの新しい分析ツールを追加しました。

## 1. プレイヤー比較分析 (`compare_players_advanced_metrics`)

複数のプレイヤーの高度な指標を比較分析します。

### 使用例

```python
# スコアリング指標での比較
result = compare_players_advanced_metrics(
    player_names=["LeBron James", "Kevin Durant", "Stephen Curry"],
    season_year="2023-24",
    metric_type="scoring"
)

# ディフェンス指標での比較
result = compare_players_advanced_metrics(
    player_names=["Rudy Gobert", "Bam Adebayo", "Joel Embiid"],
    metric_type="defensive"
)
```

### 分析可能な指標タイプ

- `scoring`: 得点、FG%、3P%、FT%、TS%
- `defensive`: スティール、ブロック、ディフェンスリバウンド、ファウル
- `efficiency`: TS%、分間得点、分間アシスト、分間リバウンド
- `all`: 全指標

## 2. チーム分析 (`analyze_team_performance_trends`)

チームのパフォーマンストレンドを時系列で分析します。

### 使用例

```python
# 月別分析
result = analyze_team_performance_trends(
    team_identifier="GSW",
    season_year="2023-24",
    analysis_period="month"
)

# 四半期別分析
result = analyze_team_performance_trends(
    team_identifier="1610612744",  # Team ID
    analysis_period="quarter"
)
```

### 分析期間

- `month`: 月別分析
- `quarter`: 四半期別分析
- `season`: シーズン別分析

## 3. 効率性深掘り分析 (`analyze_player_efficiency_deep_dive`)

プレイヤーの効率性を詳細に分析します。

### 使用例

```python
# 包括的分析
result = analyze_player_efficiency_deep_dive(
    player_name="Nikola Jokic",
    season_year="2023-24",
    analysis_type="comprehensive"
)

# スコアリング特化分析
result = analyze_player_efficiency_deep_dive(
    player_name="Stephen Curry",
    analysis_type="scoring"
)
```

### 分析タイプ

- `scoring`: スコアリング効率の詳細分析
- `defensive`: ディフェンス効率の詳細分析
- `comprehensive`: 包括的な効率性分析

## 4. ゲーム状況別分析 (`analyze_player_performance_by_game_situation`)

異なるゲーム状況でのプレイヤーパフォーマンスを分析します。

### 使用例

```python
# ホーム/アウェイ分析
result = analyze_player_performance_by_game_situation(
    player_name="Luka Doncic",
    situation_type="home_away"
)

# クラッチ分析
result = analyze_player_performance_by_game_situation(
    player_name="Damian Lillard",
    situation_type="clutch"
)
```

### 状況タイプ

- `home_away`: ホーム/アウェイ別分析
- `clutch`: クラッチ状況分析（5 点差以内）
- `all`: 全状況の組み合わせ分析

## 5. パフォーマンス予測 (`predict_player_performance`)

プレイヤーの将来パフォーマンスを予測します。

### 使用例

```python
# 次試合予測
result = predict_player_performance(
    player_name="Giannis Antetokounmpo",
    prediction_type="next_game",
    historical_games=20
)

# トレンド予測
result = predict_player_performance(
    player_name="Ja Morant",
    prediction_type="trend"
)
```

### 予測タイプ

- `next_game`: 次試合予測（最近 10 試合の平均）
- `season_avg`: シーズン平均予測
- `trend`: トレンドベース予測

## 6. 高度バスケットボール指標 (`calculate_advanced_basketball_metrics`)

専門的なバスケットボール指標を計算します。

### 使用例

```python
# 全指標計算
result = calculate_advanced_basketball_metrics(
    player_name="Joel Embiid",
    season_year="2023-24"
)

# 特定指標のみ計算
result = calculate_advanced_basketball_metrics(
    player_name="Chris Paul",
    metrics=["per", "ts_pct", "usage_rate"]
)
```

### 計算可能な指標

- `per`: Player Efficiency Rating
- `ts_pct`: True Shooting Percentage
- `efg_pct`: Effective Field Goal Percentage
- `usage_rate`: Usage Rate
- `defensive_impact`: Defensive Impact Score

## 7. 統計的相関分析 (`analyze_statistical_correlations`)

指標間の相関関係を分析します。

### 使用例

```python
# パフォーマンス相関
result = analyze_statistical_correlations(
    player_name="James Harden",
    correlation_type="performance"
)

# 効率性相関
result = analyze_statistical_correlations(
    player_name="Kawhi Leonard",
    correlation_type="efficiency"
)
```

### 相関タイプ

- `performance`: パフォーマンス指標間の相関
- `efficiency`: 効率性指標間の相関
- `defensive`: ディフェンス指標間の相関

## 8. プレイスタイルクラスタリング (`cluster_players_by_playing_style`)

プレイヤーをプレイスタイルで分類します。

### 使用例

```python
# 全ポジションでのクラスタリング
result = cluster_players_by_playing_style(
    season_year="2023-24",
    cluster_count=5
)

# 特定ポジションでのクラスタリング
result = cluster_players_by_playing_style(
    position="Guard",
    season_year="2023-24"
)
```

### クラスター分類

- `scorers`: 高得点、低アシストプレイヤー
- `playmakers`: 高アシスト率プレイヤー
- `rebounders`: 高リバウンド率プレイヤー
- `defenders`: 高ディフェンス活動プレイヤー
- `all_around`: バランス型プレイヤー

## 選手・チーム組み合わせ分析ツール

### 9. プレイヤーチーム影響分析 (`analyze_player_team_impact`)

プレイヤーがチームに与える影響を分析します。

### 使用例

```python
# 包括的影響分析
result = analyze_player_team_impact(
    player_name="Nikola Jokic",
    season_year="2023-24",
    impact_type="comprehensive"
)

# スコアリング影響分析
result = analyze_player_team_impact(
    player_name="Stephen Curry",
    impact_type="scoring"
)
```

### 影響分析タイプ

- `scoring`: スコアリングへの影響分析
- `defensive`: ディフェンスへの影響分析
- `comprehensive`: 包括的影響分析

### 発見可能な洞察

- プレイヤーのチーム貢献度（スコアリングシェア、ディフェンス貢献度）
- チーム効率性への影響
- プレイヤーの役割分類（プライマリースコアラー、セカンダリースコアラー、ロールプレイヤー）

### 10. ラインアップ効果分析 (`analyze_lineup_effectiveness`)

チームのラインアップ効果を分析します。

### 使用例

```python
# スコアリング効果分析
result = analyze_lineup_effectiveness(
    team_identifier="LAL",
    season_year="2023-24",
    analysis_type="scoring"
)

# ディフェンス効果分析
result = analyze_lineup_effectiveness(
    team_identifier="BOS",
    analysis_type="defensive"
)
```

### 分析タイプ

- `scoring`: スコアリング効果分析
- `defensive`: ディフェンス効果分析
- `comprehensive`: 包括的効果分析

### 発見可能な洞察

- キーコントリビューターの特定
- チーム内でのプレイヤーランキング
- 分間生産性の分析

### 11. プレイヤーチーム相乗効果分析 (`analyze_player_team_synergy`)

特定のプレイヤーとチームの相乗効果を分析します。

### 使用例

```python
# プレイヤーチーム相乗効果分析
result = analyze_player_team_synergy(
    player_name="LeBron James",
    team_identifier="LAL",
    season_year="2023-24"
)
```

### 発見可能な洞察

- プレイヤーのチーム内での役割
- 勝率への影響
- チームとの相性評価

### 12. チーム間プレイヤー活用比較 (`compare_teams_player_impact`)

異なるチームのプレイヤー活用方法を比較します。

### 使用例

```python
# スコアリング活用比較
result = compare_teams_player_impact(
    team_identifiers=["GSW", "LAL", "BOS"],
    season_year="2023-24",
    comparison_type="scoring"
)

# ディフェンス活用比較
result = compare_teams_player_impact(
    team_identifiers=["MIA", "UTA", "MIL"],
    comparison_type="defensive"
)
```

### 比較タイプ

- `scoring`: スコアリング活用比較
- `defensive`: ディフェンス活用比較
- `comprehensive`: 包括的活用比較

### 発見可能な洞察

- チームのプレイヤー活用パターン
- プライマリースコアラーの数と分布
- ディフェンスアンカーの配置

### 13. チーム攻撃効率性分析 (`analyze_team_offensive_efficiency_by_player_contribution`)

個別プレイヤーの貢献がチーム攻撃効率に与える影響を分析します。

### 使用例

```python
# チーム攻撃効率性分析
result = analyze_team_offensive_efficiency_by_player_contribution(
    team_identifier="PHX",
    season_year="2023-24"
)
```

### 発見可能な洞察

- 最も効率的なスコアラーの特定
- チーム効率性への貢献度
- プレイヤーの効率性がチーム全体に与える影響

## 高度な分析の活用例

### 1. 選手比較分析

```python
# トップスコアラーの効率性比較
result = compare_players_advanced_metrics(
    player_names=["Kevin Durant", "LeBron James", "Stephen Curry"],
    metric_type="efficiency"
)
```

### 2. チームトレンド分析

```python
# チームの月別パフォーマンス変化
result = analyze_team_performance_trends(
    team_identifier="LAL",
    analysis_period="month"
)
```

### 3. 予測分析

```python
# プレイヤーの次試合予測
result = predict_player_performance(
    player_name="Anthony Davis",
    prediction_type="next_game"
)
```

### 4. クラスタリング分析

```python
# ガードポジションのプレイスタイル分類
result = cluster_players_by_playing_style(
    position="Guard",
    season_year="2023-24"
)
```

### 5. プレイヤーチーム影響分析

```python
# プレイヤーのチームへの影響分析
result = analyze_player_team_impact(
    player_name="Joel Embiid",
    impact_type="comprehensive"
)
```

### 6. チーム間比較分析

```python
# 複数チームのプレイヤー活用比較
result = compare_teams_player_impact(
    team_identifiers=["GSW", "LAL", "BOS"],
    comparison_type="comprehensive"
)
```

## データ品質と制限事項

### データ要件

- 最低 10 試合以上のデータが必要
- シーズン指定により精度が向上
- 最新データの使用を推奨

### 制限事項

- 相関分析は統計的有意性を考慮
- 予測精度は過去データの質に依存
- クラスタリングは統計的プロファイルに基づく

## エラーハンドリング

すべてのツールは以下の形式でエラーを返します：

```python
{
    "status": "error",
    "message": "エラーの詳細説明"
}
```

成功時は以下の形式で返されます：

```python
{
    "status": "success",
    "data": {...},
    "metadata": {...}
}
```

## パフォーマンス最適化

### クエリ最適化

- 適切なインデックスの使用
- 必要最小限のデータ取得
- 効率的な集計クエリの使用

### キャッシュ戦略

- 頻繁に使用される結果のキャッシュ
- 段階的なデータ更新
- リアルタイム性と精度のバランス

これらの高度な分析ツールにより、NBA のプロ分析チームと同等レベルの精密で包括的な分析が可能になります。

## 新機能：選手・チーム組み合わせ分析

### 主な発見ポイント

1. **プレイヤーの真の価値**: チーム全体への貢献度を数値化
2. **チーム戦略の可視化**: 各チームのプレイヤー活用パターンを比較
3. **効率性の相乗効果**: プレイヤーとチームの相性を定量分析
4. **戦術的洞察**: ラインアップ効果とチーム戦略の関係性
5. **予測精度の向上**: チームコンテキストを考慮したより正確な予測

### 実用的な活用例

- **トレード分析**: プレイヤーの新チームでの予想パフォーマンス
- **戦術立案**: チームの強みと弱みの特定
- **選手評価**: チーム貢献度に基づく選手価値の評価
- **戦略比較**: 異なるチームの戦術的アプローチの比較
