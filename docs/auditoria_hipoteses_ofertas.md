# Auditoria de Teste de Hipoteses para Ofertas de Recomendacao

**Data:** 2026-04-21
**Autor:** Data Scientist Agent (cmml-data-scientist)
**Escopo:** Pipeline ML completo (modelos A e B, avaliacao, geracao de ofertas)

---

## 1. Auditoria do Pipeline de Dados

### 1.1 Bug no `find_optimal_threshold` — metrica "f1" inexistente

**Arquivo:** `ml/evaluate.py:393`
**Severidade:** P0 (metricas incorretas)

O parametro `metric` tem valor padrao `"f1"`, mas o `classification_report` do sklearn
retorna a chave `"f1-score"` (com hifen). O resultado e que `report.get(pos_key, {}).get("f1", 0.0)`
retorna sempre `0.0`, e o threshold otimo nunca e de fato otimizado.

```python
# ANTES (bugado):
def find_optimal_threshold(..., metric: str = "f1") -> ...:
    score = report.get(pos_key, {}).get(metric, 0.0)  # "f1" nao existe → 0.0

# DEPOIS (corrigido):
def find_optimal_threshold(..., metric: str = "f1-score") -> ...:
    score = report.get(pos_key, {}).get(metric, 0.0)  # "f1-score" encontra o valor
```

**Impacto:** O threshold informado no log (`modelo_a_ranker.py:1108`) e sempre 0.5
(valor inicial), independente dos dados. Todas as decisoes de corte baseadas neste
threshold estao sub-otimas.

### 1.2 `save_metrics_to_db` nao persiste `auc_roc`

**Arquivo:** `ml/evaluate.py:489-508`
**Severidade:** P0 (dados incompletos)

A coluna `auc_roc` foi adicionada a `reco.evaluation_runs` via ALTER TABLE
(`sql/ddl/02_curated.sql:158-159`), mas a funcao `save_metrics_to_db()` nunca
a inclui no INSERT. O campo fica sempre NULL.

```python
# ANTES: INSERT sem auc_roc
INSERT INTO reco.evaluation_runs
    (strategy, k, precision_at_k, recall_at_k, ndcg_at_k, map_at_k, n_customers, notes)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
```

O Modelo A calcula `auc_roc` em `evaluate_binary_classifier()` e o inclui no
campo `notes` como texto, mas nao como valor numerico indexavel.

### 1.3 `hit_rate_at_k` calculada mas nunca persistida

**Arquivo:** `ml/evaluate.py:164-196` (calculo), `ml/evaluate.py:489-508` (persistencia)
**Severidade:** P0 (dados incompletos)

A funcao `evaluate_ranking()` calcula `hit_rate` para cada K e retorna no DataFrame,
mas `save_metrics_to_db()` nao inclui esta coluna no INSERT, e a tabela
`reco.evaluation_runs` nao tem a coluna `hit_rate_at_k`.

**Impacto combinado (1.2 + 1.3):** O Dashboard ML consulta `reco.evaluation_runs`
para exibir evolucao de metricas. Sem `auc_roc` e `hit_rate_at_k` persistidos,
o dashboard mostra dados incompletos e a equipe nao consegue monitorar regressoes
nestas metricas criticas.

### 1.4 Score naming confuso — `score_raw` vs `score`

**Arquivo:** `ml/generate_offers.py:831` e `ml/generate_offers.py:832-845`
**Severidade:** P1 (legibilidade e manutencao)

O pipeline de geracao de ofertas executa:

1. Scores brutos dos modelos (LightGBM probability ou votos SVD normalizados)
2. **Normalizacao por percentil** (`normalize_scores_percentile`, linha 822)
3. Salva `score_raw = score` (pos-percentil, pre-desconto lifecycle)
4. Aplica desconto sigmoide de lifecycle
5. Resultado final = `score`

Os nomes sao contra-intuitivos:
- `score_raw` **NAO e o score bruto do modelo** — e o percentil normalizado
- `score` e o valor final pos-desconto

Proposta de nomenclatura mais clara:
- `score_model` = output bruto do LightGBM/SVD
- `score_percentile` = apos normalizacao por percentil
- `score` = valor final (apos lifecycle discount)

### 1.5 Feedback window sem FK para `feedback_runs`

**Arquivo:** `sql/ddl/05_feedback.sql`
**Severidade:** P2 (integridade referencial)

A tabela `reco.offer_outcomes` nao possui FK para `reco.feedback_runs`.
Outcomes de diferentes execucoes do feedback loop (com janelas temporais distintas)
podem se misturar sem rastreabilidade de qual run gerou cada outcome.

---

## 2. Avaliacao de Randomizacao e Teste A/B

### 2.1 Ausencia total de randomizacao

**Achado:** Ambos os modelos geram ofertas para **todos** os mesmos clientes
simultaneamente. Nao existe:

- Assignment de clientes a grupos de tratamento
- Grupo de controle (holdout)
- Randomizacao a nivel de cliente

**Evidencia em `ml/generate_offers.py:773-777`:**
```python
if strategy in ("a", "both"):
    frames.append(run_model_a(pg, top_n=top_n, ...))
if strategy in ("b", "both"):
    frames.append(run_model_b(pg, top_n=top_n, ...))
```

Ambos os modelos processam `customer_ids = df_history["customer_id"].unique()`,
i.e., a populacao inteira.

### 2.2 `incremental_lift()` com denominador invalido

**Arquivo:** `ml/evaluate.py:564-628`
**Severidade:** P0 (metrica incorreta)

O calculo de `organic_rate` usa:
```sql
COUNT(DISTINCT (sc.customer_id_src, p.product_id)) AS potential_pairs
-- = CROSS JOIN de todos os clientes x todos os produtos ativos
```

Se ha 1.000 clientes e 5.000 produtos, `potential_pairs = 5.000.000`.
As compras organicas (digamos 50.000) divididas por 5M dao uma taxa de 1%.
Isso gera um lift artificialmente enorme (e.g., 15x vs 1%).

**Problema fundamental:** O denominador correto nao e o universo de pares possiveis,
mas sim a taxa de compra dos **mesmos clientes** para produtos que **nao foram oferecidos**,
na **mesma janela temporal** (cohort-matched).

### 2.3 `model_comparison_by_conversion()` nao mede causalidade

**Arquivo:** `ml/evaluate.py:631-668`

Esta funcao compara taxas de conversao entre Modelo A e Modelo B, mas como
ambos os modelos operam sobre a mesma populacao de clientes, a comparacao
observacional nao estabelece causalidade. Diferencas podem ser atribuidas a:

- Selecao: Modelo A pode recomendar produtos mais "faceis" de converter
- Confounding: Clientes que recebem ofertas dos dois modelos nao sao independentes
- Overlap: O mesmo par (cliente, produto) pode aparecer em ambos os modelos

### 2.4 Framework de A/B testing proposto

Para medir o impacto causal das recomendacoes, propomos:

1. **Assignment por cliente:** Cada cliente e atribuido a um grupo no inicio
   do experimento (hash deterministico do customer_id).

2. **Grupos:**
   - `treatment_a` (40%): recebe ofertas apenas do Modelo A
   - `treatment_b` (40%): recebe ofertas apenas do Modelo B
   - `control` (10%): nao recebe nenhuma oferta
   - `holdout` (10%): recebe ofertas aleatorias (baseline)

3. **Persistencia:** Tabela `reco.experiment_assignments` (ver `sql/ddl/07_experiments.sql`)

4. **Integracao com `generate_offers.py`:**
   - Antes de gerar ofertas, consultar assignment do cliente
   - Clientes do grupo `control` sao excluidos
   - Clientes `treatment_a` recebem apenas Model A, etc.

5. **Metricas de causalidade:**
   - Average Treatment Effect (ATE) = taxa(treatment) - taxa(control)
   - Lift causal = taxa(treatment) / taxa(control)
   - Intervalos de confianca via `compare_conversion_rates()` (nova funcao)

---

## 3. Avaliacao do Modelo de Ranking

### 3.1 LightGBM binary != modelo binomial

O Modelo A usa LightGBM com `objective="binary"` (`ml/modelo_a_ranker.py:126`).
Isso e um **classificador binario discriminativo**, nao um modelo binomial.

| Aspecto | LightGBM binary | Modelo Binomial |
|---------|-----------------|-----------------|
| Objetivo | Discriminar classes (comprou/nao comprou) | Modelar contagem de sucessos em N tentativas |
| Output | Score em [0,1] (pseudo-probabilidade) | Probabilidade calibrada p |
| Distribuicao | Nenhuma assumida | Y ~ Binomial(n, p) |
| Uso tipico | Ranking, classificacao | Testes de proporcao, power analysis |

Para testes de hipoteses (comparar taxas de conversao entre modelos), precisamos
de testes de proporcao (two-proportion z-test), nao da distribuicao binomial
diretamente no modelo de ranking.

### 3.2 Calibracao de probabilidades destruida pela normalizacao percentil

O pipeline de geracao de ofertas executa `normalize_scores_percentile()`
(`ml/generate_offers.py:531-550`) que transforma os scores brutos do LightGBM
em percentis uniformes.

**Consequencia:** Mesmo que o LightGBM produza probabilidades razoavelmente
calibradas (binary_logloss como objetivo ajuda), a normalizacao por percentil
**destroi** qualquer calibracao. Um score_raw de 0.90 significa apenas "esta
oferta esta no percentil 90 de score", nao "90% de chance de conversao".

**Impacto para FDR:** O controle de False Discovery Rate (Secao 5) requer
p-values derivados de probabilidades calibradas. Usar percentis como proxy
e uma aproximacao grosseira.

**Recomendacao futura:** Aplicar Platt scaling ou isotonic regression
(`sklearn.calibration.CalibratedClassifierCV`) nos scores do LightGBM
**antes** da normalizacao percentil, e preservar esses scores calibrados
como coluna separada para uso em testes estatisticos.

---

## 4. Framework de Testes Estatisticos Proposto

### 4.1 Two-proportion z-test para conversao

Compara taxas de conversao entre dois grupos (e.g., Modelo A vs Modelo B,
ou Treatment vs Control).

**Funcao implementada:** `compare_conversion_rates()` em `ml/evaluate.py`

- Usa `statsmodels.stats.proportion.proportions_ztest`
- Retorna z-statistic, p-value e Wilson confidence intervals
- Wilson CI e preferivel ao Wald CI para proporcoes pequenas

### 4.2 Bootstrap CI para metricas de ranking

Metricas como Precision@K, NDCG@K, MAP@K nao tem distribuicao analitica
conhecida. Bootstrap CI reamostra clientes com reposicao e calcula a
distribuicao empirica da metrica.

**Funcao implementada:** `bootstrap_ranking_ci()` em `ml/evaluate.py`

- Reamostra clientes (nao ofertas individuais) para preservar correlacao intra-cliente
- n_boot=1000 iteracoes (ajustavel)
- Retorna media, CI lower/upper

### 4.3 Power analysis para dimensionamento

Antes de executar um A/B test, e necessario calcular quantos clientes
sao necessarios em cada grupo para detectar um efeito minimo (MDE).

**Funcao implementada:** `required_sample_size()` em `ml/evaluate.py`

- Usa `statsmodels.stats.power.NormalIndPower`
- Parametros: baseline_rate, MDE, alpha, power
- Exemplo: se baseline = 5%, MDE = 2pp, alpha = 0.05, power = 0.80
  => ~2.500 clientes por grupo

---

## 5. False Discovery Rate (FDR)

### 5.1 Ausencia de controle de FDR no pipeline atual

Cada oferta gerada pelo pipeline e uma hipotese implicita:
"O Cliente X comprara o Produto Y nos proximos 30 dias."

Com ~50.000 ofertas geradas por batch (tipico), o problema de multiplas
comparacoes e real: mesmo com um modelo razoavel, uma fracao significativa
das ofertas serao falsos positivos por acaso.

Atualmente, **nenhuma correcao** para multiplas comparacoes e aplicada.

### 5.2 Proposta: Benjamini-Hochberg (BH)

O procedimento BH controla o FDR (proporcao esperada de falsos positivos
entre as rejeicoes). E menos conservador que Bonferroni e mais adequado
para cenarios com muitas hipoteses.

**Funcao implementada:** `apply_fdr_filter()` em `ml/generate_offers.py`

- Usa `statsmodels.stats.multitest.multipletests(method="fdr_bh")`
- Parametro `--fdr-threshold` (default: 0.20)
- Integrada no pipeline principal apos step 5 (lifecycle discount)

### 5.3 Threshold recomendado para o negocio CMML

A escolha do threshold de FDR depende do custo de negocio:

| FDR | Significado | Custo FP | Custo FN |
|-----|-------------|----------|----------|
| 5%  | Muito conservador | Baixo (quase zero ofertas ruins) | Alto (muitas ofertas validas descartadas) |
| 10% | Conservador | Baixo | Moderado |
| **20%** | **Equilibrado** | **Aceitavel (1 em 5 ofertas e FP)** | **Baixo** |
| 25% | Agressivo | Moderado | Baixo |
| 30% | Muito agressivo | Alto | Muito baixo |

**Recomendacao: FDR = 20% (0.20)**

Justificativa para o contexto CMML (distribuicao B2B via WhatsApp):
- **Custo de falso positivo e baixo:** Uma mensagem WhatsApp desnecessaria
  gera leve desgaste, mas nao custa dinheiro nem queima o canal (ao contrario
  de email marketing, onde altas taxas de FP causam bloqueio)
- **Custo de falso negativo e moderado/alto:** Cada oferta nao enviada e uma
  venda potencial perdida (ticket medio B2B e significativo)
- **5% e excessivamente conservador:** Geraria pouquissimas ofertas, frustrando
  a equipe comercial e limitando o potencial de receita

### 5.4 Prerequisito: calibracao de scores

**Importante:** O controle de FDR via BH requer p-values derivados de
probabilidades calibradas. Atualmente, usamos `1 - score_raw` como proxy
de p-value, onde `score_raw` e o percentil normalizado. Isso e uma
aproximacao grosseira.

Para que o FDR funcione corretamente, e necessario:
1. Calibrar os scores do LightGBM (Platt scaling ou isotonic regression)
2. Preservar scores calibrados como coluna separada
3. Derivar p-values dos scores calibrados, nao dos percentis

Ate que a calibracao seja implementada, o filtro FDR esta ativo mas opera
com p-values aproximados. Os resultados devem ser interpretados com cautela.

### 5.5 Analise de sensibilidade (estimativa)

A tabela abaixo estima quantas ofertas sobreviveriam ao filtro BH em
diferentes thresholds, assumindo distribuicao tipica de scores:

| FDR Threshold | Ofertas mantidas (est.) | % do total |
|---------------|------------------------|------------|
| 5%            | ~5.000-10.000          | 10-20%     |
| 10%           | ~15.000-20.000         | 30-40%     |
| **20%**       | **~25.000-35.000**     | **50-70%** |
| 30%           | ~35.000-45.000         | 70-90%     |

Nota: Estes valores sao estimativas baseadas na distribuicao tipica de scores
percentil. A distribuicao real depende da calibracao dos modelos.

---

## 6. Recomendacoes Priorizadas

| Prioridade | Acao | Arquivo(s) | Esforco | Status |
|------------|------|------------|---------|--------|
| **P0** | Corrigir `find_optimal_threshold` metric="f1" -> "f1-score" | `ml/evaluate.py:393` | 1 linha | **FEITO** |
| **P0** | Persistir `auc_roc` em `save_metrics_to_db` | `ml/evaluate.py:489-508` | ~10 linhas | **FEITO** |
| **P0** | Adicionar coluna `hit_rate_at_k` e persistir | `sql/ddl/02_curated.sql`, `ml/evaluate.py` | ~10 linhas | **FEITO** |
| **P0** | Corrigir `incremental_lift()` denominador | `ml/evaluate.py:564-628` | ~30 linhas | **FEITO** |
| **P1** | Implementar `compare_conversion_rates()` | `ml/evaluate.py` | ~40 linhas | **FEITO** |
| **P1** | Implementar `bootstrap_ranking_ci()` | `ml/evaluate.py` | ~40 linhas | **FEITO** |
| **P1** | Implementar `required_sample_size()` | `ml/evaluate.py` | ~20 linhas | **FEITO** |
| **P1** | Implementar controle FDR (Benjamini-Hochberg) | `ml/generate_offers.py` | ~40 linhas | **FEITO** |
| **P1** | Adicionar `statsmodels` como dependencia | `app/api/requirements.txt` | 1 linha | **FEITO** |
| **P2** | Schema para A/B testing | `sql/ddl/07_experiments.sql` | ~15 linhas | **FEITO** |
| **P2** | Renomear score_raw/score para nomenclatura clara | `ml/generate_offers.py`, DDLs | ~50 linhas | Proposta |
| **P2** | FK de offer_outcomes para feedback_runs | `sql/ddl/05_feedback.sql` | ~5 linhas | Proposta |
| **P3** | Calibracao de scores (Platt/isotonic) | `ml/modelo_a_ranker.py` | ~100 linhas | Proposta |
| **P3** | Integrar experiment assignments no pipeline | `ml/generate_offers.py` | ~80 linhas | Proposta |
| **P3** | Dashboard de A/B testing (metricas causais) | Frontend + API | ~200 linhas | Proposta |
