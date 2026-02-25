# 01 — Visão Geral do Projeto

## Resumo

O CMML é uma plataforma de **recomendação personalizada de produtos** para varejo, construída sobre dados do ERP GP (SQL Server). O sistema extrai o histórico de compras, consolida entidades, e gera listas de sugestões individualizadas por cliente — prontas para consumo por uma aplicação de CRM ou envio via WhatsApp (roadmap).

---

## Contexto e Problema de Negócio

### Problema

A empresa possui um histórico rico de compras de clientes no ERP GP, mas esse dado não é explorado para **personalização de ofertas**. Clientes recebem comunicações genéricas (ou não recebem nenhuma), deixando receita incremental na mesa.

### Oportunidade

Com dados históricos de compras (tabela `MOVIMENTO_DIA` do ERP), é possível:

1. Identificar padrões individuais de compra ("esse cliente compra produto X todo mês")
2. Descobrir itens que clientes similares compram e que o cliente atual não conhece
3. Priorizar contato com clientes com maior propensão de conversão
4. Evitar spam recomendando itens já comprados recentemente

### Solução Proposta

Pipeline de dados + ML em 3 camadas:

```
[Candidate Generation] → [Ranking] → [Regras de Elegibilidade] → [Top-N por cliente]
```

---

## Escopo

### Dentro do Escopo (MVP)

- Extração do fato de compras do ERP GP (SQL Server → PostgreSQL)
- Consolidação das entidades: cliente, produto, pedido, item de pedido, loja
- Modelo 0 (baseline): mais vendidos por loja/categoria — funciona sem ML
- Candidate Generation: colaborativo (clientes similares compraram)
- Ranking: personalizado por histórico do cliente (Estratégia A)
- Tabela final de sugestões (`reco.sugestoes`) pronta para consumo
- Atualização automatizada (scheduler, cron ou Airflow — proposto)

### Fora do Escopo (Roadmap)

- Aplicação web de gestão de campanhas
- Integração WhatsApp Business API para envio de ofertas
- A/B testing automatizado
- Feedback loop em tempo real (eventos de clique, leitura, compra atribuída)
- Modelos de deep learning (embeddings neurais, transformers)
- Interface de usuário para o time de marketing configurar campanhas

---

## Glossário

| Termo | Definição no contexto do projeto |
|---|---|
| **Cliente** | Entidade compradora identificada por `ENTIDADEID_CLIENTE` no ERP. Pode ser pessoa física ou jurídica. |
| **Produto** | Item vendido, identificado por `PRODUTOID` no ERP. |
| **Pedido** | Documento de venda, identificado por `NUMDOCUMENTO`. Pode conter múltiplos itens. |
| **Item de pedido** | Uma linha do pedido (1 produto × quantidade × valor). |
| **Loja** | Ponto de venda, identificado por `ENTIDADEID_LOJA`. |
| **Fato de compras** | Tabela transacional `MOVIMENTO_DIA` com `TIPO=1` — representa vendas realizadas. |
| **ERP GP** | Sistema de gestão (ERP) da empresa, base de dados `GP_CASADASREDES` em SQL Server. |
| **Staging (stg)** | Schema PostgreSQL com dados brutos extraídos 1:1 do ERP. Sem transformações. |
| **Curado (cur)** | Schema PostgreSQL com entidades limpas, deduplicadas e enriquecidas. |
| **Recomendação (reco)** | Schema PostgreSQL com as sugestões finais por cliente. |
| **Watermark** | Ponto de controle do ETL incremental — timestamp ou ID da última carga bem-sucedida. |
| **Candidate Generation** | Etapa que gera 200–2000 candidatos de produtos por cliente (alta cobertura, baixa precisão). |
| **Ranker** | Modelo que ordena os candidatos e seleciona o top-N (alta precisão). |
| **Cold Start** | Problema de clientes ou produtos novos sem histórico de compras. |
| **Janela de recompra** | Período mínimo entre recomendações do mesmo produto para o mesmo cliente. |
| **Fallback / Modelo 0** | Estratégia de recomendação sem ML — mais vendidos — usada quando não há histórico. |
| **Top-N** | Lista final das N melhores sugestões por cliente (ex.: top-10). |
| **pgvector** | Extensão PostgreSQL para armazenar e buscar vetores de embeddings (similaridade). |
| **LGPD** | Lei Geral de Proteção de Dados — regula o uso de dados pessoais de clientes. |

---

## Arquitetura de Alto Nível

```
┌─────────────────────────────────┐
│         ERP GP (SQL Server)      │
│  banco: GP_CASADASREDES          │
│  tabela: dbo.MOVIMENTO_DIA       │
└────────────────┬────────────────┘
                 │ ODBC / pyodbc (chunks)
                 ▼
┌─────────────────────────────────┐
│     ETL (etl/common.py)         │
│  - extração incremental          │
│  - controle de watermark         │
│  - COPY + UPSERT no Postgres     │
└────────────────┬────────────────┘
                 │
                 ▼
┌─────────────────────────────────┐
│  PostgreSQL (reco-postgres)      │
│  ┌─────────┐  ┌───────────────┐ │
│  │  stg.*  │→ │    cur.*      │ │
│  │ (bruto) │  │  (curado)    │ │
│  └─────────┘  └──────┬────────┘ │
│                       │         │
│              ┌────────▼──────┐  │
│              │    reco.*     │  │
│              │  (sugestões)  │  │
│              └───────────────┘  │
└─────────────────────────────────┘
                 │
                 ▼
        [Aplicação / WhatsApp]
           (roadmap)
```

---

## Próximos Passos

1. Revisar o glossário com o time de negócio para confirmar mapeamento de termos.
2. Definir SLA de atualização das recomendações (ex.: diário às 02h).
3. Identificar restrições de LGPD aplicáveis — ver [`docs/09_seguranca_e_compliance.md`](09_seguranca_e_compliance.md).
4. Priorizar entidades para o MVP: foco em cliente + produto + fato de compras.
5. Consultar [`docs/02_arquitetura.md`](02_arquitetura.md) para detalhes técnicos.
