# 10 — FAQ — Perguntas Frequentes

## Resumo

Respostas para as dúvidas mais comuns sobre o projeto.

---

## ETL e Conexões

**Q: O script Python não conecta no SQL Server. Erro: `[Microsoft][ODBC Driver 18...] SSL Provider`**

A: Confirme que `TrustServerCertificate=yes` está no DSN. O `etl/common.py` já inclui isso. Verifique que o driver correto está instalado:
```bash
odbcinst -q -d
# Deve aparecer: [ODBC Driver 18 for SQL Server]
```

---

**Q: O script Python não conecta no SQL Server. Erro: `Can't open lib 'ODBC Driver 18...'`**

A: O driver ODBC não está instalado. Siga as instruções em [`docs/05_ambiente_e_configuracao.md`](05_ambiente_e_configuracao.md).

---

**Q: Como sei se o ETL carregou dados novos?**

A: Consulte a tabela de controle:
```sql
SELECT * FROM etl.load_control ORDER BY updated_at DESC;
```
O campo `last_ts` mostra o timestamp da última carga bem-sucedida.

---

**Q: O ETL carregou dados duplicados. O que fazer?**

A: O `pg_copy_upsert_stg` usa `INSERT ... ON CONFLICT DO UPDATE`, então duplicatas são atualizadas, não inseridas duas vezes. Se houver duplicatas, verifique se a PK da tabela staging está corretamente definida.

---

**Q: Como forçar uma recarga completa de um dataset?**

A: Zere o watermark:
```sql
UPDATE etl.load_control
SET last_ts = NULL, last_id = NULL
WHERE dataset_name = 'stg.sales';
```
Em seguida, rode o script ETL novamente.

---

**Q: Quanto tempo leva uma carga completa do fato de compras?**

A: Depende do volume de dados no ERP. Com `FETCH_CHUNK=5000` e boa conexão de rede, espere processar ~50k–100k linhas/minuto. Para volumes grandes, considere aumentar `FETCH_CHUNK` ou rodar em horário de baixa utilização.

---

## Docker e Ambiente

**Q: O container `sqlserver_gp` não sobe. Como verificar?**

A: Verifique os logs:
```bash
docker logs sqlserver_gp 2>&1 | tail -30
```
Causas comuns:
- Senha `MSSQL_PASSWORD` muito simples (SQL Server exige senha complexa: maiúsculas, minúsculas, números, símbolos)
- Porta 1433 já em uso: `ss -tlnp | grep 1433`
- Falta de memória: SQL Server requer mínimo 2 GB de RAM

---

**Q: O pgAdmin não carrega. O que fazer?**

A: Verificar se o container está rodando:
```bash
docker ps | grep pgadmin
docker logs reco-pgadmin 2>&1 | tail -20
```
URL: `http://localhost:5050`

---

**Q: O arquivo `.env` não está sendo lido pelo Python.**

A: Verifique se o `.env` existe na raiz do projeto:
```bash
ls -la /home/gameserver/projects/cmml/.env
```
O `etl/common.py` usa `load_dotenv()` que carrega o `.env` do diretório de trabalho atual. Execute o script a partir da raiz do projeto.

---

## PostgreSQL e Schemas

**Q: Erro `schema "stg" does not exist`.**

A: Os schemas ainda não foram criados. Execute os DDLs:
```bash
psql -h "$PG_HOST" -U "$PG_USER" -d "$PG_DB" -f sql/ddl/00_schemas.sql
```
Ver [`docs/03_modelagem_dados.md`](03_modelagem_dados.md) para os DDLs propostos.

---

**Q: Erro `tabela X não tem PK. Defina PK para UPSERT seguro.`**

A: O `pg_copy_upsert_stg` requer que a tabela de destino tenha uma PRIMARY KEY. Adicione a PK no DDL da tabela e recrie-a.

---

**Q: O backup `.BAK` do SQL Server é de 6.2 GB. Devo versionar no Git?**

A: Não. Arquivos binários grandes não devem ir para o Git. Armazene o backup em storage seguro (S3, NAS) e documente o caminho. Adicione `*.BAK` ao `.gitignore`.

---

## ML e Recomendações

**Q: Ainda não há modelos ML implementados. Como gerar recomendações agora?**

A: Use o Modelo 0 (fallback) — top produtos por loja dos últimos 90 dias. É simples, eficaz para cold start e não requer ML. Ver [`docs/07_ml_recomendacao.md`](07_ml_recomendacao.md).

---

**Q: Como avaliar se as recomendações são boas?**

A: Use split temporal: treine nos dados antes de uma data corte, valide nos dados após. Calcule Precision@K e Recall@K. Ver seção de avaliação em [`docs/07_ml_recomendacao.md`](07_ml_recomendacao.md).

---

**Q: Um cliente nunca comprou nada (cold start). O que recomendar?**

A: Fallback para top 10 global ou top 10 da loja mais próxima. O Modelo 0 deve cobrir esse caso. À medida que o cliente compra, o modelo colaborativo começa a funcionar.

---

## Segurança e LGPD

**Q: O `.env` foi acidentalmente comitado. O que fazer?**

A: Imediatamente:
1. Troque todas as senhas expostas nos bancos de dados.
2. Remova o `.env` do histórico Git:
```bash
git rm --cached .env
git commit -m "chore: remove .env from git tracking"
```
3. Se o repositório for público, considere invalidar o histórico com `git filter-repo`.

---

**Q: Posso usar dados de clientes para treinar os modelos ML?**

A: Sim, mas com base legal adequada (LGPD). Use `customer_id` interno (não CPF/nome) nos datasets de treinamento. Consulte o jurídico sobre a base legal aplicável (legítimo interesse ou consentimento).

---

## Próximos Passos

Ver [`docs/inventario_repo.md`](inventario_repo.md) para lista priorizada de itens a implementar.
