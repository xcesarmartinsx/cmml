# 11 — WhatsApp Business API — Estudo de Viabilidade

## Resumo Executivo

**Recomendacao: GO** — A integracao com a WhatsApp Cloud API e viavel e recomendada para o caso de uso de reativacao de clientes inativos. A Cloud API da Meta e gratuita para hospedar (Meta gerencia a infraestrutura), o custo por mensagem de marketing no Brasil e aproximadamente **US$ 0,0625** (~R$ 0,37 na cotacao atual), e o setup inicial pode ser feito em 1-2 dias uteis.

**Abordagem recomendada**: Usar a **Cloud API** (hospedada pela Meta) com mensagens do tipo **Marketing Template**, integradas ao backend FastAPI existente. O fluxo seria: usuario clica "Enviar WhatsApp" na lista de ofertas do Business 360 (porta 3001), o frontend chama o endpoint da API (porta 8001), que por sua vez envia a mensagem via Cloud API.

**Custo estimado para 1.000 mensagens/mes**: ~US$ 62,50 (R$ 375,00).

---

## 1. Detalhes Tecnicos

### 1.1 Modelo de Precos (atualizado julho/2025)

Desde 1 de julho de 2025, a Meta adotou um modelo de cobranca **por mensagem** (anteriormente era por conversa). As categorias sao:

| Categoria        | Descricao                                     | Custo por msg (Brasil, USD) |
|------------------|-----------------------------------------------|-----------------------------|
| **Marketing**    | Promocoes, reativacao, ofertas                | ~US$ 0,0625                 |
| **Utility**      | Confirmacoes de pedido, atualizacoes          | ~US$ 0,0068 (1-1.000 msgs)  |
| **Authentication** | Codigos OTP, verificacao                    | ~US$ 0,0315                 |
| **Service**      | Respostas a mensagens iniciadas pelo cliente  | Gratuito (janela 24h)       |

**Descontos por volume** (Utility e Authentication):
- 1-1.000 msgs: preco cheio
- 1.001-10.000: ~5% desconto
- 10.001+: descontos progressivos

**Tier gratuito**: Quando o cliente inicia a conversa (ex: responde a um anuncio Click-to-WhatsApp), a janela de atendimento de 24h e gratuita (extendida para 72h se via anuncio). Nao ha mais 1.000 conversas de servico gratuitas como no modelo anterior.

**Faturamento local (BRL)**: Previsto para o segundo semestre de 2026. Ate la, cobranca em USD.

### 1.2 Janela de Conversacao (Regra das 24h)

| Situacao                              | O que pode ser enviado                                   |
|---------------------------------------|----------------------------------------------------------|
| **Dentro da janela de 24h**           | Qualquer tipo de mensagem (texto livre, midia, templates)|
| **Fora da janela de 24h**             | **Somente Message Templates** pre-aprovados pela Meta    |
| **Cliente iniciou via anuncio**       | Janela estendida para 72h                                |

Para o caso de uso do CMML (reativacao de clientes inativos), as mensagens serao **sempre fora da janela de 24h**, pois o cliente nao iniciou contato. Portanto, **somente Message Templates aprovados** poderao ser usados.

### 1.3 Message Templates

**Categorias disponiveis**:
- **Marketing**: promocoes, ofertas, reativacao (nosso caso de uso principal)
- **Utility**: atualizacoes transacionais, confirmacoes
- **Authentication**: codigos OTP, verificacao de conta

**Processo de aprovacao**:
1. Criar o template no Meta Business Manager (nome, idioma, corpo, variaveis)
2. Submeter para revisao da Meta
3. Aprovacao automatica por ML em **30 minutos a 24 horas** (tipicamente minutos)
4. Em caso de rejeicao, ajustar e resubmeter

**Status possiveis**: Approved, Rejected, Paused (feedback negativo), Disabled (violacoes repetidas).

**Dica**: Templates que misturam conteudo promocional e transacional sao **sempre classificados como Marketing**.

### 1.4 Requisitos Tecnicos (Pre-requisitos)

| Requisito                         | Descricao                                                        |
|-----------------------------------|------------------------------------------------------------------|
| **Meta Business Manager**         | Conta empresarial verificada no business.facebook.com            |
| **WABA**                          | WhatsApp Business Account vinculada ao Business Manager          |
| **Numero de telefone**            | Numero dedicado, verificado via SMS/chamada de voz               |
| **Verificacao de negocios**       | Documentacao da empresa para liberacao de limites                |
| **App no Meta for Developers**    | App criado em developers.facebook.com com produto WhatsApp       |
| **Access Token permanente**       | Gerado via System User no Business Manager                       |
| **Webhook URL (HTTPS)**           | Para receber callbacks de status de entrega (opcional no MVP)    |

**Importante**: O numero de telefone **nao pode estar vinculado** a uma conta pessoal do WhatsApp ou WhatsApp Business App.

### 1.5 Limites de Taxa (Rate Limits)

| Metrica                        | Limite                                              |
|--------------------------------|-----------------------------------------------------|
| **Throughput (MPS)**           | 80 msgs/segundo (default), ate 1.000 MPS (auto-upgrade) |
| **Msgs unicas/24h (Tier 1)**  | 250 destinatarios unicos (sem verificacao de negocios)   |
| **Msgs unicas/24h (Tier 2)**  | 1.000 destinatarios unicos                               |
| **Msgs unicas/24h (Tier 3)**  | 10.000 destinatarios unicos                              |
| **Msgs unicas/24h (Tier 4)**  | 100.000 destinatarios unicos                             |
| **Unlimited**                  | Sem limite (qualidade alta sustentada)                   |

**Upgrade de tier**: Avaliado a cada 6 horas pela Meta (desde outubro 2025). Criterios: verificacao de negocios, qualidade da conta, volume sustentado.

**Nota**: Desde outubro 2025, os limites sao por **Business Portfolio**, nao por numero de telefone.

### 1.6 Cloud API vs On-Premise

| Criterio            | Cloud API (Recomendado)            | On-Premise API                    |
|---------------------|------------------------------------|-----------------------------------|
| **Hospedagem**      | Meta gerencia                      | Servidor proprio necessario       |
| **Setup**           | Minutos a horas                    | Dias a semanas                    |
| **Custo infra**     | Zero                               | Servidor dedicado                 |
| **Manutencao**      | Meta atualiza automaticamente      | Atualizacoes manuais             |
| **Escalabilidade**  | Automatica                         | Manual                            |
| **Novos registros** | Suportado                          | **Nao aceita novos clientes**    |
| **MVP**             | Ideal                              | Nao recomendado                  |

**Recomendacao para o CMML**: **Cloud API** sem duvida. Setup rapido, zero infraestrutura adicional, ideal para MVP.

---

## 2. Exemplos de Templates de Mensagem

Os templates abaixo seguem as diretrizes da Meta para a categoria **Marketing** e podem ser submetidos para aprovacao.

### Template 1: Cliente ha mais de 2 anos sem compra (reativacao forte)

```
Nome: reativacao_cliente_2anos
Categoria: Marketing
Idioma: pt_BR

Corpo:
Ola {{1}}! Faz tempo que voce nao nos visita. Sentimos sua falta!
Que tal conferir nossas novidades? Temos produtos especiais esperando por voce.

Responda SIM para receber nossas melhores ofertas ou visite nossa loja.

Variaveis:
  {{1}} = primeiro nome do cliente

Botoes (opcional):
  [Ver Ofertas] -> URL
  [Nao tenho interesse] -> Quick Reply
```

### Template 2: Cliente ha mais de 1 ano sem compra (com referencia ao produto)

```
Nome: reativacao_produto_1ano
Categoria: Marketing
Idioma: pt_BR

Corpo:
Ola {{1}}! Lembra quando voce comprou {{2}}?
Ja faz mais de um ano e temos novidades na mesma linha que voce vai adorar.

Aproveite condicoes especiais para clientes como voce!

Variaveis:
  {{1}} = primeiro nome do cliente
  {{2}} = nome do ultimo produto comprado

Botoes (opcional):
  [Ver Produtos Similares] -> URL
  [Nao, obrigado] -> Quick Reply
```

### Template 3: Oferta personalizada com probabilidade de interesse

```
Nome: oferta_personalizada
Categoria: Marketing
Idioma: pt_BR

Corpo:
Ola {{1}}! Com base no seu historico de compras, selecionamos
ofertas especiais para voce em {{2}}.

Nossos especialistas acreditam que voce vai gostar!
Confira as ofertas feitas sob medida para o seu perfil.

Variaveis:
  {{1}} = primeiro nome do cliente
  {{2}} = categoria ou nome do produto recomendado

Botoes (opcional):
  [Ver Minhas Ofertas] -> URL dinamica com token
  [Parar de receber] -> Quick Reply (opt-out obrigatorio)
```

**Observacao**: Templates de Marketing devem sempre incluir opcao de opt-out. A Meta exige e o nao-cumprimento pode pausar ou desabilitar o template.

---

## 3. Esboco de Arquitetura

### 3.1 Fluxo de Envio

```
Business 360 (porta 3001)
  |
  | POST /api/whatsapp/send
  | { customer_id, template_name, parameters }
  |
  v
FastAPI (porta 8001)
  |
  |-- Valida autenticacao JWT (obrigatorio - PII)
  |-- Busca dados do cliente em cur.customers (telefone, nome)
  |-- Busca dados da oferta em reco.sugestoes
  |-- Monta payload do template com variaveis
  |-- Registra envio em reco.whatsapp_messages (auditoria)
  |
  | POST https://graph.facebook.com/v21.0/{PHONE_NUMBER_ID}/messages
  | Authorization: Bearer {WHATSAPP_ACCESS_TOKEN}
  | Content-Type: application/json
  |
  v
WhatsApp Cloud API (Meta)
  |
  | Entrega a mensagem ao cliente
  |
  v
Cliente recebe no WhatsApp
```

### 3.2 Endpoint da FastAPI

```python
# app/api/routers/whatsapp.py

@router.post("/api/whatsapp/send")
async def send_whatsapp_message(
    payload: WhatsAppSendRequest,
    current_user = Depends(get_current_user),  # JWT obrigatorio
    db = Depends(get_db)
):
    # 1. Buscar dados do cliente
    customer = get_customer(db, payload.customer_id)

    # 2. Buscar oferta/sugestao
    offer = get_offer(db, payload.offer_id)

    # 3. Montar payload para Cloud API
    wa_payload = {
        "messaging_product": "whatsapp",
        "to": customer.phone_number,  # formato internacional: 5511999999999
        "type": "template",
        "template": {
            "name": payload.template_name,
            "language": {"code": "pt_BR"},
            "components": [
                {
                    "type": "body",
                    "parameters": [
                        {"type": "text", "text": customer.first_name},
                        {"type": "text", "text": offer.product_name}
                    ]
                }
            ]
        }
    }

    # 4. Enviar via Cloud API
    response = httpx.post(
        f"https://graph.facebook.com/v21.0/{PHONE_NUMBER_ID}/messages",
        headers={
            "Authorization": f"Bearer {WHATSAPP_ACCESS_TOKEN}",
            "Content-Type": "application/json"
        },
        json=wa_payload
    )

    # 5. Registrar envio para auditoria
    log_whatsapp_message(db, customer.id, payload.template_name, response.status_code)

    return {"status": "sent", "wa_message_id": response.json().get("messages", [{}])[0].get("id")}
```

### 3.3 Variaveis de Ambiente Necessarias

```env
# .env (NUNCA commitar)
WHATSAPP_ACCESS_TOKEN=EAAxxxxxxx...
WHATSAPP_PHONE_NUMBER_ID=123456789012345
WHATSAPP_BUSINESS_ACCOUNT_ID=987654321098765
WHATSAPP_API_VERSION=v21.0
```

### 3.4 Tabela de Auditoria (PostgreSQL)

```sql
CREATE TABLE IF NOT EXISTS reco.whatsapp_messages (
    id              SERIAL PRIMARY KEY,
    customer_id     INTEGER NOT NULL,
    template_name   VARCHAR(100) NOT NULL,
    phone_number    VARCHAR(20) NOT NULL,  -- mascarado nos logs
    status_code     INTEGER,
    wa_message_id   VARCHAR(100),
    sent_by         VARCHAR(100) NOT NULL, -- usuario que clicou
    sent_at         TIMESTAMPTZ DEFAULT NOW(),
    delivery_status VARCHAR(20) DEFAULT 'sent'  -- sent, delivered, read, failed
);

CREATE INDEX idx_wa_msgs_customer ON reco.whatsapp_messages(customer_id);
CREATE INDEX idx_wa_msgs_sent_at ON reco.whatsapp_messages(sent_at);
```

---

## 4. Proposta de UI — Business 360 (porta 3001)

### 4.1 Botao na Lista de Ofertas

Na tabela de ofertas/sugestoes existente, adicionar uma coluna "Acoes" com um botao de icone do WhatsApp:

```
| Cliente              | Produto        | % Chance | Ultima Compra | Acoes         |
|----------------------|----------------|----------|---------------|---------------|
| Maria Adelaide M.    | Rede Especial  | 100%     | 15/03/2023    | [WA] [...]    |
| Joao Silva           | Corda Sisal    | 92%      | 01/08/2024    | [WA] [...]    |
```

O botao `[WA]` (icone verde do WhatsApp) abre um **modal de confirmacao**.

### 4.2 Modal de Envio de WhatsApp

```
+----------------------------------------------------------+
|  Enviar Oferta via WhatsApp                          [X]  |
+----------------------------------------------------------+
|                                                           |
|  Cliente: Maria Adelaide Mesquita                         |
|  Telefone: (85) 9****-**34                               |
|  Produto: Rede Especial Miss Ceara                       |
|  Ultima compra: 15/03/2023 (2 anos e 11 meses atras)     |
|                                                           |
|  Template: [Reativacao 2+ anos         v]                 |
|                                                           |
|  Pre-visualizacao:                                        |
|  +------------------------------------------------------+|
|  | Ola Maria! Faz tempo que voce nao nos visita.        ||
|  | Sentimos sua falta! Que tal conferir nossas           ||
|  | novidades? Temos produtos especiais esperando         ||
|  | por voce.                                             ||
|  +------------------------------------------------------+|
|                                                           |
|  [Cancelar]                        [Enviar via WhatsApp]  |
+----------------------------------------------------------+
```

**Funcionalidades do modal**:
- Exibe dados do cliente (telefone parcialmente mascarado por LGPD)
- Dropdown para selecionar o template adequado
- Pre-visualizacao da mensagem com variaveis preenchidas
- Botao de envio com confirmacao
- Feedback visual de sucesso/erro apos envio

### 4.3 Historico de Mensagens (Aba no Detalhe do Cliente)

No detalhe do cliente (Business 360), adicionar uma aba "WhatsApp" mostrando:

```
+----------------------------------------------------------+
| Historico de Mensagens WhatsApp                          |
+----------------------------------------------------------+
| Data/Hora          | Template              | Status       |
|--------------------|----------------------|--------------|
| 28/02/2026 14:30   | Reativacao 2+ anos   | Entregue     |
| 15/01/2026 10:15   | Oferta Personalizada | Lida         |
+----------------------------------------------------------+
```

### 4.4 Pagina de Gestao de Templates (futura)

Para uma fase posterior, uma pagina administrativa para gerenciar templates:

```
+----------------------------------------------------------+
| Gestao de Templates WhatsApp                    [+ Novo]  |
+----------------------------------------------------------+
| Nome                    | Categoria  | Status   | Acoes  |
|-------------------------|------------|----------|--------|
| reativacao_2anos        | Marketing  | Aprovado | [Edit] |
| reativacao_produto_1ano | Marketing  | Aprovado | [Edit] |
| oferta_personalizada    | Marketing  | Pendente | [Edit] |
+----------------------------------------------------------+
```

**Nota**: No MVP, os templates sao gerenciados diretamente no Meta Business Manager. A pagina de gestao interna e uma evolucao futura.

---

## 5. Pre-requisitos para Implementacao

### Fase 0 — Setup Administrativo (1-2 dias)
- [ ] Criar conta no Meta Business Manager (business.facebook.com)
- [ ] Solicitar verificacao de negocios (pode levar 2-5 dias uteis)
- [ ] Criar WhatsApp Business Account (WABA) no Business Manager
- [ ] Obter numero de telefone dedicado (nao pode ser pessoal)
- [ ] Verificar o numero via SMS/chamada
- [ ] Criar app em developers.facebook.com com produto WhatsApp
- [ ] Gerar System User Token permanente

### Fase 1 — Templates (1 dia)
- [ ] Submeter os 3 templates de Marketing para aprovacao
- [ ] Aguardar aprovacao (tipicamente < 24h)
- [ ] Testar envio com numero de teste

### Fase 2 — Backend (2-3 dias)
- [ ] Criar router `app/api/routers/whatsapp.py`
- [ ] Criar tabela `reco.whatsapp_messages`
- [ ] Implementar endpoint de envio com autenticacao JWT
- [ ] Adicionar variaveis de ambiente (token, phone_number_id)
- [ ] Testes unitarios para o endpoint

### Fase 3 — Frontend (2-3 dias)
- [ ] Adicionar botao WhatsApp na lista de ofertas
- [ ] Criar modal de envio com pre-visualizacao
- [ ] Implementar feedback de sucesso/erro
- [ ] Adicionar aba de historico no detalhe do cliente

### Fase 4 — Observabilidade e Compliance (1 dia)
- [ ] Webhook para status de entrega (delivered, read, failed)
- [ ] Dashboard de metricas (msgs enviadas, taxa de entrega, taxa de leitura)
- [ ] Garantir mascaramento de telefone nos logs
- [ ] Documentar base legal LGPD para envio de mensagens de marketing

---

## 6. Estimativa de Custos

| Cenario                  | Msgs/mes | Custo Marketing (USD) | Custo Marketing (BRL ~6x) |
|--------------------------|----------|-----------------------|---------------------------|
| Piloto                   | 100      | US$ 6,25              | ~R$ 37,50                 |
| Operacao pequena         | 500      | US$ 31,25             | ~R$ 187,50                |
| Operacao media           | 1.000    | US$ 62,50             | ~R$ 375,00                |
| Operacao grande          | 5.000    | US$ 312,50            | ~R$ 1.875,00              |
| Escala                   | 10.000   | US$ 625,00            | ~R$ 3.750,00              |

**Nota**: Custos de infraestrutura da Cloud API sao zero (Meta hospeda). O unico custo e o preco por mensagem.

---

## 7. Riscos e Mitigacoes

| Risco                                    | Impacto | Mitigacao                                              |
|------------------------------------------|---------|--------------------------------------------------------|
| Template rejeitado pela Meta             | Medio   | Seguir guidelines rigorosamente; ter templates backup  |
| Numero bloqueado por spam reports        | Alto    | Respeitar opt-out; enviar apenas para clientes com relacionamento previo |
| Limite de 250 msgs/dia (sem verificacao) | Medio   | Completar verificacao de negocios antes do lancamento  |
| Token expirado                           | Medio   | Usar System User Token (permanente) em vez de User Token |
| LGPD — envio sem consentimento           | Alto    | Documentar base legal (interesse legitimo ou consentimento) |
| Custo inesperado com escala              | Baixo   | Implementar limites diarios no sistema; alertas de custo |

---

## 8. Referencias

- [WhatsApp Cloud API - Postman Collection](https://www.postman.com/meta/whatsapp-business-platform/collection/wlk6lh4/whatsapp-cloud-api)
- [WhatsApp Business API Pricing 2026 - FlowCall](https://flowcall.co/blog/whatsapp-business-api-pricing-2026)
- [WhatsApp API Pricing - Respond.io](https://respond.io/blog/whatsapp-business-api-pricing)
- [WhatsApp Template Categories - Sanuker](https://sanuker.com/guideline-to-whatsapp-template-message-categories/)
- [WhatsApp Rate Limits - Wati](https://www.wati.io/en/blog/whatsapp-business-api/whatsapp-api-rate-limits/)
- [WhatsApp Business Platform Pricing (Oficial)](https://business.whatsapp.com/products/platform-pricing)
- [Setup WhatsApp Cloud API - Social Intents](https://www.socialintents.com/blog/how-to-set-up-whatsapp-business-api/)
- [WhatsApp API Send Message Python - Chatarmin](https://chatarmin.com/en/blog/whats-app-api-send-messages)
