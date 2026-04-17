# Conciliador Contábil (Streamlit)

Aplicação inicial em Streamlit para conciliação automática e semiautomática de registros contábeis, com base em REL003 e lançamentos do SINAF.

## Funcionalidades implementadas

- Login básico (usuário/senha local, com trilha de auditoria).
- Carga de dados por CSV para:
  - REL003 (snapshot dinâmico).
  - Lançamentos SINAF (base cumulativa).
- Geração de chave técnica (`record_key`) por hash a partir de colunas de negócio.
- Conciliação automática 1x1 (crédito vs débito por valor na mesma subconta).
- Conciliação manual de pendências.
- Sugestões de conciliação por combinação de valores (até 3 itens).
- Anotações persistentes por `record_key`.
- Exportação CSV de acertos conciliados para integração no SINAF/ERP.
- Trilha de auditoria de ações dos usuários.

## Estrutura de dados

A aplicação cria automaticamente um SQLite `conciliator.db` com as tabelas:

- `users`
- `rel003_entries`
- `sinaf_lancamentos`
- `reconciliations`
- `annotations`
- `audit_log`

## Como executar

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
streamlit run app.py
```

## Credenciais iniciais

- Usuário: `admin`
- Senha: `admin123`

> Em produção, substitua por SSO corporativo e armazenamento seguro de credenciais.

## Próximos passos recomendados

1. Substituir autenticação local por SSO (OIDC/SAML).
2. Implementar regras de conciliação parametrizadas por subconta.
3. Enriquecer integração com histórico SINAF para OLs específicas (1107, 1207, 3342).
4. Incluir governança de perfis (analista, supervisor, auditor).
5. Criar testes automatizados para normalização e engine de conciliação.
