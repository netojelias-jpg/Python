# 🎯 Dashboard de Clusters - Análise de Crédito Sicoob

Sistema completo de análise de clusters de clientes com visualização interativa.

## 🚀 Como Usar

### 1. Iniciar o Dashboard de Clusters

```powershell
python .\web\clusters_dashboard.py
```

O dashboard estará disponível em: **http://localhost:5001**

### 2. Iniciar o Dashboard Principal (Carteira)

```powershell
python .\web\app.py
```

O dashboard principal estará disponível em: **http://localhost:5000**

## 📊 Funcionalidades do Dashboard de Clusters

### Página Principal (`/`)
- Visualização de todos os perfis analisados
- Métricas gerais: total de perfis, clusters e silhouette médio
- Cards interativos com qualidade da clusterização (Excelente/Bom/Razoável/Fraco)
- Informações de cada perfil: número de clusters, score de silhouette, data de execução

### Detalhes do Perfil (`/perfil/<run_id>`)
- Visualização detalhada de todos os clusters de um perfil
- **Explicação automática** de cada cluster baseada em:
  - Nível de risco (Baixo/Moderado/Elevado/Muito Alto)
  - Cobertura de garantias
  - Situação de atraso
  - Ticket médio dos contratos
- Métricas por cluster:
  - Total de clientes
  - Risco médio
  - Cobertura média
  - Atraso médio
  - Ticket médio e saldo médio
- Fatores médios do cluster (componentes da análise fatorial)

### Lista de Clientes (`/cluster/<run_id>/<cluster_id>`)
- Lista completa de todos os clientes do cluster
- Filtro de busca em tempo real (por ID, agência, carteira, linha)
- Informações detalhadas:
  - ID do cliente
  - Agência e carteira
  - Linha de crédito
  - Rating de risco com badges coloridos
  - Fatores individuais de cada cliente
- Contador dinâmico de clientes filtrados

## 🎨 Design e UX

- **Gradiente moderno** roxo/azul no fundo
- **Cards interativos** com hover effects
- **Badges coloridos** para indicadores de qualidade
- **Busca em tempo real** sem recarregar página
- **Layout responsivo** que se adapta a diferentes telas
- **Navegação breadcrumb** para fácil retorno

## 📈 Interpretação dos Clusters

### Score Silhouette
- **≥ 0.75**: Excelente (clusters muito bem definidos)
- **0.60-0.74**: Bom (clusters bem separados)
- **0.45-0.59**: Razoável (clusters com alguma sobreposição)
- **< 0.45**: Fraco (clusters pouco definidos)

### Explicações Automáticas
O sistema gera explicações baseadas em:

1. **Risco Inicial Médio**:
   - 0-2: Risco Baixo (AA, A, B)
   - 3-4: Risco Moderado (BB, C)
   - 5-7: Risco Elevado (CC, D, DD)
   - 8+: Risco Muito Alto (E, EE, F, G)

2. **Cobertura de Garantias**:
   - ≥150%: Excelente cobertura
   - 100-149%: Cobertura adequada
   - <100%: Cobertura insuficiente

3. **Atraso**:
   - 0 dias: Adimplente
   - 1-30 dias: Atrasos leves
   - 31-90 dias: Atrasos moderados
   - >90 dias: Atrasos graves

4. **Ticket Médio**:
   - ≥R$ 100.000: Alto valor
   - R$ 30.000-99.999: Médio valor
   - <R$ 30.000: Baixo valor

## 🗂️ Estrutura de Dados

### Tabelas Utilizadas
- `cluster_run`: Metadados de cada execução (UUID, perfil, parâmetros, métricas)
- `cluster_run_clientes`: Clientes com seus fatores individuais (JSONB)
- `cluster_run_resumo`: Agregações por cluster (médias, totais)

### APIs Disponíveis
- `GET /api/runs`: Lista todos os runs
- `GET /api/cluster_summary/<run_id>`: Resumo dos clusters de um run

## 🔧 Tecnologias

- **Backend**: Flask 3.1+
- **Database**: PostgreSQL com JSONB
- **Frontend**: HTML5 + CSS3 + Vanilla JavaScript
- **Visualização**: Gradientes CSS, badges responsivos
- **Data Processing**: pandas, SQLAlchemy

## 📝 Próximos Passos

- [ ] Exportar lista de clientes para Excel
- [ ] Comparar clusters entre diferentes execuções
- [ ] Gráficos interativos com Chart.js ou Plotly
- [ ] Dashboard de evolução temporal dos clusters
- [ ] Alertas automáticos para mudança de cluster de clientes críticos

---

**Desenvolvido para Sicoob - Sistema de Análise de Crédito** 🏦
