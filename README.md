# Case Bees Breweries Case

## Visão Geral
Este projeto implementa um pipeline de dados para processamento de informações sobre cervejarias utilizando Apache Airflow, Spark e Delta Lake sobre a API da "openbrewerydb.org". O pipeline segue uma arquitetura em camadas (bronze, silver, gold) para garantir a qualidade e confiabilidade dos dados.

## Estrutura do Projeto
```
bees_breweries_case/
├── dags/                           # DAGs do Airflow
│   ├── dag_bronze_breweries_ingestion.py
│   ├── dag_silver_breweries_transformation.py
│   └── dag_gold_breweries_aggregation.py
├── include/                        # Scripts e recursos
│   ├── scripts/                    # Scripts Spark
│   │   ├── script_silver_breweries_transformation.py
│   │   └── script_gold_breweries_aggregation.py
│   ├── data/                       # Dados processados
│   │   ├── bronze/                 # Camada bronze
│   │   ├── prata/                  # Camada silver
│   │   └── ouro/                   # Camada gold
│   └── logs/                       # Logs de execução
└── README.md                       # Documentação em inglês
```

## Pré-requisitos
- Docker e Docker Compose
- Astro CLI
- Python 3.8+
- Apache Spark
- Delta Lake

## Configuração do Ambiente

### 1. Inicialização do Airflow com Astro CLI
```bash
# Instalar Astro CLI
curl -sSL install.astronomer.io | sudo bash -s

# Iniciar o projeto
astro dev start
```

### 2. Configuração do Spark
O projeto utiliza o Delta Lake para armazenamento de dados. Certifique-se de que o Spark está configurado com as seguintes dependências:
- io.delta:delta-spark_2.12:3.3.0

## Pipeline de Dados

### Camada Bronze
- **Objetivo**: Ingestão inicial dos dados da API Open Brewery DB
- **Processo**:
  - Coleta dados da API
  - Armazena em formato JSON
  - Mantém os dados originais sem transformações

### Camada Silver
- **Objetivo**: Transformação e limpeza dos dados
- **Processos**:
  - Padronização de números de telefone
  - Limpeza de URLs
  - Criação de endereços completos
  - Validação de coordenadas
  - Sanitização de caracteres especiais
  - Implementação de SCD2 para controle de versões

### Camada Gold
- **Objetivo**: Agregação e modelagem dimensional
- **Estrutura**:
  - Tabelas de dimensão (location, brewery_type)
  - Tabela de fatos (breweries)
  - Visões agregadas

## Execução do Pipeline

1. **Iniciar o Airflow**:
```bash
astro dev start
```

2. **Acessar a Interface do Airflow**:
- Abra o navegador e acesse: http://localhost:8080
- Faça login com as credenciais padrão (admin/admin)

3. **Ativar as DAGs**:
- Localize as DAGs no painel
- Ative `dag_bronze_breweries_ingestion`
- As DAGs subsequentes serão acionadas automaticamente

## Tratamento de Dados

### Padronização de Números de Telefone
- Remove caracteres não numéricos (exceto '+')
- Remove '+' e '1' iniciais
- Exemplo: "+ 351 289 815 218" → "351289815218"

### Limpeza de URLs
- Remove barras invertidas escapadas
- Exemplo: "http:\/\/www.alewife.beer" → "http://www.alewife.beer"

### Endereços Completos
- Combina campos de endereço em uma única string
- Exemplo: "514 51st Ave, Long Island City, New York 11101-5879, United States"

### Validação de Coordenadas
- Longitude: -180 a 180
- Latitude: -90 a 90
- Coordenadas inválidas são definidas como nulas

### Sanitização de Caracteres
- Normalização NFKD para caracteres especiais
- Substituição por equivalentes ASCII
- Exemplo: "Kärnten" → "Karnten"

## Monitoramento
- Logs são armazenados em `/usr/local/airflow/include/logs`
- Cada etapa do pipeline gera logs detalhados
- Métricas são armazenadas no XCom do Airflow

## Solução de Problemas

### Problemas Comuns
1. **Erro de Conexão com a API**
   - Verifique a conectividade com a internet
   - Verifique se a API está disponível

2. **Erros de Permissão**
   - Verifique as permissões dos diretórios de dados
   - Execute `chmod 777` nos diretórios necessários

3. **Erros de Spark**
   - Verifique a configuração do Spark
   - Verifique as dependências do Delta Lake

### Logs
- Consulte os logs em `/usr/local/airflow/include/logs`
- Cada execução gera um arquivo de log com timestamp

## Manutenção
- Limpeza periódica dos logs
- Monitoramento do espaço em disco
- Atualização das dependências conforme necessário

## Funcionalidades não implementadas
- Testes
- Captura de erros
- CI/CD