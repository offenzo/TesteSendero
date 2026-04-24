# TesteSendero

Este projeto contém os seguintes arquivos principais:

- `pipeline.py`: Script principal do pipeline.
- `processos.txt`: Lista de processos ou dados utilizados pelo pipeline.

## Estrutura

- **pipeline.py**: Código principal do pipeline.
- **processos.txt**: Arquivo de texto com informações auxiliares.

## Instalação das dependências

Para executar este projeto, é necessário instalar as dependências listadas abaixo. Recomenda-se utilizar um ambiente virtual (venv) para isolar as dependências do projeto.

### 1. Crie um ambiente virtual (opcional, mas recomendado)

```bash
python3 -m venv venv
source venv/bin/activate
```

### 2. Instale as dependências

O projeto utiliza as seguintes bibliotecas Python:
- pandas
- requests
- fastapi (opcional, apenas para uso da API)
- openpyxl (para salvar arquivos Excel)

Você pode instalar todas as dependências com o comando:

```bash
pip install pandas requests fastapi openpyxl
```

Se for utilizar a API, também será necessário instalar o Uvicorn para rodar o servidor:

```bash
pip install uvicorn
```

## Como executar

1. Instale as dependências seguindo as instruções acima.
2. Execute o script principal:

```bash
python pipeline.py
```

Ou, para executar a API:

```bash
uvicorn pipeline:app --reload
```