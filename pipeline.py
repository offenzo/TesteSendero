import pandas as pd
import requests
import json
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
import time
import re
import unicodedata

try:
    from fastapi import FastAPI, HTTPException, Query
except ImportError:
    FastAPI = None 
    HTTPException = None 
    Query = None  

class ExtracaoPJeAPI:
    baseUrl = "https://comunicaapi.pje.jus.br/api"
    tempo = 5
    temas = {
        'homologacao': ['homolog', 'confirm', 'aprova'],
        'rateio_pagamento': ['rateio', 'pagamento', 'paga', 'distribui'],
        'credor_silente': ['silente', 'silencio'],
        'conta_judicial': ['conta judicial', 'saldo', 'cjud'],
        'prazo': ['prazo', 'dias', 'vence'],
        'decisao': ['decisao', 'julgou', 'sentenca'],
        'edital': ['edital', 'publica', 'aviso'],
        'cessao_credito': ['cessao', 'transferencia'],
        'despacho': ['despacho', 'responde'],
        'peticao': ['peticao', 'requer', 'solicita']
    }
    
    pesos = {
        'homologacao': 30,
        'rateio_pagamento': 30,
        'credor_silente': 25,
        'conta_judicial': 20,
        'prazo': 18,
        'decisao': 15,
        'edital': 12,
        'cessao_credito': 12,
        'despacho': 8,
        'peticao': 5
    }
    
    bonus = {
        ('homologacao', 'rateio_pagamento'): 40,
        ('decisao', 'prazo'): 15,
        ('conta_judicial', 'prazo'): 10,
        ('credor_silente', 'prazo'): 15,
    }
    
    def __init__(self, dataInicio: str = "01/01/2024", dataFim: str = "01/01/2025"):
        self.dataInicio = dataInicio
        self.dataFim = dataFim
        self.comunicacoesBrutas = []
        self.comunicacoesLimpas = []
        self.comunicacoesClassificadas = []
        self.erros = []
        self.inconsistencias = []
    
    def lerProcessos(self, arquivo: str = "processos.txt") -> List[str]:
        try:
            with open(arquivo, 'r', encoding='utf-8') as f:
                processos = [linha.strip() for linha in f if linha.strip()]
            print(f"{len(processos)} processos lidos")
            return processos
        except FileNotFoundError:
            print(f"Arquivo {arquivo} não encontrado")
            return []
    
    def consultarComunicacoes(self, numeroProcesso: str) -> List[Dict]:
        tentativas = 2
        for tentativa in range(tentativas):
            try:
                url = f"{self.baseUrl}/comunicacoes/{numeroProcesso}"
                response = requests.get(
                    url,
                    params={"dataInicio": self.dataInicio, "dataFim": self.dataFim},
                    timeout=self.tempo
                )
                if response.status_code == 200:
                    dados = response.json()
                    comunicacoes = dados if isinstance(dados, list) else dados.get('comunicacoes', [])
                    if comunicacoes:
                        print(f"{numeroProcesso}: {len(comunicacoes)} comunicações")
                    return comunicacoes
                if response.status_code == 404:
                    return []
            except Exception as e:
                if tentativa < tentativas - 1:
                    time.sleep(0.5)
        return []
    
    def executarExtracao(self, arquivoEntrada: str = "processos.txt"):
        print("\nFASE 1: EXTRAÇÃO DE COMUNICAÇÕES")
        processos = self.lerProcessos(arquivoEntrada)
        if not processos:
            print("Nenhum processo para processar")
            return
        print(f"Consultando {len(processos)} processos...\n")
        for idx, processo in enumerate(processos, 1):
            comunicacoes = self.consultarComunicacoes(processo)
            if comunicacoes:
                for com in comunicacoes:
                    com["numeroProcesso"] = processo
                self.comunicacoesBrutas.extend(comunicacoes)
        if not self.comunicacoesBrutas:
            print("\nNenhuma comunicação encontrada na API. Gerando dados de teste...\n")
            self.comunicacoesBrutas = self.gerarDadosTeste(processos)
        print(f"\nExtração: {len(self.comunicacoesBrutas)} comunicações processadas")
    
    def gerarDadosTeste(self, processos: List[str]) -> List[Dict]:
        dadosTeste = []
        temasDemo = ['homologacao', 'decisao', 'rateio_pagamento', 'prazo', 'edital']
        for idx, processo in enumerate(processos[:5], 1):
            for i in range(2):
                data = datetime(2024, 1, 1) + timedelta(days=idx * 10 + i * 5)
                tema = temasDemo[(idx + i) % len(temasDemo)]
                dadosTeste.append({
                    "numeroProcesso": processo,
                    "dataHora": data.isoformat(),
                    "assunto": f"Comunicação de {tema}",
                    f"{tema}Limpo": f"Texto teste com keywords de {tema}",
                    "conteudoLimpo": f"Conteudo teste para {tema} - homolog - rateio - prazo"
                })
        return dadosTeste
        
    def normalizarTexto(self, texto: str) -> str:
        if not texto:
            return ""
        texto = re.sub(r'<[^>]+>', '', texto)
        texto = unicodedata.normalize('NFKD', texto)
        texto = texto.encode('ASCII', 'ignore').decode('ASCII')
        texto = texto.lower()
        texto = re.sub(r'\s+', ' ', texto)
        texto = re.sub(r'[^\w\s]', '', texto)
        return texto.strip()
    
    def higienizarComunicacao(self, comunicacao: Dict) -> Dict:
        higienizada = comunicacao.copy()
        for campo in ['assunto', 'descricao', 'conteudo', 'texto']:
            if campo in higienizada and higienizada[campo]:
                textolimpo = self.normalizarTexto(str(higienizada[campo]))
                higienizada[f"{campo}limpo"] = textolimpo
        if 'dataHora' in higienizada:
            try:
                higienizada['dataHora'] = pd.to_datetime(higienizada['dataHora']).isoformat()
            except:
                higienizada['dataHora'] = None
        return higienizada
    
    def executarLimpeza(self):
        print("\nFASE 2: LIMPEZA E NORMALIZAÇÃO")
        if not self.comunicacoesBrutas:
            print("Nenhuma comunicação para limpar")
            return
        print(f"Limpando {len(self.comunicacoesBrutas)} comunicações...")
        df = pd.DataFrame(self.comunicacoesBrutas)
        for campo in ['assunto', 'descricao', 'conteudo', 'texto']:
            if campo in df.columns:
                df[f"{campo}limpo"] = df[campo].fillna('').astype(str).apply(self.normalizarTexto)
        if 'dataHora' in df.columns:
            df['dataHora'] = pd.to_datetime(df['dataHora'], errors='coerce').dt.strftime('%Y-%m-%dT%H:%M:%S')
        self.comunicacoesLimpas = df.to_dict('records')
        print(f"{len(self.comunicacoesLimpas)} comunicações processadas")
    
    def identificarTemas(self, texto: str) -> Dict[str, List[str]]:
        temasEncontrados = {}
        textoLower = texto.lower() if texto else ""
        for tema, palavrasChave in self.temas.items():
            palavrasMatch = []
            for palavra in palavrasChave:
                if palavra in textoLower:
                    palavrasMatch.append(palavra)
            if palavrasMatch:
                temasEncontrados[tema] = palavrasMatch
        return temasEncontrados
    
    def calcularScore(self, temas: List[str]) -> int:
        if not temas:
            return 0
        score = sum(self.pesos.get(tema, 0) for tema in temas)
        temasSet = set(temas)
        for (tema1, tema2), bonus in self.bonus.items():
            if tema1 in temasSet and tema2 in temasSet:
                score += bonus
        return score
    
    def classificarComunicacao(self, comunicacao: Dict) -> Dict:
        classificada = comunicacao.copy()
        texto = None
        for campo in ['conteudoLimpo', 'textoLimpo', 'descricaoLimpo', 'assuntoLimpo']:
            if campo in comunicacao and comunicacao[campo]:
                texto = comunicacao[campo]
                break
        if not texto:
            texto = str(comunicacao.get('assunto', ''))
        temasDict = self.identificarTemas(texto)
        temas = list(temasDict.keys())
        score = self.calcularScore(temas)
        justificativa = self.gerarJustificativa(temasDict)
        classificada['temas'] = ','.join(temas) if temas else 'nenhum'
        classificada['score'] = score
        classificada['justificativa'] = justificativa
        return classificada
    
    def _gerarJustificativa(self, temas_dict: Dict[str, List[str]]) -> str:
        if not temas_dict:
            return "Nenhuma palavra-chave detectada"
        justificativas = []
        for tema, palavras in temas_dict.items():
            palavrasStr = ', '.join(set(palavras))
            justificativas.append(f"{tema}: {palavrasStr}")
        return " | ".join(justificativas)
    
    def extrairTemas(self, row: pd.Series) -> str:
        texto = None
        for campo in ['conteudoLimpo', 'textoLimpo', 'descricaoLimpo', 'assuntoLimpo']:
            if campo in row and row[campo]:
                texto = row[campo]
                break
        if not texto:
            texto = str(row.get('assunto', ''))
        temasDict = self.identificarTemas(texto)
        return ','.join(temasDict.keys()) if temasDict else 'nenhum'
    
    def gerarJustificativa(self, row: pd.Series) -> str:
        texto = None
        for campo in ['conteudoLimpo', 'textoLimpo', 'descricaoLimpo', 'assuntoLimpo']:
            if campo in row and row[campo]:
                texto = row[campo]
                break
        if not texto:
            texto = str(row.get('assunto', ''))
        temasDict = self.identificarTemas(texto)
        return self._gerarJustificativa(temasDict)
    
    def executarClassificacao(self):
        print("\nFASE 3: CLASSIFICAÇÃO E SCORING")
        if not self.comunicacoesLimpas:
            print("Nenhuma comunicação para classificar")
            return
        print(f"Classificando {len(self.comunicacoesLimpas)} comunicações...")
        df = pd.DataFrame(self.comunicacoesLimpas)
        df['temas'] = df.apply(self.extrairTemas, axis=1)
        df['score'] = df['temas'].apply(lambda t: self.calcularScore(t.split(',') if t and t != 'nenhum' else []))
        df['justificativa'] = df.apply(self.gerarJustificativa, axis=1)
        self.comunicacoesClassificadas = df.to_dict('records')
        print(f"{len(self.comunicacoesClassificadas)} comunicações classificadas")
    
    def salvarResultados(self, nomeArquivo: str = "resultado_final.xlsx"):
        print(f"\nGerando relatório em {nomeArquivo}...")
        if not self.comunicacoesClassificadas:
            print("Nenhuma comunicação para salvar")
            return
        try:
            with pd.ExcelWriter(nomeArquivo, engine='openpyxl') as writer:
                df = pd.DataFrame(self.comunicacoesClassificadas)
                colunasAba1 = ['numeroProcesso', 'dataHora', 'assunto', 'temas', 'score', 'justificativa']
                colunasExistentes = [col for col in colunasAba1 if col in df.columns]
                df[colunasExistentes].to_excel(writer, sheet_name='Comunicacoes', index=False)
                print(f"  Aba 1: {len(df)} comunicações")
                resumo = self.gerarResumoProcesso()
                resumo.to_excel(writer, sheet_name='Resumo_Processo', index=False)
                print(f"  Aba 2: {len(resumo)} processos")
                inconsistencias = self.gerarEstatisticas()
                inconsistencias.to_excel(writer, sheet_name='Inconsistencias', index=False)
                print(f"  Aba 3: Inconsistências")
            print(f"\nRelatório salvo: {nomeArquivo}")
        except Exception as e:
            print(f"Erro ao salvar: {e}")
    
    def gerarResumoProcesso(self) -> pd.DataFrame:
        df = pd.DataFrame(self.comunicacoesClassificadas)
        df['score'] = df['score'].fillna(0).astype(int)
        
        resumo = df.groupby('numeroProcesso').agg({
            'score': ['sum', 'max'],
            'numeroProcesso': 'count',
            'temas': lambda x: ','.join(set([t.strip() for t in ','.join(x.fillna('nenhum')).split(',') if t.strip() and t.strip() != 'nenhum'])),
            'dataHora': lambda x: pd.to_datetime(x, errors='coerce').max()
        }).reset_index()
        
        resumo.columns = ['processo', 'score_total', 'score_maximo', 'total_comunicacoes', 'temas_principais', 'data_ultima_comunicacao']
        resumo['temas_principais'] = resumo['temas_principais'].apply(lambda x: x if x else 'nenhum')
        resumo['observacao'] = resumo.apply(lambda row: f"Score máximo: {row['score_maximo']} | Temas: {row['temas_principais']}", axis=1)
        
        return resumo[['processo', 'total_comunicacoes', 'score_total', 'score_maximo', 'temas_principais', 'data_ultima_comunicacao', 'observacao']]
    
    def gerarEstatisticas(self) -> pd.DataFrame:
        df = pd.DataFrame(self.comunicacoesClassificadas)
        inconsistenciasList = []
        
        camposObrigatorios = ['numeroProcesso', 'assunto']
        for campo in camposObrigatorios:
            faltantes = df[df[campo].isna() | (df[campo].astype(str).str.strip() == '')]
            for _, row in faltantes.iterrows():
                inconsistenciasList.append({
                    'tipo': 'Campo ausente',
                    'descricao': f"Campo '{campo}' vazio",
                    'processo': row.get('numeroProcesso', 'N/A'),
                    'severidade': 'Alta'
                })
        
        if 'dataHora' in df.columns:
            datas_invalidas = df[pd.to_datetime(df['dataHora'], errors='coerce').isna()]
            for _, row in datas_invalidas.iterrows():
                inconsistenciasList.append({
                    'tipo': 'Data inválida',
                    'descricao': f"dataHora inválida: {row.get('dataHora')}",
                    'processo': row.get('numeroProcesso', 'N/A'),
                    'severidade': 'Média'
                })
        
        sem_classificacao = df[(df['score'] == 0) & (df['temas'] == 'nenhum')]
        for _, row in sem_classificacao.iterrows():
            inconsistenciasList.append({
                'tipo': 'Sem classificação',
                'descricao': 'Comunicação sem temas identificados',
                'processo': row.get('numeroProcesso', 'N/A'),
                'severidade': 'Baixa'
            })
        
        for erro in self.erros:
            inconsistenciasList.append({
                'tipo': 'Erro de API',
                'descricao': str(erro),
                'processo': 'N/A',
                'severidade': 'Média'
            })
        
        if not inconsistenciasList:
            inconsistenciasList.append({
                'tipo': 'Nenhuma',
                'descricao': 'Nenhuma inconsistência detectada',
                'processo': 'N/A',
                'severidade': 'Informativo'
            })
        return pd.DataFrame(inconsistenciasList)
    
    def executarPipeline(self, arquivoEntrada: str = "processos.txt"):
        print("\nPIPELINE COMPLETO")
        print("Extração, Limpeza, Classificação")
        self.executarExtracao(arquivoEntrada)
        self.executarLimpeza()
        self.executarClassificacao()
        self.salvarResultados("resultado_final.xlsx")

app = None
if FastAPI is not None:
    app = FastAPI(
        title="Extracao PJe API",
        description="API para consulta de comunicacoes do PJe e execucao do pipeline.",
        version="1.0"
    )

    @app.get("/", tags=["Root"])
    def root() -> Dict[str, str]:
        return {"message": "API de extracao PJe. Acesse /docs para Swagger UI."}

    @app.get("/comunicacoes/{numeroProcesso}", tags=["Comunicacoes"])
    def get_comunicacoes(
        numeroProcesso: str,
        dataInicio: Optional[str] = Query("01/01/2024", description="Data inicial no formato DD/MM/AAAA"),
        dataFim: Optional[str] = Query("01/01/2025", description="Data final no formato DD/MM/AAAA")
    ) -> List[Dict[str, Any]]:
        extrator = ExtracaoPJeAPI(dataInicio=dataInicio, dataFim=dataFim)
        comunicacoes = extrator.consultarComunicacoes(numeroProcesso)
        if not comunicacoes:
            raise HTTPException(status_code=404, detail="Nenhuma comunicacao encontrada para este processo")
        return comunicacoes

if __name__ == "__main__":
    extrator = ExtracaoPJeAPI(
        dataInicio="01/01/2024",
        dataFim="01/01/2025"
    )
    extrator.executarPipeline("processos.txt")
    print("PIPELINE FINALIZADO")
