#!/home/matheusrabetti/anaconda2/lib/R/bin/Rscript

# Como utilizar este Script via shell?
# passar argumentos de ano e trimestre após chamar este arquivo
# Se eu quiser atualizar segundo os dados da PNAD Continua do quarto tri de 2016
# Exemplo: Rscript mobilidade-pdi.R 2016 4

# Rodar todos os periodos
# for year in {2012..2016}; do for tri in {1..4}; do Rscript estoque.R $year $tri; done; done

options(echo = TRUE)
args<-commandArgs(TRUE)

options(java.parameters = "-Xmx8048m")

suppressMessages(library(RJDBC))
suppressMessages(library(teradataR))
suppressMessages(library(data.table)) # pacote para manipulacao de dados - velocidade
suppressMessages(library(dplyr)) # pacote para manipulacao de dados - facil
suppressMessages(library(magrittr))
suppressMessages(library(stringr)) # pacote para manipulacao de string - id's
suppressMessages(library(tidyr)) # deitar as variaveis - painel

options(scipen=999)


# CONEXAO TERADATA --------------------------------------------------------


.jinit()
.jaddClassPath('/home/matheusrabetti/Desktop/data-integration/lib/terajdbc4.jar')
.jaddClassPath("/home/matheusrabetti/Desktop/data-integration/lib/tdgssconfig.jar")

teradataR::tdConnect("10.252.1.21/TMODE=ANSI,CHARSET=UTF8",
                     uid = "d_unb_carga",
                     pwd = "d_unb_carga", dType = 'jdbc')

ano <- args[1]
trimestre <- args[2]
cat(sprintf('Lendo a base de dados do Ano de %s Trimestre %s \n',ano,trimestre))

if(trimestre == 1){
  periodo_ant = c(ano-1, 4)
} else {
  periodo_ant = c(ano, trimestre-1)
}

pnadc <-
  tdQuery(
    sprintf(
      "SELECT ANO_BASE, TRIMESTRE,
              UPA, ESTRATO, PESO_DOM_PESSOAS, PESO_DOM_COM_EST, 
              DOM_PROJECAO, PROJ_POPULACAO, 
              NUM_SEL_DOM, PAINEL, NUM_ENT_DOM,
              SEXO, DIA_NASC, MES_NASC, ANO_NASC,
              POS_OCUP_CAT_EMP, CONT_INST_PREV, 
              COND_OCUP_SEM_REF, UF_DESC, GRUP_ATIV_PRIN_EMP
      FROM DEV_ODS.pnad_cont
      WHERE ANO_BASE IN %s AND TRIMESTRE = %s AND NUM_ENT_DOM = 2", ano, trimestre))

if(nrow(pnadc) == 0) {
  stop("Ano e trimestre não existentes no Teradata. Por favor, utilizar um período correto ou atualizar a base de dados")
}
gc()

pnadc_anterior <-
  tdQuery(
    sprintf(
      "SELECT ANO_BASE, TRIMESTRE,
              UPA, ESTRATO, PESO_DOM_PESSOAS, PESO_DOM_COM_EST, 
              DOM_PROJECAO, PROJ_POPULACAO, 
              NUM_SEL_DOM, PAINEL, NUM_ENT_DOM,
              SEXO, DIA_NASC, MES_NASC, ANO_NASC,
              POS_OCUP_CAT_EMP, CONT_INST_PREV, 
              COND_OCUP_SEM_REF, UF_DESC, GRUP_ATIV_PRIN_EMP
      FROM DEV_ODS.pnad_cont
      WHERE ANO_BASE IN %s AND TRIMESTRE = %s AND NUM_ENT_DOM = 1", periodo_ant[1], periodo_ant[2]))

if(nrow(pnadc_pre) == 0) {
  stop("Ano e trimestre não existentes no Teradata. Por favor, utilizar um período correto ou atualizar a base de dados")
}


# AGRUPANDO E ID's ------------------------------------------------------------

pnadc <- rbind(pnadc, pnadc_anterior)
rm(pnadc_anterior)
setDT(pnadc)
pnadc[, iddom := paste0(UPA, NUM_SEL_DOM, PAINEL) ]
pnadc[, idpes := paste0(iddom, SEXO, DIA_NASC, MES_NASC, ANO_NASC) ]
pnadc[, data_ref := paste0(ANO_BASE, TRIMESTRE)]
setkey(pnadc, iddom, idpes)

# VARIAVEIS INTERESSE -----------------------------------------------------

cat(sprintf('Leitura finalizada... \n Iniciando a manipulação de dados \n'))

paste(1:dim(pnadc)[2],names(pnadc))
cols = c(1,3,4,6:8,16:18,20)
setDF(pnadc)
pnadc[,cols] %<>% lapply(function(x) as.numeric(x))

setDT(pnadc)
pnadc[, one := 1]
pnadc[, ocup := 0]
pnadc[COND_OCUP_SEM_REF %in% 1, ocup := 1]
pnadc[, desocup := 0]
pnadc[COND_OCUP_SEM_REF %in% 2, desocup := 1]
pnadc[, informal := 0]
pnadc[POS_OCUP_CAT_EMP %in% c(2,4,6,10) | 
        (POS_OCUP_CAT_EMP %in%  9 & CONT_INST_PREV %in% 2), informal := 1]
pnadc[, formal := 0]
pnadc[POS_OCUP_CAT_EMP %in% c(1,5,7,3,8) | 
        (POS_OCUP_CAT_EMP %in%  9 & CONT_INST_PREV %in% 1), formal := 1]
pnadc[, pea := 0]
pnadc[ocup %in% 1 | desocup %in% 1, pea := 1]
pnadc[, npea := 0]
pnadc[pea != 1, npea := 1]


# Setor
pnadc[, gr_setor := 'Mal Definido']
pnadc[is.na(GRUP_ATIV_PRIN_EMP), gr_setor := NA]
pnadc[GRUP_ATIV_PRIN_EMP %in% 1, gr_setor := 'Agricultura']
pnadc[GRUP_ATIV_PRIN_EMP %in% 2, gr_setor := 'Indústria']
pnadc[GRUP_ATIV_PRIN_EMP %in% 3, gr_setor := 'Construção']
pnadc[GRUP_ATIV_PRIN_EMP %in% 4, gr_setor := 'Agricultura']
pnadc[GRUP_ATIV_PRIN_EMP %in% 5:11, gr_setor := 'Comércio']

# CLEAN ------------------------------------------------------------------

# Tirar individuos sem data de nascimento
pnadc <- pnadc[-grep('99999999', pnadc$idpes)]

# Duas primeiras entrevistas
pnadc <- pnadc[NUM_ENT_DOM %in% 1:2,]

# Excluir os gemeos - IDPES igual
# Dentro do mesmo domicílio duas pessoas com mesma data de nascimento
pnadc %>% 
  group_by(idpes, data_ref) %>% 
  summarise(count = n(),
            ocupados = sum(ocup*PESO_DOM_COM_EST)) %>% 
  filter(count == 1) %>% 
  select(idpes, data_ref) -> ids_amostra

# Pegando os dados de 2014 e fazendo o match com os id's de 2015
setkey(pnadc, idpes)
setDT(ids_amostra)
setkey(ids_amostra, idpes)

merge(pnadc, ids_amostra, by = c('data_ref', 'idpes')) -> pnadc
rm(ids_amostra)

# PAINEL ------------------------------------------------------------------

# Transformar em painel
# 0001 - inativo N; 0010 - desocupado D; 0100 - informal I; 1000 - formal F // K - não encontrado
pnadc %>% 
  select(idpes, data_ref, formal, informal, desocup, npea) %>% 
  unite(transicao, formal, informal, desocup, npea, sep = '') %>% 
  mutate(transicao = ifelse(transicao %in% '0001', 'N',
                            ifelse(transicao %in% '0010', 'D',
                                   ifelse(transicao %in% '0100', 'I', 
                                          ifelse(transicao %in% '1000', 'F','K'))))) %>% 
  spread(data_ref, transicao) -> transicao

setDT(transicao)
# Todo NA -> N
transicao[, apply(.SD, 2, function(x) {x[is.na(x)] <-'K'; x} )] -> transicao
transicao <- data.table(transicao)

# mudar o nome '2014' pra outro aceitavel no R
names(transicao)[-1] <- c('tri_anterior', 'tri_atual')

# Adicionar coluna com a sequencia de todas as entrevistas
transicao %>% 
  unite(trans, tri_anterior, tri_atual,
        remove = F, sep = '') -> transicao

transicao[!grepl('K', trans)] -> transicao

transicao %>% 
  select(idpes, trans) %>% 
  left_join(pnadc, ., by = c('idpes')) -> pnadc


# RESULTADOS --------------------------------------------------------------

pnadc %>% 
  filter(ANO_BASE == ano & TRIMESTRE == trimestre) %>% 
  group_by(trans, UF_DESC) %>% 
  summarise(total = sum(one*PESO_DOM_COM_EST)) %>% 
  mutate(condicao_t1 = substr(trans, 1, 1)) %>% 
  group_by(condicao_t1, UF_DESC) %>% 
  filter(!is.na(trans)) %>% 
  mutate(probabilidade = round(total/ sum(total) * 100, 2)) %>% 
  arrange(trans) -> resultado

resultado$ano <- ano
resultado$trimestre <- trimestre

resultado %<>% ungroup() %>%  select(-total, -condicao_t1)


pnadc %>% 
  filter(ANO_BASE == ano & TRIMESTRE == trimestre) %>% 
  group_by(trans, UF_DESC) %>% 
  summarise(total = sum(one*PESO_DOM_COM_EST)) %>% 
  filter(trans %in% c('NF', 'DF', 'NI', 'DI')) %>% 
  mutate(contratacao = substr(trans, 2, 2)) %>% 
  group_by(contratacao, UF_DESC) %>% 
  summarise(total = sum(total)) %>% 
  group_by(UF_DESC) %>% 
  mutate(probabilidade = round(total/ sum(total) * 100, 2)) %>% 
  arrange(UF_DESC, contratacao) -> contratacao

contratacao$ano <- ano
contratacao$trimestre <- trimestre

contratacao %<>% ungroup() %>%  select(-total)



# EXPORT ------------------------------------------------------------------

tdWriteTable_ps(databasename = 'DEV_ACC', 
                tablename = 'pnadc_mobilidade', df = resultado)

tdWriteTable_ps(databasename = 'DEV_ACC', 
                tablename = 'pnadc_mobilidade_contratacao', df = contratacao)

tdClose()
