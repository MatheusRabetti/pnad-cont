#!/home/matheusrabetti/anaconda2/lib/R/bin/Rscript

# Como utilizar este Script via shell?
# passar argumentos de ano e trimestre após chamar este arquivo
# Se eu quiser atualizar segundo os dados da PNAD Continua do quarto tri de 2016
# Exemplo: Rscript survey-setorial-pdi.R 2016 4

# Rodar todos os periodos
# for year in {2012..2016}; do for tri in {1..4}; do Rscript desemprego.R $year $tri; done; done

# tdWritetale faz update na tabela

# Drop tables
# dbGetQuery(tdConnection, 'drop table dev_acc.pnadc_desocup')
# dbGetQuery(tdConnection, 'drop table dev_acc.pnadc_desocup_sexo')
# dbGetQuery(tdConnection, 'drop table dev_acc.pnadc_desocup_grst')


options(echo = TRUE)
args<-commandArgs(TRUE)

options(java.parameters = "-Xmx8048m")

suppressMessages(library(RJDBC))
suppressMessages(library(teradataR))
suppressMessages(library(survey))
suppressMessages(library(dplyr))
suppressMessages(library(magrittr))
suppressMessages(library(data.table))

options(scipen=999)


# CONEXAO TERADATA --------------------------------------------------------


.jinit()
.jaddClassPath('/home/matheusrabetti/Desktop/data-integration/lib/terajdbc4.jar')
.jaddClassPath("/home/matheusrabetti/Desktop/data-integration/lib/tdgssconfig.jar")

teradataR::tdConnect("10.252.1.21/TMODE=ANSI,CHARSET=UTF8",
                     uid = "d_unb_carga",
                     pwd = "d_unb_carga", dType = 'jdbc')

#  EXTRACT & TRANSFORM ----------------------------------------------------

ano = args[1]
trimestre = args[2]

cat(sprintf('Lendo a base de dados do Ano de %s Trimestre %s \n',ano,trimestre))

pnadc <-
  tdQuery(
    paste(
      "SELECT ANO_BASE, TRIMESTRE, NIVEL_INSTRUCAO_DESC,
        UPA, ESTRATO, PESO_DOM_PESSOAS, PESO_DOM_COM_EST, 
        DOM_PROJECAO, PROJ_POPULACAO, COND_DOMIC, SEXO_DESC,
        POS_OCUP_CAT_EMP, CONT_INST_PREV, IDADE_MORA_DT_REF,
        COND_OCUP_SEM_REF, UF_DESC, GRUP_ATIV_PRIN_EMP
        FROM DEV_ODS.pnad_cont
        WHERE ANO_BASE =", ano, "AND TRIMESTRE =", trimestre,";"))

if(nrow(pnadc) == 0) {
  stop("Ano e trimestre não existentes no Teradata. Por favor, utilizar um período correto ou atualizar a base de dados")
}

cat(sprintf('Leitura finalizada... \n Iniciando a manipulação de dados \n'))

paste(1:dim(pnadc)[2],names(pnadc))
cols = c(1,2,4:10,12:15,17)
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

pnadc[COND_DOMIC == 1, chefe := 'Chefe de Família']
pnadc[COND_DOMIC != 1, chefe := 'Outro']

pnadc[IDADE_MORA_DT_REF %in% 0:24, fx_etaria := 'Até 24 anos']
pnadc[IDADE_MORA_DT_REF %in% 25:29, fx_etaria := 'De 25 a 29 anos']
pnadc[IDADE_MORA_DT_REF %in% 30:39, fx_etaria := 'De 30 a 39 anos']
pnadc[IDADE_MORA_DT_REF %in% 40:49, fx_etaria := 'De 40 a 49 anos']
pnadc[IDADE_MORA_DT_REF %in% 50:130, fx_etaria := '50 anos ou mais']


source('~/Documents/Analises/PDI-R/pnadc-survey/pnadc-metrics.R')

pnadc.surv <- amostragem(pnadc)

# desocupados(ano, trimestre)

tx_desemprego(ano, trimestre)

tdClose()
