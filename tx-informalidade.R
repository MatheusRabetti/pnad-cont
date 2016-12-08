#!/home/matheusrabetti/anaconda2/lib/R/bin/Rscript

# Como utilizar este Script via shell?
# passar argumentos de ano e trimestre após chamar este arquivo
# Se eu quiser atualizar segundo os dados da PNAD Continua do quarto tri de 2016
# Exemplo: Rscript survey-setorial-pdi.R 2016 4

# Rodar todos os periodos
# for year in {2012..2016}; do for tri in {1..4}; do Rscript tx-informalidade.R $year $tri; done; done

# tdWritetale faz update na tabela

# Drop tables
# dbGetQuery(tdConnection, 'drop table dev_acc.pnadc_informal')
# dbGetQuery(tdConnection, 'drop table dev_acc.pnadc_informal_subst')
# dbGetQuery(tdConnection, 'drop table dev_acc.pnadc_informal_grst')


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
      "SELECT ANO_BASE, TRIMESTRE,
      UPA, ESTRATO, PESO_DOM_PESSOAS, PESO_DOM_COM_EST, 
      DOM_PROJECAO, PROJ_POPULACAO, 
      POS_OCUP_CAT_EMP, CONT_INST_PREV, 
      COND_OCUP_SEM_REF, UF_DESC, GRUP_ATIV_PRIN_EMP,
      COD_PRINC_ATIV_NEG
      FROM DEV_ODS.pnad_cont
      WHERE ANO_BASE =", ano, "AND TRIMESTRE =", trimestre,";"))

if(nrow(pnadc) == 0) {
  stop("Ano e trimestre não existentes no Teradata. Por favor, utilizar um período correto ou atualizar a base de dados")
}

cat(sprintf('Leitura finalizada... \n Iniciando a manipulação de dados \n'))

paste(1:dim(pnadc)[2],names(pnadc))
cols = c(1:11,13)
pnadc[,cols] %<>% lapply(function(x) as.numeric(x))

ifelse(pnadc$COD_PRINC_ATIV_NEG == 'NA', NA,
       pnadc$COD_PRINC_ATIV_NEG) -> pnadc$COD_PRINC_ATIV_NEG

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

# Setor
pnadc %<>% 
  mutate(sub_setor = 
           ifelse(is.na(GRUP_ATIV_PRIN_EMP), NA,
                  ifelse(grepl('^(011)|^(01401)', COD_PRINC_ATIV_NEG) , 'Agricultura',
                         ifelse(grepl('^(012)|^(015)|^(01402)', COD_PRINC_ATIV_NEG), 'Pecuária',
                                ifelse(grepl('^(02)|^(03)', COD_PRINC_ATIV_NEG), 'Produção florestal, pesca e aquicultura',
                                       ifelse(grepl('^(0[5-9])', COD_PRINC_ATIV_NEG), 'Indústria extrativa',
                                              ifelse(grepl('^([1-2][0-9])|^(3[0-3])', COD_PRINC_ATIV_NEG), 'Indústria de transformação',
                                                     ifelse(grepl('^3[5-9]', COD_PRINC_ATIV_NEG), 'SIUP',
                                                            ifelse(grepl('^4[1-3]', COD_PRINC_ATIV_NEG), 'Contrução',
                                                                   ifelse(GRUP_ATIV_PRIN_EMP %in% 4, 'Comércio e reparação de veículos automotores e motocicletas',
                                                                          ifelse(GRUP_ATIV_PRIN_EMP %in% 5, 'Transporte, Armazenagem e Correios',
                                                                                 ifelse(GRUP_ATIV_PRIN_EMP %in% 6, ' Alojamento e alimentação',
                                                                                        ifelse(grepl('^(5[8-9])|^(6[0-3])', COD_PRINC_ATIV_NEG), 'Informação e comunicação',
                                                                                               ifelse(grepl('^(6[4-6])', COD_PRINC_ATIV_NEG), 'Atividades financeiras, de seguros e serviços relacionados',
                                                                                                      ifelse(grepl('^68', COD_PRINC_ATIV_NEG), 'Atividades Imobiliárias',
                                                                                                             ifelse(grepl('^(69)|^(7[0-9])|^(8[0-2])', COD_PRINC_ATIV_NEG), 'Atividades profissionais, científicas e técnicas, administrativas e serviços complementares',
                                                                                                                    ifelse(GRUP_ATIV_PRIN_EMP %in% 8, 'Administração, educação, saúde, pesquisa e desenvolvimento públicas, defesa, seguridade social',
                                                                                                                           ifelse(GRUP_ATIV_PRIN_EMP %in% 9, 'Educação e Saúde Mercantil',
                                                                                                                                  ifelse(grepl('^9[0-69]', COD_PRINC_ATIV_NEG), 'Artes, cultura, esporte e recreação e outros serviços',
                                                                                                                                         ifelse(GRUP_ATIV_PRIN_EMP %in% 11, 'Serviços Domésticos',
                                                                                                                                                ifelse(GRUP_ATIV_PRIN_EMP %in% 12, 'Atividades mal definidas', 'Agropecuária')
                                                                                                                                         )))))))))))))))))))
  )


source('~/Documents/Analises/PDI-R/pnadc-survey/pnadc-metrics.R')

pnadc.surv <- amostragem(pnadc)
options(survey.lonely.psu = "adjust")

tx_informalidade(ano, trimestre)


tdClose()
